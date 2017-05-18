#![feature(drop_types_in_const)]

extern crate bincode;
extern crate either;
extern crate futures;
extern crate futures_cpupool;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate num_cpus;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate symtern;

use std::collections::{BTreeMap, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::env;
use std::error::Error;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::fs::{self, DirEntry, File};
use std::hash::Hasher;
use std::io::{Error as IoError, Read, Result as IoResult, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::slice::Iter;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::u64;

use bincode::Infinite;
use futures::{future, stream, Future, Stream};
use futures_cpupool::CpuPool;
use symtern::{Pool, Sym};
use symtern::prelude::*;

const DONE_FILE: &str = "done-size.txt";
const TREE_FILE: &str = "files.dump";

type Index = u64;
type Intern = Pool<Path, Index>;
type Symbol = Sym<Index>;

struct TreeNode {
    segment: Symbol,
    parent: usize,
}

#[derive(Clone, Serialize)]
struct FileInfo {
    pub node: usize,
    pub size: u64,
}

struct FileTree {
    segments: Intern,
    nodes: Vec<TreeNode>,
    files: Vec<FileInfo>,
}

#[derive(Serialize)]
struct StorableTreeNode {
    segment: Index,
    parent: usize,
}

#[derive(Serialize)]
struct StorableTree<'a> {
    // Store the path segments as byte slices, so serde doesn't complain about invalid utf8s (we
    // have them).
    segments: HashMap<Index, &'a [u8]>,
    nodes: Vec<StorableTreeNode>,
    files: &'a Vec<FileInfo>,
}

impl FileTree {
    pub fn build<P: AsRef<Path>>(path: P) -> Self {
        info!("Scanning path {}", path.as_ref().to_string_lossy());
        let mut tree = FileTree {
            segments: Intern::new(),
            nodes: Vec::new(),
            files: Vec::new(),
        };
        // Push a sentinel/root
        let segment = tree.segments.intern(path.as_ref()).unwrap();
        tree.nodes.push(TreeNode {
            segment,
            parent: 0,
        });
        // Traverse the tree
        tree.recurse_dir(path, 0);
        tree.nodes.shrink_to_fit();
        tree.files.shrink_to_fit();
        tree
    }
    fn insert<S: AsRef<OsStr>>(&mut self, name: S, parent: usize) -> usize {
        let segment = self.segments.intern(Path::new(&name)).unwrap();
        self.nodes.push(TreeNode { segment, parent });
        self.nodes.len() - 1
    }
    fn do_sub(&mut self, entry: IoResult<DirEntry>, parent: usize) -> IoResult<()> {
        let entry = entry?;
        let meta = entry.metadata()?;
        let ftype = meta.file_type();
        if ftype.is_file() && meta.len() > 0 {
            let node = self.insert(entry.file_name(), parent);
            self.files.push(FileInfo { node, size: meta.len() });
        } else if ftype.is_dir() {
            let me = self.insert(entry.file_name(), parent);
            self.recurse_dir(entry.path(), me);
        }
        Ok(())
    }
    fn recurse_dir<P: AsRef<Path>>(&mut self, path: P, parent: usize) {
        match fs::read_dir(&path) {
            Ok(dirs) => for sub in dirs {
                if let Err(e) = self.do_sub(sub, parent) {
                    error!("Error handling file: {}", e);
                }
            },
            Err(e) => error!("Error reading dir {}: {}", path.as_ref().to_string_lossy(), e),
        }
    }
    pub fn len(&self) -> usize {
        self.files.len()
    }
    fn node_path(&self, node: usize) -> PathBuf {
        let node_ref = &self.nodes[node];
        let segment = self.segments.resolve(node_ref.segment).unwrap();
        if node == 0 {
            PathBuf::from(segment)
        } else {
            let mut parent = self.node_path(node_ref.parent);
            parent.push(segment);
            parent
        }
    }
    pub fn path(&self, file: &FileInfo) -> PathBuf {
        self.node_path(file.node)
    }
    pub fn store<P: AsRef<Path>>(&self, file: P) -> IoResult<()> {
        info!("Storing tree into {}", file.as_ref().to_string_lossy());
        let mut segments: HashMap<Index, &[u8]> = HashMap::new();
        let mut mapping: HashMap<Symbol, Index> = HashMap::new();
        let mut cnt: Index = 0;
        let nodes = (&self.nodes)
            .into_iter()
            .map(|&TreeNode { segment, parent }| {
                let idx = mapping.entry(segment).or_insert_with(|| {
                    cnt += 1;
                    let segments_bytes = self.segments
                        .resolve(segment)
                        .unwrap()
                        .as_os_str()
                        .as_bytes();
                    segments.insert(cnt, segments_bytes);
                    cnt
                });
                StorableTreeNode {
                    segment: *idx,
                    parent,
                }
            })
            .collect::<Vec<_>>();
        let data = StorableTree {
            files: &self.files,
            nodes,
            segments,
        };
        let mut f = File::create(file)?;
        bincode::serialize_into(&mut f, &data, Infinite).unwrap();
        Ok(())
    }
}

impl<'a> IntoIterator for &'a FileTree {
    type Item = &'a FileInfo;
    type IntoIter = Iter<'a, FileInfo>;
    fn into_iter(self) -> Self::IntoIter {
        (&self.files).into_iter()
    }
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct Details {
    len: u64,
    hash: u64,
}

// Read and hash files in 10MB chunks
const CHUNK: usize = 10 * 1028 * 1024;

impl Details {
    fn compute<P: AsRef<Path>>(path: P, len: u64) -> IoResult<Self> {
        debug!("Computing details for {}", path.as_ref().to_string_lossy());
        let mut f = File::open(path)?;
        let mut hasher = DefaultHasher::new();
        let mut buffer = Vec::with_capacity(CHUNK);
        loop {
            let mut chunk = (&mut f).take(CHUNK as u64);
            chunk.read_to_end(&mut buffer)?;
            if buffer.len() == 0 {
                // EOF
                break;
            }
            hasher.write(&buffer);
            buffer.clear();
        }
        Ok(Details {
            len,
            hash: hasher.finish(),
        })
    }
}

fn get_limit() -> Result<u64, Box<Error>> {
    let mut f = File::open(DONE_FILE)?;
    let mut input = Vec::new();
    f.read_to_end(&mut input)?;
    let num = String::from_utf8(input)?.parse()?;
    Ok(num)
}

fn main() {
    env_logger::init().unwrap();
    let limit = get_limit().unwrap_or(u64::MAX);
    let p = PathBuf::from(env::args().nth(1).expect("Expected a path"));
    let tree = FileTree::build(p);
    tree.store(TREE_FILE).unwrap();
    let total = tree.len();
    info!("Going to examine {} files", total);
    let done = AtomicUsize::new(0);
    let cpus = num_cpus::get() * 2;
    let pool = CpuPool::new(cpus);
    let mut size_prehash = BTreeMap::new();
    for node in &tree {
        size_prehash
            .entry(node.size)
            .or_insert_with(Vec::new)
            .push(node);
    }
    // Work on the sizes, starting with the biggest
    let iter = size_prehash
        .into_iter()
        .rev()
        .map(Ok::<_, IoError>);
    stream::iter(iter)
        .map(|(size, mut nodes)| {
            if size > limit {
                done.fetch_add(nodes.len(), Ordering::Relaxed);
                // A cheat, to skip the ones that are already done
                nodes.clear();
            } else {
                info!("Working on size {}, {} of {} files done", size, done.load(Ordering::Relaxed), total);
                let mut s = File::create(DONE_FILE).unwrap();
                write!(s, "{}", size).unwrap();
            }
            // Group the ones with the same basename.suffix together. There's a chance these
            // actually correspond to the same extents, so use the OS page cache by reading them
            // together.
            let mut name_prehash = HashMap::new();
            for node in nodes.into_iter() {
                name_prehash
                    .entry(node.node)
                    .or_insert_with(Vec::new)
                    .push(node);
            }
            let grouped = name_prehash
                .into_iter()
                .flat_map(|(_basename, nodes)| nodes.into_iter())
                .map(Ok::<_, IoError>);
            stream::iter(grouped)
                // Make sure we move just the size → it's own map (there's likely a better trick
                // for that)
                .map(|node| {
                    let size = node.size;
                    let path = tree.path(node);
                    let node = node.clone();
                    let done = done.fetch_add(1, Ordering::Relaxed);
                    debug!("[{}/{}] Coputing details of {} of size {}", done, total, path.to_string_lossy(), size);
                    pool.spawn(future::lazy(move || {
                        let details = Details::compute(path, node.size);
                        Ok((node, details)) as IoResult<_>
                    }))
                })
                .buffer_unordered(cpus)
                .filter_map(|(node, details)| match details {
                    Ok(details) => Some((node, details)),
                    Err(e) => {
                        error!("Error getting file info: {}", e);
                        None
                    },
                })
                .fold(Box::new(HashMap::new()), |mut hash, (node, details)| {
                    hash.entry(details)
                        .or_insert_with(Vec::new)
                        .push(node);
                    Ok(hash) as IoResult<_>
                })
                .and_then(|hash| {
                     let s = stream::iter(hash.into_iter().map(|val| Ok(val)))
                        .filter(|&(_, ref nodes)| nodes.len() > 1)
                        .map(|(details, nodes): (Details, Vec<_>)| {
                            let paths = nodes
                                .iter()
                                .map(|node| tree.path(&node))
                                .collect::<Vec<_>>();
                            let run = move || {
                                let mut command = Command::new("btrfs-extent-same");
                                command.arg(format!("{}", details.len));
                                for path in &paths {
                                    command.arg(path).arg("0");
                                }
                                debug!("cmd: {:?}", command);
                                match command.status() {
                                    Ok(status) => if !status.success() {
                                        error!("Failed to run: {}", status);
                                    },
                                    Err(e) => error!("Failed to run: {}", e),
                                }
                                Ok(())
                            };
                            pool.spawn(future::lazy(run))
                        });
                     Ok(s)
                })
                .flatten_stream()
        })
        .flatten()
        .buffer_unordered(cpus)
        .for_each(|_| Ok(()))
        .wait()
        .unwrap();
    fs::remove_file(TREE_FILE).unwrap();
    fs::remove_file(DONE_FILE).unwrap();
}
