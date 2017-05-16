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
extern crate typed_arena;

use std::collections::{BTreeMap, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::env;
use std::ffi::{OsStr, OsString};
use std::fs::{self, DirEntry, File};
use std::hash::Hasher;
use std::io::{Error as IoError, Read, Result as IoResult};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::slice::Iter;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use bincode::Infinite;
use either::Either;
use futures::{future, stream, Future, Sink, Stream};
use futures::sink::Wait;
use futures::sync::mpsc::{self, Sender};
use futures_cpupool::CpuPool;
use symtern::{Pool, Sym};
use symtern::prelude::*;
use typed_arena::Arena;

type Index = u64;
type Intern = Pool<OsStr, Index>;
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
    segments: Pool<Path, Index>,
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
    segments: HashMap<Index, &'a Path>,
    nodes: Vec<StorableTreeNode>,
    files: &'a Vec<FileInfo>,
}

impl FileTree {
    pub fn build<P: AsRef<Path>>(path: P) -> Self {
        info!("Scanning path {}", path.as_ref().to_string_lossy());
        let mut tree = FileTree {
            segments: Pool::new(),
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
        let mut segments: HashMap<Index, &Path> = HashMap::new();
        let mut mapping: HashMap<Symbol, Index> = HashMap::new();
        let mut cnt: Index = 0;
        let nodes = (&self.nodes)
            .into_iter()
            .map(|&TreeNode { segment, parent }| {
                let idx = mapping.entry(segment).or_insert_with(|| {
                    cnt += 1;
                    segments.insert(cnt, self.segments.resolve(segment).unwrap());
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

fn main() {
    env_logger::init().unwrap();
    let p = PathBuf::from(env::args().nth(1).expect("Expected a path"));
    let tree = FileTree::build(p);
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
    let prehashed = size_prehash
        .into_iter()
        .rev()
        .map(Ok::<_, IoError>);
    stream::iter(prehashed)
        .map(|(size, nodes)| {
            info!("Working on size {}, {} of {} files done", size, done.load(Ordering::Relaxed), total);
            // Group the ones with the same basename.suffix together. There's a chance these
            // actually correspond to the same extents, so use the OS page cache by reading them
            // together.
            let mut name_prehash = HashMap::new();
            for node in &nodes {
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
                .map(|node| {
                    let path = tree.path(node);
                    let node = (*node).clone();
                    debug!("Coputing details of {}", path.to_string_lossy());
                    pool.spawn(future::lazy(move || {
                        let details = Details::compute(path, node.size);
                        Ok((node, details)) as IoResult<_>
                    }))
                })
                .buffer_unordered(cpus);
        });
    /*
    unsafe { ARENA = Some(Arena::new()) };
    let (sender, receiver) = mpsc::channel::<(&SegPath, u64)>(102400);
    let details_thread = thread::spawn(move || {
        let pool = CpuPool::new(cpus);
        receiver
            .filter(|&(_, len)| len > 0)
            .fold(Box::new(HashMap::new()), |mut hash, (segments, details)| {
                debug!("Name-hashing segment {}", segments.to_path().to_string_lossy());
                hash.entry(segments.segment.to_owned())
                    .or_insert_with(Vec::new)
                    .push((segments, details));
                Ok(hash)
            })
            .and_then(|hash| {
                let iter = hash.into_iter()
                    .flat_map(|(_, infos)| infos.into_iter())
                    .map(Ok);
                Ok(stream::iter(iter))
            })
            .flatten_stream()
            .map(|(path_segments, len)| {
                pool.spawn(
                    future::lazy(move || {
                        let details = Details::compute(path_segments.to_path(), len);
                        Ok((path_segments, details))
                    })
                )
            })
            .buffer_unordered(cpus)
            .filter_map(|(segments, details)| match details {
                Ok(details) => Some((segments, details)),
                Err(e) => {
                    error!("Error getting file info: {}", e);
                    None
                },
            })
            .fold(Box::new(HashMap::new()), |mut hash, (segments, details)| {
                hash.entry(details)
                    .or_insert_with(Vec::new)
                    .push(segments);
                Ok(hash)
            })
            .and_then(|hash| {
                 stream::iter(hash.into_iter().map(|val| Ok(val)))
                    .filter(|&(_, ref segments)| segments.len() > 1)
                    .map(|(details, segments)| {
                        let run = move || {
                            let mut command = Command::new("btrfs-extent-same");
                            command.arg(format!("{}", details.len));
                            for seg in &segments {
                                command.arg(seg.to_path()).arg("0");
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
                    })
                    .buffer_unordered(cpus)
                    .for_each(|()| Ok(()))
            })
            .wait()
            .unwrap();
    });
    recurse_dir(&p, &mut sender.wait(), Either::Right(Arc::new(p.clone())));
    details_thread.join().unwrap();
    */
}
