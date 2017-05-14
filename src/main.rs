#![feature(drop_types_in_const)]

extern crate either;
extern crate futures;
extern crate futures_cpupool;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate num_cpus;
extern crate typed_arena;

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::env;
use std::ffi::OsString;
use std::fs::{self, DirEntry, File};
use std::hash::Hasher;
use std::io::{Read, Result as IoResult};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::thread;

use either::Either;
use futures::{future, stream, Future, Sink, Stream};
use futures::sink::Wait;
use futures::sync::mpsc::{self, Sender};
use futures_cpupool::CpuPool;
use typed_arena::Arena;

#[derive(Debug)]
struct SegPath {
    parent: Parent,
    segment: OsString,
}

type Parent = Either<&'static SegPath, Arc<PathBuf>>;

impl SegPath {
    fn to_path(&self) -> PathBuf {
        let mut par: PathBuf = match self.parent {
            Either::Left(ref l) => l.to_path(),
            Either::Right(ref r) => (**r).to_owned(),
        };
        par.push(&self.segment);
        par
    }
}

type SegmentSender = Wait<Sender<(&'static SegPath, u64)>>;

static mut ARENA: Option<Arena<SegPath>> = None;

fn do_sub(entry: IoResult<DirEntry>, target: &mut SegmentSender, parent: Parent) -> IoResult<()> {
    let entry = entry?;
    let meta = entry.metadata()?;
    let ftype = meta.file_type();
    let segment: &SegPath = unsafe { ARENA.as_ref().unwrap() }.alloc(SegPath { parent, segment: entry.file_name() });
    if ftype.is_file() {
        target.send((segment, meta.len())).unwrap();
    } else if ftype.is_dir() {
        recurse_dir(entry.path(), target, Either::Left(segment));
    }
    Ok(())
}

fn recurse_dir<P: AsRef<Path>>(path: P, target: &mut SegmentSender, parent: Parent) {
    match fs::read_dir(&path) {
        Ok(dirs) => for sub in dirs {
            if let Err(e) = do_sub(sub, target, parent.clone()) {
                error!("Error handling file: {}", e);
            }
        },
        Err(e) => error!("Error reading dir {}: {}", path.as_ref().to_string_lossy(), e),
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
    unsafe { ARENA = Some(Arena::new()) };
    let cpus = num_cpus::get() * 2;
    let p = PathBuf::from(env::args().nth(1).unwrap());
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
}
