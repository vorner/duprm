extern crate env_logger;
extern crate futures;
#[macro_use]
extern crate log;
extern crate num_cpus;
extern crate tokio_core;
extern crate tokio_process;
extern crate walkdir;

use std::cmp::min;
use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::{Read, Result as IoResult, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;

use futures::{stream, Future, Stream};
use tokio_core::reactor::Core;
use tokio_process::CommandExt;
use walkdir::WalkDir;

// For some reason, it doesn't want to do more than 16MB at a time. So split into blocks of this
// size.
const BLOCK: u64 = 16777216;

const DONE_FILE: &str = "done.txt";

fn file_preload<P: AsRef<Path>>(p: P) -> IoResult<()> {
    debug!("Preloading {}", p.as_ref().to_string_lossy());
    let mut f = File::open(p)?;
    let mut buf = Vec::with_capacity(BLOCK as usize);
    buf.extend((0..BLOCK).map(|_| 0));
    while f.read(&mut buf[..])? > 0 {}
    Ok(())
}

fn done_read() -> Result<usize, Box<Error>> {
    let mut f = File::open(DONE_FILE)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    Ok(String::from_utf8(buf)?.parse()?)
}

fn main() {
    env_logger::init().unwrap();
    let (sender, receiver) = mpsc::channel();
    let reader = thread::spawn(move || {
        let p = PathBuf::from(env::args().nth(1).expect("Expected a path"));
        info!("Scanning directories in {}", p.to_string_lossy());
        let files = WalkDir::new(p)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_file())
            .filter_map(|entry| {
                entry.metadata()
                    .map(|m| m.len())
                    .ok()
                    .and_then(|s| {
                        if s > 0 {
                            Some((entry.path().to_path_buf(), s))
                        } else {
                            None
                        }
                    })
            });
        for f in files {
            sender.send(f).unwrap();
        }
    });
    let files = receiver.iter().collect::<Vec<_>>();
    reader.join().unwrap();
    info!("Processing {} files", files.len());
    let mut commands = files
        .iter()
        .fold(Box::new(BTreeMap::new()), |mut map, &(ref path, size)| {
            map.entry((size, path.file_name()))
                .or_insert_with(Vec::new)
                .push(path);
            map
        })
        .into_iter()
        .rev()
        .filter(|&(_, ref entries)| entries.len() > 1)
        .filter(|&(_, ref entries)| {
            let local = entries.iter()
                .any(|entry| entry.to_string_lossy().contains("/backups/"));
            let remote = entries.iter()
                .any(|entry| entry.to_string_lossy().contains("/others/"));
            local && remote
        })
        .flat_map(|((size, _name), entries)| {
            let max_block = size / BLOCK;
            (0..max_block + 1).map(move |i| {
                let start = i * BLOCK;
                let end = min(size, start + BLOCK);
                let len = end - start;
                let mut command = Command::new("btrfs-extent-same");
                command.arg(format!("{}", len));
                for entry in &entries {
                    command.arg(entry).arg(format!("{}", start));
                }
                let entry = if i == 0 {
                    Some(entries.first().unwrap().to_path_buf())
                } else {
                    None
                };
                (command, entry)
            })
        })
        .collect::<Vec<_>>();
    drop(files);
    info!("Generated {} commands", commands.len());
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let counter = AtomicUsize::new(0);
    let total = commands.len();
    let skip = done_read().unwrap_or(0);
    info!("Skipping {} commands", skip);
    let spawned = commands.iter_mut()
        .skip(skip)
        .map(|&mut (ref mut cmd, ref entry)| {
            let done = counter.fetch_add(1, Ordering::Relaxed);
            if done % 100 == 0 {
                info!("Done {} out of {}", done, total);
            }
            debug!("Running command #{}/{}: {:?}", done, total, cmd);
            if let &Some(ref path) = entry {
                let _ = file_preload(path);
            }
            let run = cmd.status_async(&handle)
                .map(move |_| {
                    let mut f = File::create(DONE_FILE).unwrap();
                    write!(f, "{}", done).unwrap();
                });
            Ok(run) as IoResult<_>
        });
    let task = stream::iter(spawned)
        .buffer_unordered(num_cpus::get() + 2)
        .for_each(|_| Ok(())); // TODO Better error ignoring
    core.run(task).unwrap();
}
