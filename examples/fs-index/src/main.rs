use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use ignore::{ParallelVisitor, ParallelVisitorBuilder, WalkBuilder, WalkState};
use minidex::{FilesystemEntry, Index, Kind};

struct Scanner<'a> {
    index: &'a Index,
    file_count: Arc<AtomicU64>,
}

impl<'s, 'a: 's> ParallelVisitorBuilder<'s> for Scanner<'a> {
    fn build(&mut self) -> Box<dyn ParallelVisitor + 's> {
        Box::new(Self {
            file_count: self.file_count.clone(),
            ..*self
        })
    }
}

impl<'a> ParallelVisitor for Scanner<'a> {
    fn visit(&mut self, entry: Result<ignore::DirEntry, ignore::Error>) -> WalkState {
        if let Ok(entry) = entry {
            let Ok(metadata) = entry.metadata() else {
                return WalkState::Skip;
            };
            let kind = if metadata.is_dir() {
                Kind::Directory
            } else if metadata.is_symlink() {
                Kind::Symlink
            } else {
                Kind::File
            };
            let last_modified = metadata
                .modified()
                .unwrap()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64;
            let last_accessed = last_modified;
            let _ = self.index.insert(FilesystemEntry {
                path: entry.path().to_path_buf(),
                volume: "/".to_string(),
                kind,
                last_modified,
                last_accessed,
            });
            self.file_count.fetch_add(1, Ordering::SeqCst);
            WalkState::Continue
        } else {
            WalkState::Skip
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let index_path = "./index";

    let index = Index::open(index_path)?;

    let home_dir = if cfg!(windows) {
        std::env::var("USERPROFILE")
    } else {
        std::env::var("HOME")
    }
    .unwrap();

    let mut builder = WalkBuilder::new(format!("{home_dir}"));

    let walk = builder.threads(2).build_parallel();

    let now = std::time::Instant::now();
    let mut scanner = Scanner {
        index: &index,
        file_count: Arc::new(AtomicU64::new(0)),
    };
    walk.visit(&mut scanner);

    println!(
        "Done scanning, scanned {} files in {} ms",
        scanner.file_count.load(Ordering::SeqCst),
        now.elapsed().as_millis()
    );

    let now = std::time::Instant::now();
    println!("Searching");
    let results = index.search("jpg", 500, 0)?;
    println!(
        "Found {} results in {} ms",
        results.len(),
        now.elapsed().as_millis()
    );

    Ok(())
}
