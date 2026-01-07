use std::collections::BTreeSet;

use ignore::{ParallelVisitor, ParallelVisitorBuilder, WalkBuilder, WalkState};
use minidex_core::{FilesystemEntry, Index, Kind, SearchResult};

struct Scanner<'a> {
    index: &'a Index,
}

impl<'s, 'a: 's> ParallelVisitorBuilder<'s> for Scanner<'a> {
    fn build(&mut self) -> Box<dyn ParallelVisitor + 's> {
        Box::new(Self { ..*self })
    }
}

impl<'a> ParallelVisitor for Scanner<'a> {
    fn visit(&mut self, entry: Result<ignore::DirEntry, ignore::Error>) -> WalkState {
        if let Ok(entry) = entry {
            let metadata = entry.metadata().unwrap();
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
                kind,
                last_modified,
                last_accessed,
            });
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

    let mut builder = WalkBuilder::new(format!("{home_dir}/Documents"));

    let walk = builder.threads(2).build_parallel();

    let mut scanner = Scanner { index: &index };
    walk.visit(&mut scanner);

    println!("Done scanning");

    println!("Searching");
    let results = index.search("jpg")?;
    println!("Results: {results:?}");

    Ok(())
}
