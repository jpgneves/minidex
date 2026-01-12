use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{RwLock, atomic::AtomicU64},
    thread::JoinHandle,
    time::SystemTime,
};

use fst::{IntoStreamer, Streamer};

use thiserror::Error;

mod common;
pub use common::Kind;
use common::*;
mod entry;
pub use entry::FilesystemEntry;
use entry::*;
mod matcher;
use matcher::*;
mod segmented_index;
use segmented_index::{compactor::CompactorConfig, *};
mod opstamp;
use opstamp::*;

pub struct Index {
    path: PathBuf,
    base: RwLock<SegmentedIndex>,
    next_op_seq: AtomicU64,
    mem_idx: RwLock<BTreeMap<String, IndexEntry>>,
    compactor_config: segmented_index::compactor::CompactorConfig,
    compactor: RwLock<Option<JoinHandle<()>>>,
}

impl Index {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, IndexError> {
        Self::open_with_config(path, CompactorConfig::default())
    }

    pub fn open_with_config<P: AsRef<Path>>(
        path: P,
        compactor_config: CompactorConfig,
    ) -> Result<Self, IndexError> {
        let (base, last_op) = SegmentedIndex::open(&path).map_err(IndexError::SegmentedIndex)?;

        let last_op = last_op.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64
        });

        let base = RwLock::new(base);
        let next_op_seq = AtomicU64::new(last_op);
        let mem_idx = RwLock::new(BTreeMap::new());

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            base,
            next_op_seq,
            mem_idx,
            compactor_config,
            compactor: RwLock::new(None),
        })
    }

    fn next_op_seq(&self) -> u64 {
        self.next_op_seq
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn insert(&self, item: FilesystemEntry) -> Result<(), IndexError> {
        let seq = self.next_op_seq();
        self.mem_idx
            .write()
            .map_err(|_| IndexError::WriteLock)?
            .insert(
                item.path.to_string_lossy().to_string(),
                IndexEntry {
                    opstamp: Opstamp::insertion(seq),
                    kind: item.kind,
                    content_type: 0,
                    last_modified: item.last_modified,
                    last_accessed: item.last_accessed,
                },
            );

        if let Ok(true) = self.should_compact() {
            if let Err(e) = self.compact() {
                eprintln!("Failed to compact: {}", e);
            }
        }
        Ok(())
    }

    pub fn delete(&self, item: &PathBuf) -> Result<(), IndexError> {
        let seq = self.next_op_seq();
        self.mem_idx
            .write()
            .map_err(|_| IndexError::WriteLock)?
            .insert(
                item.to_string_lossy().to_string(),
                IndexEntry {
                    opstamp: Opstamp::deletion(seq),
                    kind: Kind::File,
                    content_type: 0,
                    last_modified: 0,
                    last_accessed: 0,
                },
            );
        Ok(())
    }

    pub fn commit(&self) -> Result<(), IndexError> {
        let mut mem = self.mem_idx.write().map_err(|_| IndexError::WriteLock)?;

        if mem.is_empty() {
            return Ok(());
        };

        let segment_path = self.path.join(format!("{}", self.next_op_seq()));

        let mut base = self.base.write().map_err(|_| IndexError::WriteLock)?;
        base.write_segment(&segment_path, std::mem::take(&mut *mem).into_iter())
            .map_err(IndexError::SegmentedIndex)?;

        base.load(&segment_path)
            .map_err(IndexError::SegmentedIndex)?;

        base.save_last_op(self.next_op_seq.load(std::sync::atomic::Ordering::SeqCst))
            .map_err(IndexError::SegmentedIndex)?;

        Ok(())
    }

    pub fn rollback(&self) -> Result<(), IndexError> {
        let mut mem = self.mem_idx.write().map_err(|_| IndexError::WriteLock)?;

        mem.clear();

        Ok(())
    }

    pub fn search(&self, query: &str) -> Result<Vec<SearchResult>, IndexError> {
        let mut pattern = String::from("(?i)(?s).*");

        for word in query.split_whitespace() {
            for ch in word.chars() {
                let escaped = regex_syntax::escape(&ch.to_string());
                pattern.push_str(&escaped);
            }
            pattern.push_str(".*");
        }

        let matcher = RegexMatcher::new(&pattern).map_err(|e| IndexError::Regex(e.to_string()))?;
        let segments = self.base.read().map_err(|_| IndexError::ReadLock)?;
        let mem = self.mem_idx.read().map_err(|_| IndexError::ReadLock)?;

        let mut candidates: HashMap<String, (String, IndexEntry)> = HashMap::new();

        for (path, entry) in mem.iter() {
            if matcher.is_match(path) {
                let normalized = path.to_lowercase();

                candidates
                    .entry(normalized)
                    .and_modify(|(current_path, current_entry)| {
                        if entry.opstamp.sequence() > current_entry.opstamp.sequence() {
                            *current_entry = *entry;
                            *current_path = path.clone()
                        }
                    })
                    .or_insert((path.clone(), *entry));
            }
        }

        for segment in segments.segments() {
            let mut stream = segment.as_ref().as_ref().search(&matcher).into_stream();
            while let Some((term, offset)) = stream.next() {
                if let Some(entry) = segment.get_entry(offset) {
                    let path = std::str::from_utf8(term).expect("invalid term").to_string();

                    let key = path.to_lowercase();
                    candidates
                        .entry(key)
                        .and_modify(|(current_path, current_entry)| {
                            let current_seq = current_entry.opstamp.sequence();
                            let new_seq = entry.opstamp.sequence();
                            if new_seq > current_seq {
                                *current_entry = entry;
                                *current_path = path.clone();
                            }
                        })
                        .or_insert((path, entry));
                }
            }
        }

        let mut results = Vec::new();
        for (_, (path, entry)) in candidates {
            if !entry.opstamp.is_deletion() {
                results.push(SearchResult {
                    path: PathBuf::from(path),
                    kind: entry.kind,
                    last_modified: entry.last_modified,
                    last_accessed: entry.last_accessed,
                });
            }
        }

        results.sort();
        Ok(results)
    }

    fn compact(&self) -> Result<(), IndexError> {
        let mut compactor = self
            .compactor
            .write()
            .expect("failed to get compactor lock");
        let snapshot = {
            let base = self.base.read().map_err(|_| IndexError::ReadLock)?;
            base.snapshot()
        };

        if snapshot.is_empty() {
            return Ok(());
        }

        let path = self.path.clone();
        let next_seq = self.next_op_seq();

        *compactor = Some(
            std::thread::Builder::new()
                .name("minidex-compactor".to_string())
                .spawn(move || {
                    let tmp_path = path.join(&format!("{}.tmp", next_seq));

                    println!("Starting compaction with {} segments", snapshot.len());
                    match compactor::merge_segments(&snapshot, tmp_path.clone()) {
                        Ok(_) => {}
                        Err(e) => eprintln!("Compaction failed: {}", e),
                    }
                })
                .map_err(IndexError::Io)?,
        );

        Ok(())
    }

    fn should_compact(&self) -> Result<bool, IndexError> {
        if let Some(ref compactor) = *self.compactor.read().expect("failed to get compactor")
            && !compactor.is_finished()
        {
            return Ok(false);
        }
        Ok(self
            .base
            .read()
            .map_err(|_| IndexError::ReadLock)?
            .segments()
            .count()
            > self.compactor_config.min_merge_count)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct SearchResult {
    pub path: PathBuf,
    pub kind: Kind,
    pub last_modified: u64,
    pub last_accessed: u64,
}

impl Ord for SearchResult {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .last_modified
            .cmp(&self.last_modified)
            .then_with(|| self.kind.cmp(&other.kind))
            .then_with(|| self.path.cmp(&other.path))
    }
}

impl PartialOrd for SearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("failed to open index on disk: {0}")]
    Open(std::io::Error),
    #[error("failed to read lock data")]
    ReadLock,
    #[error("failed to write lock data")]
    WriteLock,
    #[error(transparent)]
    SegmentedIndex(SegmentedIndexError),
    #[error("failed to compile matching regex: {0}")]
    Regex(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
