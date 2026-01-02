use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{atomic::AtomicU64, RwLock},
    time::SystemTime,
};

use fst::{IntoStreamer, Streamer};

use thiserror::Error;

mod matcher;
use matcher::*;
mod segmented_index;
use segmented_index::*;
mod opstamp;
use opstamp::*;

impl From<std::io::Error> for SegmentedIndexError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

pub struct Index {
    path: PathBuf,
    base: RwLock<SegmentedIndex>,
    next_op_seq: AtomicU64,
    mem_idx: RwLock<BTreeMap<String, Opstamp>>,
}

impl Index {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, IndexError> {
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
        })
    }

    fn next_op_seq(&self) -> u64 {
        self.next_op_seq
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn insert(&self, item: &str) -> Result<(), IndexError> {
        let seq = self.next_op_seq();
        self.mem_idx
            .write()
            .map_err(|_| IndexError::WriteLock)?
            .insert(item.to_string(), Opstamp::insertion(seq));
        Ok(())
    }

    pub fn delete(&self, item: &str) -> Result<(), IndexError> {
        let seq = self.next_op_seq();
        self.mem_idx
            .write()
            .map_err(|_| IndexError::WriteLock)?
            .insert(item.to_string(), Opstamp::deletion(seq));
        Ok(())
    }

    pub fn commit(&self) -> Result<(), IndexError> {
        let mut mem = self.mem_idx.write().map_err(|_| IndexError::WriteLock)?;

        if mem.is_empty() {
            return Ok(());
        };

        let segment_path = self.path.join(format!("{}.seg", self.next_op_seq()));

        self.base
            .write()
            .map_err(|_| IndexError::WriteLock)?
            .append_segment(&segment_path, std::mem::take(&mut *mem).into_iter())
            .map_err(IndexError::SegmentedIndex)?;

        self.base
            .write()
            .map_err(|_| IndexError::WriteLock)?
            .load(&segment_path)
            .map_err(IndexError::SegmentedIndex)?;

        Ok(())
    }

    pub fn search(&self, query: &str) -> Result<Vec<String>, IndexError> {
        let mut pattern = String::from("(?i)(?s).*");

        for ch in query.chars() {
            let escaped = regex_syntax::escape(&ch.to_string());
            pattern.push_str(&escaped);
            pattern.push_str(".*");
        }

        let matcher = RegexMatcher::new(&pattern).map_err(|e| IndexError::Regex(e.to_string()))?;
        let segments = self.base.read().map_err(|_| IndexError::ReadLock)?;
        let mem = self.mem_idx.read().map_err(|_| IndexError::ReadLock)?;

        let mut candidates: HashMap<String, Opstamp> = HashMap::new();

        for (k, v) in mem.iter() {
            if self.is_fuzzy_match(query, k) {
                candidates.insert(k.clone(), *v);
            }
        }

        for segment in segments.segments() {
            let mut stream = segment.search(&matcher).into_stream();
            while let Some((term, val)) = stream.next() {
                let val = Opstamp::from(val);
                let s = std::str::from_utf8(term).expect("invalid term");

                candidates
                    .entry(s.to_string())
                    .and_modify(|current| {
                        let current_seq = current.sequence();
                        let new_seq = val.sequence();
                        if new_seq > current_seq {
                            *current = val;
                        }
                    })
                    .or_insert(val);
            }
        }

        let mut results = Vec::new();
        for (k, v) in candidates {
            if !v.is_deletion() {
                results.push(k);
            }
        }

        results.sort();
        Ok(results)
    }

    fn is_fuzzy_match(&self, query: &str, target: &str) -> bool {
        let mut target_chars = target.chars();

        for query_char in query.chars() {
            if !target_chars.any(|c| c == query_char) {
                return false;
            }
        }

        true
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
}
