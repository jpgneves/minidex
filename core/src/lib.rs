use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    sync::RwLock,
};

use fst::{automaton::Subsequence, IntoStreamer, Map, Streamer};
use memmap2::Mmap;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Opstamp(u64);

impl Opstamp {
    const TOMBSTONE_BIT: u64 = 1 << 63;
    const SEQ_MASK: u64 = !Self::TOMBSTONE_BIT;

    #[inline]
    fn deletion(seq: u64) -> Self {
        Self(seq | Self::TOMBSTONE_BIT)
    }

    #[inline]
    fn insertion(seq: u64) -> Self {
        Self(seq & Self::SEQ_MASK)
    }

    #[inline]
    fn is_deletion(&self) -> bool {
        self.0 & Self::TOMBSTONE_BIT == 1
    }

    #[inline]
    fn sequence(&self) -> u64 {
        self.0 & Self::SEQ_MASK
    }
}

struct SegmentedIndex {
    segments: Vec<Map<Mmap>>,
}

impl SegmentedIndex {
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self, std::io::Error> {
        let mut segments = Vec::new();

        let entries = std::fs::read_dir(dir)?;

        for entry in entries.flatten() {
            let entry_file = std::fs::File::open(entry.path())?;
            let mmap = unsafe { Mmap::map(&entry_file)? };
            if let Ok(map) = Map::new(mmap) {
                segments.push(map);
            }
        }

        Ok(Self { segments })
    }
}

pub struct Index {
    base: RwLock<SegmentedIndex>,
    mem_idx: RwLock<BTreeMap<String, Opstamp>>,
}

impl Index {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, IndexError> {
        let base = SegmentedIndex::open(path)
            .map_err(IndexError::Open)
            .map(RwLock::new)?;
        let mem_idx = RwLock::new(BTreeMap::new());

        Ok(Self { base, mem_idx })
    }

    pub fn search(&self, query: &str) -> Result<Vec<String>, IndexError> {
        let matcher = Subsequence::new(query);
        let segments = self.base.read().map_err(|_| IndexError::ReadLock)?;
        let mem = self.mem_idx.read().map_err(|_| IndexError::ReadLock)?;

        let mut candidates: HashMap<String, Opstamp> = HashMap::new();

        for (k, v) in mem.iter() {
            if self.is_fuzzy_match(query, k) {
                candidates.insert(k.clone(), *v);
            }
        }

        for segment in segments.segments.iter() {
            let mut stream = segment.search(matcher.clone()).into_stream();
            while let Some((term, val)) = stream.next() {
                let val = Opstamp(val);
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
    #[error("failed to open index on disk")]
    Open(std::io::Error),
    #[error("failed to read lock data")]
    ReadLock,
}
