use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{Arc, RwLock, atomic::AtomicU64},
    thread::JoinHandle,
    time::SystemTime,
};

use fst::{Automaton as _, IntoStreamer as _, Streamer, automaton::Str};

use thiserror::Error;

mod common;
pub use common::Kind;
mod entry;
pub use entry::FilesystemEntry;
use entry::*;
mod segmented_index;
pub use segmented_index::compactor::*;
use segmented_index::*;
mod opstamp;
use opstamp::*;
use wal::Wal;
mod search;
mod tokenizer;
mod wal;
pub use search::{ScoringConfig, SearchOptions, SearchResult};

/// A Minidex Index, managing both the in-memory and disk data.
/// Insertions and deletions auto-commit to the Write-Ahead Log
/// and may trigger compaction.
pub struct Index {
    path: PathBuf,
    base: Arc<RwLock<SegmentedIndex>>,
    next_op_seq: AtomicU64,
    mem_idx: RwLock<BTreeMap<String, (String, IndexEntry)>>,
    wal: RwLock<Wal>,
    compactor_config: segmented_index::compactor::CompactorConfig,
    compactor: Arc<RwLock<Option<JoinHandle<()>>>>,
    flusher: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl Index {
    /// Open the index on disk with a default compactor configuration.
    /// This function will:
    /// 1. Create (if it doesn't exist) the directory at `path`
    /// 2. Try to obtain a lock on the directory
    /// 3. Load the discovered segments, data and posting
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, IndexError> {
        Self::open_with_config(path, CompactorConfig::default())
    }

    /// Open the index on disk with a custom compactor configuration.
    /// This function will:
    /// 1. Create (if it doesn't exist) the directory at `path`
    /// 2. Try to obtain a lock on the directory
    /// 3. Load the discovered segments, data and posting
    pub fn open_with_config<P: AsRef<Path>>(
        path: P,
        compactor_config: CompactorConfig,
    ) -> Result<Self, IndexError> {
        let base = SegmentedIndex::open(&path).map_err(IndexError::SegmentedIndex)?;

        let last_op = Self::generate_op_seq();

        let base = Arc::new(RwLock::new(base));
        let next_op_seq = AtomicU64::new(last_op);
        let mut mem_idx = BTreeMap::new();
        let wal_path = path.as_ref().join("journal.wal");

        let recovered = Wal::replay(&wal_path).map_err(IndexError::Io)?;
        for (path, volume, entry) in recovered {
            mem_idx.insert(path, (volume, entry));
        }

        let wal = Wal::open(&wal_path).map_err(IndexError::Io)?;

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            base,
            next_op_seq,
            mem_idx: RwLock::new(mem_idx),
            wal: RwLock::new(wal),
            compactor_config,
            compactor: Arc::new(RwLock::new(None)),
            flusher: Arc::new(RwLock::new(None)),
        })
    }

    fn next_op_seq(&self) -> u64 {
        self.next_op_seq
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Insert a filesystem entry into the index.
    pub fn insert(&self, item: FilesystemEntry) -> Result<(), IndexError> {
        let seq = self.next_op_seq();
        let path_str = item.path.to_string_lossy().to_string();
        let volume = item.volume;
        let entry = IndexEntry {
            opstamp: Opstamp::insertion(seq),
            kind: item.kind,
            last_modified: item.last_modified,
            last_accessed: item.last_accessed,
        };

        {
            let mut wal = self.wal.write().map_err(|_| IndexError::WriteLock)?;
            wal.append(&path_str, &volume, &entry)
                .map_err(IndexError::Io)?;
        }

        {
            self.mem_idx
                .write()
                .map_err(|_| IndexError::WriteLock)?
                .insert(path_str, (volume, entry));
        }

        if self.should_flush() {
            let _ = self.trigger_flush();
        }

        Ok(())
    }

    pub fn delete(&self, item: &Path) -> Result<(), IndexError> {
        let seq = self.next_op_seq();

        let path_str = item.to_string_lossy().to_string();
        let entry = IndexEntry {
            opstamp: Opstamp::deletion(seq),
            kind: Kind::File,
            last_modified: 0,
            last_accessed: 0,
        };

        {
            let mut wal = self.wal.write().map_err(|_| IndexError::WriteLock)?;
            wal.append(&path_str, "", &entry).map_err(IndexError::Io)?;
        }

        {
            self.mem_idx
                .write()
                .map_err(|_| IndexError::WriteLock)?
                .insert(path_str, ("".to_owned(), entry));
        }

        if self.should_flush() {
            let _ = self.trigger_flush();
        }

        Ok(())
    }

    /// Writes the in-memory index to disk.
    /// This method can fail if the disk is not writable.
    pub fn sync(&self) -> Result<(), IndexError> {
        let mut wal = self.wal.write().map_err(|_| IndexError::WriteLock)?;
        wal.flush().map_err(IndexError::Io)?;

        Ok(())
    }

    /// Search the index for the given search term (usually a path or
    /// file name), bound by limit and offset.
    pub fn search(
        &self,
        query: &str,
        limit: usize,
        offset: usize,
        options: SearchOptions<'_>,
    ) -> Result<Vec<SearchResult>, IndexError> {
        let mut tokens = crate::tokenizer::tokenize(query);

        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        tokens.sort_by_key(|b| std::cmp::Reverse(b.len()));

        let segments = self.base.read().map_err(|_| IndexError::ReadLock)?;
        let mem = self.mem_idx.read().map_err(|_| IndexError::ReadLock)?;

        let mut candidates: HashMap<String, (String, String, IndexEntry)> = HashMap::new();

        let required_matches = limit + offset;

        let short_circuit_threshold = std::cmp::max(5000, required_matches * 10);

        for (path, (volume, entry)) in mem.iter() {
            let normalized = path.to_lowercase();
            if let Some(filter) = options.volume_filter {
                if volume != filter {
                    continue;
                }
            }
            let matches_all = tokens.iter().all(|t| normalized.contains(t));
            if matches_all {
                candidates
                    .entry(normalized.clone())
                    .and_modify(|(current_path, current_volume, current_entry)| {
                        if entry.opstamp.sequence() > current_entry.opstamp.sequence() {
                            *current_entry = *entry;
                            *current_volume = volume.clone();
                            *current_path = path.clone()
                        }
                    })
                    .or_insert((path.clone(), volume.clone(), *entry));
            }
        }

        for segment in segments.segments() {
            let mut segment_doc_matches: Option<Vec<u64>> = if let Some(vol) = options.volume_filter
            {
                let vol_token = crate::tokenizer::synthesize_volume_token(&vol.to_lowercase());
                let map = segment.as_ref().as_ref();
                match map.get(&vol_token) {
                    Some(post_offset) => {
                        let mut docs = segment.read_posting_list(post_offset);
                        docs.sort_unstable();
                        docs.dedup();
                        Some(docs)
                    }
                    None => continue, // We can skip this segment since it has no entries for this volume
                }
            } else {
                None
            };

            for token in &tokens {
                if let Some(existing) = &segment_doc_matches
                    && existing.len() <= short_circuit_threshold
                {
                    break;
                }
                let matcher = Str::new(token).starts_with();

                let mut token_docs = Vec::new();
                let map = segment.as_ref().as_ref();
                let mut stream = map.search(&matcher).into_stream();

                while let Some((_, post_offset)) = stream.next() {
                    let docs = segment.read_posting_list(post_offset);
                    token_docs.extend(docs);

                    if segment_doc_matches.is_none() && token_docs.len() > short_circuit_threshold {
                        break;
                    }
                }

                token_docs.sort_unstable();
                token_docs.dedup();

                if let Some(mut existing) = segment_doc_matches {
                    existing.retain(|doc_id| token_docs.binary_search(doc_id).is_ok());
                    segment_doc_matches = Some(existing);
                } else {
                    segment_doc_matches = Some(token_docs);
                }

                if segment_doc_matches.as_ref().is_some_and(|m| m.is_empty()) {
                    break;
                }
            }

            if let Some(mut valid_docs) = segment_doc_matches {
                valid_docs.reverse();
                let mut resolved_count = 0;
                for dat_offset in valid_docs {
                    if resolved_count >= required_matches {
                        break;
                    }
                    if let Some((path, volume, entry)) = segment.read_document(dat_offset) {
                        let key = path.to_lowercase();
                        candidates
                            .entry(key)
                            .and_modify(|(current_path, current_volume, current_entry)| {
                                if entry.opstamp.sequence() > current_entry.opstamp.sequence() {
                                    *current_entry = entry;
                                    *current_volume = volume.clone();
                                    *current_path = path.clone();
                                }
                            })
                            .or_insert((path, volume, entry));

                        resolved_count += 1;
                    }
                }
            }
        }

        let mut results: Vec<_> = candidates
            .into_iter()
            .filter(|(_, (_, _, entry))| !entry.opstamp.is_deletion())
            .collect();

        // Rough top-k
        let scoring_cap = std::cmp::max(500, (offset + limit) * 10);
        if results.len() > scoring_cap {
            results.select_nth_unstable_by(scoring_cap, |a, b| {
                b.1.2.last_modified.cmp(&a.1.2.last_modified)
            });
            results.truncate(scoring_cap);
        }

        let now_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("failed to get system time")
            .as_micros() as f64;

        let config = if let Some(config) = options.scoring {
            config
        } else {
            &ScoringConfig::default()
        };

        let mut scored: Vec<_> = results
            .into_iter()
            .map(|(_, (path, volume, entry))| {
                let score = crate::search::compute_score(
                    config,
                    &path,
                    &tokens,
                    entry.last_modified,
                    entry.kind,
                    now_micros,
                );
                SearchResult {
                    path: PathBuf::from(path),
                    volume: volume,
                    kind: entry.kind,
                    last_modified: entry.last_modified,
                    last_accessed: entry.last_accessed,
                    score,
                }
            })
            .collect();

        scored.sort();

        let paginated_results = scored.into_iter().skip(offset).take(limit).collect();

        Ok(paginated_results)
    }

    /// Force index compaction, minimizing the amount of disk space
    /// utilized by the index.
    /// NOTE: this operation is very IO intensive and can take some time
    pub fn force_compact_all(&self) -> Result<(), IndexError> {
        if let Ok(mut flusher) = self.flusher.write()
            && let Some(handle) = flusher.take()
        {
            log::debug!("Waiting for background flush to finish...");
            let _ = handle.join();
        }

        if let Ok(mut compactor) = self.compactor.write()
            && let Some(handle) = compactor.take()
        {
            log::debug!("Waiting for background compactor to finish...");
            let _ = handle.join();
        }

        let snapshot = {
            let base = self.base.read().map_err(|_| IndexError::ReadLock)?;
            let segments = base.snapshot();

            // If we have 1 or 0 segments, the database is already perfectly compacted!
            if segments.len() <= 1 {
                log::debug!("Database is already fully compacted.");
                return Ok(());
            }
            segments
        };

        log::debug!("Forcing full compaction of {} segments...", snapshot.len());

        let compactor_seq = Self::generate_op_seq();

        let tmp_path = self.path.join(format!("{}.tmp", compactor_seq));

        compactor::merge_segments(&snapshot, tmp_path.clone())
            .map_err(|e| IndexError::Io(std::io::Error::other(e)))?;

        let mut base_guard = self.base.write().map_err(|_| IndexError::WriteLock)?;
        base_guard
            .apply_compaction(&snapshot, tmp_path)
            .map_err(|e| IndexError::Io(std::io::Error::other(e)))?;

        log::debug!("Full compaction complete");
        Ok(())
    }

    fn should_flush(&self) -> bool {
        self.mem_idx.read().unwrap().len() > 50_000
    }

    fn trigger_flush(&self) -> Result<(), IndexError> {
        if let Some(ref flusher) = *self.flusher.read().expect("failed to read flusher")
            && !flusher.is_finished()
        {
            return Ok(());
        }
        let mut mem = self.mem_idx.write().expect("failed to lock memory");
        let mut wal = self.wal.write().expect("failed to lock wal");

        if mem.is_empty() {
            return Ok(());
        }

        let snapshot = std::mem::take(&mut *mem);
        let path = self.path.clone();
        let next_seq = self.next_op_seq();

        let flushing_path = path.join(format!("journal.{}.flushing.wal", next_seq));
        wal.rotate(&flushing_path).map_err(IndexError::Io)?;

        drop(wal);
        drop(mem);

        let base = Arc::clone(&self.base);
        let min_merge_count = self.compactor_config.min_merge_count;
        let compactor_lock = Arc::clone(&self.compactor);

        let flusher = std::thread::Builder::new()
            .name("minidex-flush".to_owned())
            .spawn(move || {
                let final_segment_path = path.join(format!("{}", next_seq));
                let tmp_segment_path = path.join(format!("{}.tmp", next_seq));

                {
                    let mut base_guard = base.write().expect("failed to lock base");

                    if let Err(e) = base_guard.write_segment(
                        &tmp_segment_path,
                        snapshot
                            .into_iter()
                            .map(|(path, (volume, entry))| (path, volume, entry)),
                    ) {
                        log::error!("flush failed to write: {}", e);
                        let (tmp_seg, tmp_dat, tmp_post) = Segment::to_paths(&tmp_segment_path);
                        let _ = std::fs::remove_file(tmp_seg);
                        let _ = std::fs::remove_file(tmp_dat);
                        let _ = std::fs::remove_file(tmp_post);
                        return;
                    }

                    let (tmp_seg, tmp_dat, tmp_post) =
                        Segment::paths_with_additional_extension(&tmp_segment_path);

                    let (final_seg, final_dat, final_post) =
                        Segment::paths_with_additional_extension(&final_segment_path);

                    let _ = std::fs::rename(tmp_seg, final_seg);
                    let _ = std::fs::rename(tmp_dat, final_dat);
                    let _ = std::fs::rename(tmp_post, final_post);
                    base_guard
                        .load(&final_segment_path)
                        .expect("failed to reload segment during flush");
                }

                if let Err(e) = std::fs::remove_file(&flushing_path) {
                    log::error!("failed to delete rotated WAL: {}", e);
                }

                let snapshot = {
                    let base = base.read().expect("failed to read-lock base");
                    if base.segments().count() <= min_merge_count {
                        return;
                    }

                    base.snapshot()
                };

                let mut compactor_guard = compactor_lock
                    .write()
                    .expect("failed to acquire compactor write-lock");
                if let Some(handle) = compactor_guard.as_ref()
                    && !handle.is_finished()
                {
                    return;
                }

                *compactor_guard = Self::compact(base, path, snapshot);
            })
            .map_err(IndexError::Io)?;

        *self.flusher.write().unwrap() = Some(flusher);
        Ok(())
    }

    fn compact(
        base: Arc<RwLock<SegmentedIndex>>,
        path: PathBuf,
        snapshot: Vec<Arc<Segment>>,
    ) -> Option<JoinHandle<()>> {
        if snapshot.is_empty() {
            return None;
        }

        std::thread::Builder::new()
            .name("minidex-compactor".to_string())
            .spawn(move || {
                let next_seq = Self::generate_op_seq();
                let tmp_path = path.join(format!("{}.tmp", next_seq));

                log::debug!("Starting compaction with {} segments", snapshot.len());
                match compactor::merge_segments(&snapshot, tmp_path.clone()) {
                    Ok(_) => {
                        let mut base_guard = base
                            .write()
                            .expect("failed to lock base for compaction apply");
                        if let Err(e) = base_guard.apply_compaction(&snapshot, tmp_path) {
                            log::error!("Failed to apply compaction: {}", e);
                        }
                        log::debug!("Compaction finished");
                    }
                    Err(e) => log::error!("Compaction failed: {}", e),
                }
            })
            .ok()
    }

    fn generate_op_seq() -> u64 {
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }
}

impl Drop for Index {
    fn drop(&mut self) {
        let _ = self.sync();

        if let Ok(mut flusher) = self.flusher.write()
            && let Some(flusher) = flusher.take()
        {
            let _ = flusher.join();
        }

        if let Ok(mut compactor) = self.compactor.write()
            && let Some(compactor) = compactor.take()
        {
            let _ = compactor.join();
        }
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
