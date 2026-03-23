use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    thread::JoinHandle,
};

use common::is_tombstoned;
use fst::{Automaton as _, IntoStreamer as _, Streamer, automaton::Str};

use thiserror::Error;

mod collector;
mod common;
mod leb128;
use collector::*;
pub use common::{Kind, VolumeType, category};
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
pub use tokenizer::tokenize;
mod wal;
pub use search::{ScoringConfig, ScoringInputs, ScoringWeights, SearchOptions, SearchResult};

pub type Tombstone = (Option<String>, String, u64);

/// A Minidex Index, managing both the in-memory and disk data.
/// Insertions and deletions auto-commit to the Write-Ahead Log
/// and may trigger compaction.
pub struct Index {
    path: PathBuf,
    base: Arc<RwLock<SegmentedIndex>>,
    next_op_seq: Arc<AtomicU64>,
    mem_idx: RwLock<BTreeMap<String, (String, IndexEntry)>>,
    wal: RwLock<Wal>,
    compactor_config: segmented_index::compactor::CompactorConfig,
    compactor: Arc<RwLock<Option<JoinHandle<()>>>>,
    flusher: Arc<RwLock<Option<JoinHandle<()>>>>,
    prefix_tombstones: Arc<RwLock<Arc<Vec<Tombstone>>>>,
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

        let base = Arc::new(RwLock::new(base));
        let mut max_seq = 0u64;
        let mut mem_idx = BTreeMap::new();

        let mut prefix_tombstones = Vec::new();

        let mut apply_replay = |replay_data: crate::wal::ReplayData| {
            for (path, volume, entry) in replay_data.inserts {
                max_seq = max_seq.max(entry.opstamp.sequence());
                mem_idx.insert(path, (volume, entry));
            }
            for (volume, prefix, seq) in replay_data.tombstones {
                max_seq = max_seq.max(seq);
                prefix_tombstones.push((volume, prefix, seq));
            }
        };

        let entries = path.as_ref().read_dir().map_err(IndexError::Io)?;

        let mut flushing_wals = Vec::new();

        // Recover partial, flushing WAL files
        for entry in entries {
            if let Ok(e) = entry
                && let Ok(file_type) = e.file_type()
                && file_type.is_file()
                && e.file_name().to_string_lossy().ends_with(".flushing.wal")
            {
                flushing_wals.push(e.path());
            }
        }
        flushing_wals.sort_unstable();

        for wal_path in flushing_wals {
            let partial = Wal::replay(wal_path).map_err(IndexError::Io)?;

            apply_replay(partial);
        }

        let wal_path = path.as_ref().join("journal.wal");

        let recovered = Wal::replay(&wal_path).map_err(IndexError::Io)?;
        apply_replay(recovered);

        let next_op_seq = Arc::new(AtomicU64::new(max_seq + 1));

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
            prefix_tombstones: Arc::new(RwLock::new(Arc::new(prefix_tombstones))),
        })
    }

    fn next_op_seq(&self) -> u64 {
        self.next_op_seq
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Insert a filesystem entry into the index.
    pub fn insert(&self, item: FilesystemEntry) -> Result<(), IndexError> {
        let threshold = self.compactor_config.flush_threshold * 3;

        // Backpressure mechanism - block inserts if we're blowing through
        // the flushing threshold.
        if self.mem_idx.read().map_err(|_| IndexError::ReadLock)?.len() > threshold {
            let flusher = {
                self.flusher
                    .write()
                    .map_err(|_| IndexError::WriteLock)?
                    .take()
            };

            if let Some(handle) = flusher {
                let _ = handle.join();
            }

            let _ = self.trigger_flush();
        }

        let seq = self.next_op_seq();
        let path_str = item.path.to_string_lossy().to_string();
        let volume = item.volume;
        let entry = IndexEntry {
            opstamp: Opstamp::insertion(seq),
            kind: item.kind,
            last_modified: item.last_modified,
            last_accessed: item.last_accessed,
            category: item.category,
            volume_type: item.volume_type,
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
            category: 0,
            volume_type: common::VolumeType::Local,
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

    /// Deletes all index entries under the given prefix, across all volumes
    pub fn delete_prefix(&self, prefix: &str) -> Result<(), IndexError> {
        self.delete_by_volume_name(None, prefix)
    }

    /// Deletes all index items under the given prefix,
    /// belonging to the given volume. If volume is `None`, we delete
    /// all entries for the prefix across all volumes.
    pub fn delete_by_volume_name(
        &self,
        volume: Option<&str>,
        prefix: &str,
    ) -> Result<(), IndexError> {
        let seq = self.next_op_seq.fetch_add(1, Ordering::SeqCst);
        let normalized_prefix = prefix
            .replace(['/', '\\'], std::path::MAIN_SEPARATOR_STR)
            .to_lowercase();
        {
            let mut tombstones = self
                .prefix_tombstones
                .write()
                .map_err(|_| IndexError::WriteLock)?;

            Arc::make_mut(&mut tombstones).push((
                volume.map(|s| s.to_string()),
                normalized_prefix.clone(),
                seq,
            ));
        }

        {
            let mut wal = self.wal.write().map_err(|_| IndexError::WriteLock)?;

            wal.write_prefix_tombstone(volume, &normalized_prefix, seq)?;
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

        let query_lower = query.to_lowercase();
        let raw_query_tokens: Vec<&str> = query_lower
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .collect();

        tokens.sort_by_key(|b| std::cmp::Reverse(b.len()));

        let segments = self.base.read().map_err(|_| IndexError::ReadLock)?;
        let mem = self.mem_idx.read().map_err(|_| IndexError::ReadLock)?;

        let required_matches = limit + offset;
        let scoring_cap = std::cmp::max(500, required_matches * 3).min(1000);

        let active_tombstones = self
            .prefix_tombstones
            .read()
            .map_err(|_| IndexError::ReadLock)?
            .clone();

        let mut collector = LsmCollector::new(&active_tombstones);

        let volume_type_mask = Self::compile_allowed_volume_mask(options.volume_type);

        // In-memory searches
        for (path, (volume, entry)) in mem.iter() {
            let path_bytes = path.as_bytes();

            if let Some(filter) = options.volume_name
                && volume != filter
            {
                continue;
            }

            if let Some(category) = options.category
                && entry.category & category == 0
            {
                continue;
            }

            if let Some(kind) = options.kind
                && entry.kind != kind
            {
                continue;
            }

            if (volume_type_mask & (1 << entry.volume_type as u8)) == 0 {
                continue;
            }

            let matches_all = if path.is_ascii() {
                tokens.iter().all(|t| {
                    let token_bytes = t.as_bytes();
                    if path_bytes.len() < token_bytes.len() {
                        return false;
                    }
                    path_bytes
                        .windows(token_bytes.len())
                        .enumerate()
                        .any(|(idx, window)| {
                            if window.eq_ignore_ascii_case(token_bytes) {
                                if idx == 0 {
                                    true
                                } else {
                                    !path_bytes[idx - 1].is_ascii_alphanumeric()
                                }
                            } else {
                                false
                            }
                        })
                })
            } else {
                let folded_path = crate::tokenizer::fold_path(path);
                tokens.iter().all(|t| {
                    folded_path.match_indices(t.as_str()).any(|(idx, _)| {
                        if idx == 0 {
                            true
                        } else {
                            !folded_path[..idx].chars().last().unwrap().is_alphanumeric()
                        }
                    })
                })
            };

            if matches_all {
                collector.insert(path.to_string(), volume.clone(), *entry);
            }
        }

        // Disk searches
        let mut token_docs = Vec::new();
        let mut current_matches = Vec::new();
        let mut swap_buffer = Vec::new();

        let vol_token = options
            .volume_name
            .map(|vol| crate::tokenizer::synthesize_volume_token(&vol.to_lowercase()));

        for segment in segments.segments() {
            current_matches.clear();
            let mut first_token = true;
            let mut valid_matches = true;

            if let Some(ref vol_token) = vol_token {
                let map = segment.as_ref().as_ref();
                if let Some(post_offset) = map.get(vol_token) {
                    segment.append_posting_list(post_offset, &mut current_matches);
                    current_matches.sort_unstable();
                    current_matches.dedup();
                    first_token = false;
                } else {
                    continue;
                }
            }

            for token in &tokens {
                // Skip on 0 matches
                if !first_token && current_matches.is_empty() {
                    valid_matches = false;
                    break;
                }

                let matcher = Str::new(token).starts_with();

                token_docs.clear();
                let map = segment.as_ref().as_ref();
                let mut stream = map.search(&matcher).into_stream();

                while let Some((_, post_offset)) = stream.next() {
                    segment.append_posting_list(post_offset, &mut token_docs);
                }

                token_docs.sort_unstable();
                token_docs.dedup();

                if first_token {
                    std::mem::swap(&mut current_matches, &mut token_docs);
                    first_token = false;
                } else {
                    let t_len = token_docs.len();
                    let c_len = current_matches.len();

                    if c_len * 10 < t_len || t_len * 10 < c_len {
                        if c_len > t_len {
                            // current_matches is massive, so we iterate on token docs isnstead
                            swap_buffer.clear();

                            for &doc_id in &token_docs {
                                if current_matches.binary_search(&doc_id).is_ok() {
                                    swap_buffer.push(doc_id);
                                }
                            }
                            std::mem::swap(&mut current_matches, &mut swap_buffer);
                        } else {
                            current_matches
                                .retain(|doc_id| token_docs.binary_search(doc_id).is_ok());
                        }
                    } else {
                        // O(N+M) Two-Pointer traversal otherwise
                        let mut j = 0;
                        current_matches.retain(|&doc_id| {
                            while j < t_len && token_docs[j] < doc_id {
                                j += 1;
                            }
                            j < t_len && token_docs[j] == doc_id
                        })
                    }
                }
            }

            if valid_matches && !current_matches.is_empty() {
                let valid_docs = &current_matches;
                let mut enriched_docs: Vec<u128> = Vec::with_capacity(valid_docs.len());
                let meta_mmap = segment.meta_map();

                for &doc_id in valid_docs {
                    let byte_offset = (doc_id as usize) * size_of::<u128>();
                    let packed_bytes: [u8; 16] = meta_mmap
                        [byte_offset..byte_offset + size_of::<u128>()]
                        .try_into()
                        .expect("failed to unpack");
                    let packed_val = u128::from_le_bytes(packed_bytes);

                    let (_, _, _, _, is_dir, doc_category, vol_type) =
                        SegmentedIndex::unpack_u128(packed_val);

                    // Kind filter
                    if let Some(kind) = options.kind {
                        let is_target_dir = kind == Kind::Directory;
                        if is_dir != is_target_dir {
                            continue;
                        }
                    }

                    // Filter categories
                    if let Some(category) = options.category
                        && doc_category & category == 0
                    {
                        continue;
                    }

                    // Filter volume type
                    if (volume_type_mask & (1 << vol_type)) == 0 {
                        continue;
                    }

                    enriched_docs.push(packed_val);
                }

                if enriched_docs.len() > scoring_cap {
                    // O(N) quickselect
                    enriched_docs.select_nth_unstable_by(scoring_cap, |&a, &b| {
                        let (_, a_modified_at, _, a_depth, a_dir, _, _) =
                            SegmentedIndex::unpack_u128(a);
                        let (_, b_modified_at, _, b_depth, b_dir, _, _) =
                            SegmentedIndex::unpack_u128(b);

                        b_dir
                            .cmp(&a_dir)
                            .then_with(|| a_depth.cmp(&b_depth))
                            .then_with(|| b_modified_at.cmp(&a_modified_at))
                    });

                    enriched_docs.truncate(scoring_cap);
                }

                // Re-sort by dat_offset ascending to align with in-disk layout
                enriched_docs.sort_unstable_by_key(|&packed| {
                    let (dat_offset, _, _, _, _, _, _) = SegmentedIndex::unpack_u128(packed);
                    dat_offset
                });

                for packed_val in enriched_docs {
                    let (dat_offset, _, _, _, _, _, _) = SegmentedIndex::unpack_u128(packed_val);

                    if let Some((path, volume, entry)) = segment.read_document(dat_offset) {
                        collector.insert(path, volume, entry);
                    }
                }
            }
        }

        let mut results: Vec<_> = collector.finish().collect();

        // Rough top-k
        if results.len() > scoring_cap {
            results.select_nth_unstable_by(scoring_cap, |a, b| {
                b.2.last_modified.cmp(&a.2.last_modified)
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

        let weights = config.weights.unwrap_or_default();
        let mut scored: Vec<_> = results
            .into_iter()
            .map(|(path, volume, entry)| {
                let inputs = ScoringInputs {
                    path: &path,
                    query_tokens: &tokens,
                    raw_query_tokens: &raw_query_tokens,
                    last_modified: entry.last_modified,
                    last_accessed: entry.last_accessed,
                    kind: entry.kind,
                    now_micros,
                };

                let score = (config.scoring_fn)(&weights, &inputs);

                SearchResult {
                    path: PathBuf::from(path),
                    volume,
                    volume_type: entry.volume_type,
                    kind: entry.kind,
                    last_modified: entry.last_modified,
                    last_accessed: entry.last_accessed,
                    category: entry.category,
                    score,
                }
            })
            .collect();

        scored.sort();

        let paginated_results = scored.into_iter().skip(offset).take(limit).collect();

        Ok(paginated_results)
    }

    /// Retrieve all indexed files last accessed until the given timestamp (in seconds).
    pub fn recent_files(
        &self,
        since: u64, // Renamed 'until' to 'since' for clarity
        limit: usize,
        offset: usize,
        options: SearchOptions<'_>,
    ) -> Result<Vec<SearchResult>, IndexError> {
        let segments = self.base.read().unwrap();
        let mem = self.mem_idx.read().unwrap();

        let active_tombstones = self
            .prefix_tombstones
            .read()
            .map_err(|_| IndexError::ReadLock)?
            .clone();

        let mut collector = LsmCollector::new(&active_tombstones);

        let volume_type_mask = Self::compile_allowed_volume_mask(options.volume_type);

        for (path, (volume, entry)) in mem.iter() {
            if entry.last_accessed >= since {
                if let Some(filter) = options.volume_name
                    && volume != filter
                {
                    continue;
                }
                if let Some(category) = options.category
                    && entry.category & category == 0
                {
                    continue;
                }
                if let Some(kind) = options.kind
                    && entry.kind != kind
                {
                    continue;
                }
                if (volume_type_mask & (1 << entry.volume_type as u8)) == 0 {
                    continue;
                }
                collector.insert(path.clone(), volume.clone(), *entry);
            }
        }

        let required_matches = offset + limit;
        // Buffer to account for items that might be filtered out by volume or tombstones
        let disk_cap = required_matches + 500;

        let mut disk_candidates: Vec<(&std::sync::Arc<Segment>, u128)> = Vec::new();

        for segment in segments.segments() {
            let meta_mmap = segment.meta_map();

            for chunk in meta_mmap.chunks_exact(16) {
                let packed = u128::from_le_bytes(chunk.try_into().unwrap());
                let (_, _, accessed, _, is_dir, doc_category, doc_vol_type) =
                    SegmentedIndex::unpack_u128(packed);

                if accessed >= since {
                    if let Some(target_kind) = options.kind {
                        let is_target_dir = target_kind == Kind::Directory;
                        if is_dir != is_target_dir {
                            continue;
                        }
                    }

                    if let Some(category) = options.category
                        && doc_category & category == 0
                    {
                        continue;
                    }

                    if (volume_type_mask & (1 << doc_vol_type)) == 0 {
                        continue;
                    }

                    // DO NOT read the document yet! Just save the integer.
                    disk_candidates.push((segment, packed));
                }
            }
        }

        if disk_candidates.len() > disk_cap {
            disk_candidates.select_nth_unstable_by(disk_cap, |a, b| {
                let (_, _, a_acc, _, _, _, _) = SegmentedIndex::unpack_u128(a.1);
                let (_, _, b_acc, _, _, _, _) = SegmentedIndex::unpack_u128(b.1);
                b_acc.cmp(&a_acc) // Sort descending by access time
            });
            disk_candidates.truncate(disk_cap);
        }

        for (segment, packed) in disk_candidates {
            let (dat_offset, _, _, _, _, _, _) = SegmentedIndex::unpack_u128(packed);

            if let Some((path, volume, entry)) = segment.read_document(dat_offset) {
                if let Some(filter) = options.volume_name
                    && volume != filter
                {
                    continue;
                }
                collector.insert(path, volume, entry);
            }
        }

        let mut results: Vec<_> = collector.finish().collect();

        if results.len() > required_matches {
            results.select_nth_unstable_by(required_matches, |a, b| {
                b.2.last_accessed.cmp(&a.2.last_accessed)
            });
            results.truncate(required_matches);
        }

        results.sort_unstable_by(|a, b| b.2.last_accessed.cmp(&a.2.last_accessed));

        let paginated_results = results
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|(path, volume, entry)| SearchResult {
                path: PathBuf::from(path),
                volume,
                volume_type: entry.volume_type,
                kind: entry.kind,
                last_modified: entry.last_modified,
                last_accessed: entry.last_accessed,
                category: entry.category,
                score: 0.0,
            })
            .collect();

        Ok(paginated_results)
    }

    /// Force index compaction, minimizing the amount of disk space
    /// utilized by the index.
    /// NOTE: this operation is very IO intensive and can take some time
    pub fn force_compact_all(&self) -> Result<(), IndexError> {
        // Force all data to be flushed before proceeding
        loop {
            if let Ok(mut flusher) = self.flusher.write()
                && let Some(handle) = flusher.take()
            {
                log::debug!("Waiting for background flush to finish...");
                let _ = handle.join();
            }

            if self
                .mem_idx
                .read()
                .map_err(|_| IndexError::ReadLock)?
                .is_empty()
            {
                break;
            }

            self.trigger_flush()?;
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

        let compactor_seq = self.next_op_seq.fetch_add(1, Ordering::SeqCst);

        let tmp_path = self.path.join(format!("{}.tmp", compactor_seq));

        let snapshot_tombstones = {
            let guard = self.prefix_tombstones.read().expect("lock poisoned");
            guard.clone()
        };

        compactor::merge_segments(&snapshot, snapshot_tombstones, tmp_path.clone())
            .map_err(|e| IndexError::Io(std::io::Error::other(e)))?;

        let mut base_guard = self.base.write().map_err(|_| IndexError::WriteLock)?;
        base_guard
            .apply_compaction(&snapshot, tmp_path)
            .map_err(|e| IndexError::Io(std::io::Error::other(e)))?;

        log::debug!("Full compaction complete");
        Ok(())
    }

    fn should_flush(&self) -> bool {
        self.mem_idx.read().unwrap().len() > self.compactor_config.flush_threshold
            || self.prefix_tombstones.read().unwrap().len()
                > self.compactor_config.tombstone_threshold
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

        // Re-write tombstones to the WAL until a full compaction runs.
        let tombstones_cow = { self.prefix_tombstones.read().unwrap().clone() };
        for (volume, prefix, seq) in tombstones_cow.iter() {
            wal.write_prefix_tombstone(volume.as_deref(), prefix, *seq)?;
        }

        drop(wal);
        drop(mem);

        let base = Arc::clone(&self.base);
        let min_merge_count = self.compactor_config.min_merge_count;
        let compactor_lock = Arc::clone(&self.compactor);
        let op_seq = Arc::clone(&self.next_op_seq);
        let prefix_tombstones = Arc::clone(&self.prefix_tombstones);

        let flusher = std::thread::Builder::new()
            .name("minidex-flush".to_owned())
            .spawn(move || {
                let final_segment_path = path.join(format!("{}", next_seq));
                let tmp_segment_path = path.join(format!("{}.tmp", next_seq));

                if let Err(e) = SegmentedIndex::build_segment_files(
                    &tmp_segment_path,
                    snapshot
                        .into_iter()
                        .map(|(path, (volume, entry))| (path, volume, entry)),
                    false,
                ) {
                    log::error!("flush failed to write: {}", e);
                    let tmp_paths = Segment::paths_with_additional_extension(&tmp_segment_path);
                    Segment::remove_files(&tmp_paths);
                    return;
                }

                let tmp_paths = Segment::paths_with_additional_extension(&tmp_segment_path);

                let final_paths = Segment::paths_with_additional_extension(&final_segment_path);

                let _ = Segment::rename_files(&tmp_paths, &final_paths);

                let new_segment =
                    Arc::new(Segment::load(final_segment_path).expect("failed to load"));
                {
                    let mut base_guard = base.write().expect("failed to lock base");
                    base_guard.add_segment(new_segment);
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

                *compactor_guard = Self::compact(base, path, snapshot, prefix_tombstones, op_seq);
            })
            .map_err(IndexError::Io)?;

        *self.flusher.write().unwrap() = Some(flusher);
        Ok(())
    }

    fn compact(
        base: Arc<RwLock<SegmentedIndex>>,
        path: PathBuf,
        snapshot: Vec<Arc<Segment>>,
        prefix_tombstones: Arc<RwLock<Arc<Vec<Tombstone>>>>,
        next_op_seq: Arc<AtomicU64>,
    ) -> Option<JoinHandle<()>> {
        if snapshot.is_empty() {
            return None;
        }

        std::thread::Builder::new()
            .name("minidex-compactor".to_string())
            .spawn(move || {
                let next_seq = next_op_seq.fetch_add(1, Ordering::SeqCst);
                let tmp_path = path.join(format!("{}.tmp", next_seq));

                log::debug!("Starting compaction with {} segments", snapshot.len());
                let snapshot_tombstones = { prefix_tombstones.read().unwrap().clone() };
                match compactor::merge_segments(&snapshot, snapshot_tombstones, tmp_path.clone()) {
                    Ok(compactor_seq) => {
                        let mut base_guard = base
                            .write()
                            .expect("failed to lock base for compaction apply");
                        if let Err(e) = base_guard.apply_compaction(&snapshot, tmp_path) {
                            log::error!("Failed to apply compaction: {}", e);
                        }
                        let mut tombstones = prefix_tombstones.write().unwrap();
                        Arc::make_mut(&mut tombstones).retain(|(_, _, seq)| *seq >= compactor_seq);

                        log::debug!("Compaction finished");
                    }
                    Err(e) => log::error!("Compaction failed: {}", e),
                }
            })
            .ok()
    }

    fn compile_allowed_volume_mask(allowed_volume_types: Option<&[VolumeType]>) -> u8 {
        match allowed_volume_types {
            Some(allowed) => allowed.iter().fold(0, |acc, &vt| acc | (1 << (vt as u8))),
            None => 0b0000_1111,
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{VolumeType, category};

    #[test]
    fn test_index_basic_lifecycle() -> Result<(), IndexError> {
        let temp_dir = std::env::temp_dir().join(format!("minidex_test_lib_{}", rand_id()));
        std::fs::create_dir_all(&temp_dir)?;

        let sep = std::path::MAIN_SEPARATOR_STR;
        let path1 = format!("{}foo{}bar.txt", sep, sep);

        {
            let index = Index::open(&temp_dir)?;
            index.insert(FilesystemEntry {
                path: PathBuf::from(&path1),
                volume: "vol1".to_string(),
                kind: Kind::File,
                last_modified: 100,
                last_accessed: 100,
                category: category::TEXT,
                volume_type: VolumeType::Local,
            })?;

            let results = index.search("bar", 10, 0, SearchOptions::default())?;
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].path, PathBuf::from(&path1));

            index.sync()?;
        }

        // Reopen index and verify data is still there
        {
            let index = Index::open(&temp_dir)?;
            let results = index.search("bar", 10, 0, SearchOptions::default())?;
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].path, PathBuf::from(&path1));
        }

        std::fs::remove_dir_all(temp_dir)?;
        Ok(())
    }

    #[test]
    fn test_index_flush_and_search() -> Result<(), IndexError> {
        let temp_dir = std::env::temp_dir().join(format!("minidex_test_lib_flush_{}", rand_id()));
        std::fs::create_dir_all(&temp_dir)?;

        let config = CompactorConfig {
            flush_threshold: 1,
            ..Default::default()
        };

        let sep = std::path::MAIN_SEPARATOR_STR;

        let index = Index::open_with_config(&temp_dir, config)?;
        index.insert(FilesystemEntry {
            path: PathBuf::from(format!("{}foo{}a.txt", sep, sep)),
            volume: "vol1".to_string(),
            kind: Kind::File,
            last_modified: 100,
            last_accessed: 100,
            category: category::TEXT,
            volume_type: VolumeType::Local,
        })?;

        // This insert should trigger a flush in the background
        index.insert(FilesystemEntry {
            path: PathBuf::from(format!("{}foo{}b.txt", sep, sep)),
            volume: "vol1".to_string(),
            kind: Kind::File,
            last_modified: 100,
            last_accessed: 100,
            category: category::TEXT,
            volume_type: VolumeType::Local,
        })?;

        // Wait a bit for background flush
        std::thread::sleep(std::time::Duration::from_millis(500));

        let results = index.search("foo", 10, 0, SearchOptions::default())?;
        assert_eq!(results.len(), 2);

        std::fs::remove_dir_all(temp_dir)?;
        Ok(())
    }

    #[test]
    fn test_index_prefix_delete() -> Result<(), IndexError> {
        let temp_dir = std::env::temp_dir().join(format!("minidex_test_lib_del_{}", rand_id()));
        std::fs::create_dir_all(&temp_dir)?;

        let sep = std::path::MAIN_SEPARATOR_STR;

        let index = Index::open(&temp_dir)?;
        index.insert(FilesystemEntry {
            path: PathBuf::from(format!("{}foo{}bar{}a.txt", sep, sep, sep)),
            volume: "vol1".to_string(),
            kind: Kind::File,
            last_modified: 100,
            last_accessed: 100,
            category: 0,
            volume_type: VolumeType::Local,
        })?;
        let other_path = format!("{}other{}b.txt", sep, sep);
        index.insert(FilesystemEntry {
            path: PathBuf::from(&other_path),
            volume: "vol1".to_string(),
            kind: Kind::File,
            last_modified: 100,
            last_accessed: 100,
            category: 0,
            volume_type: VolumeType::Local,
        })?;

        // Delete everything under /foo
        index.delete_prefix(&format!("{}foo", sep))?;

        let results = index.search("txt", 10, 0, SearchOptions::default())?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].path, PathBuf::from(&other_path));

        std::fs::remove_dir_all(temp_dir)?;
        Ok(())
    }

    #[test]
    fn test_index_volume_prefix_delete() -> Result<(), IndexError> {
        let temp_dir = std::env::temp_dir().join(format!("minidex_test_lib_vol_del_{}", rand_id()));
        std::fs::create_dir_all(&temp_dir)?;

        let sep = std::path::MAIN_SEPARATOR_STR;

        let index = Index::open(&temp_dir)?;
        index.insert(FilesystemEntry {
            path: PathBuf::from(format!("{}foo{}bar{}a.txt", sep, sep, sep)),
            volume: "vol1".to_string(),
            kind: Kind::File,
            last_modified: 100,
            last_accessed: 100,
            category: 0,
            volume_type: VolumeType::Local,
        })?;
        index.insert(FilesystemEntry {
            path: PathBuf::from(format!("{}foo{}bar{}b.txt", sep, sep, sep)),
            volume: "vol2".to_string(),
            kind: Kind::File,
            last_modified: 100,
            last_accessed: 100,
            category: 0,
            volume_type: VolumeType::Local,
        })?;

        // Delete /foo on vol1 only
        index.delete_by_volume_name(Some("vol1"), &format!("{}foo", sep))?;

        let results = index.search("txt", 10, 0, SearchOptions::default())?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].volume, "vol2");

        std::fs::remove_dir_all(temp_dir)?;
        Ok(())
    }

    #[test]
    fn test_index_compaction() -> Result<(), IndexError> {
        let temp_dir = std::env::temp_dir().join(format!("minidex_test_lib_comp_{}", rand_id()));
        std::fs::create_dir_all(&temp_dir)?;

        let config = CompactorConfig {
            flush_threshold: 1,
            ..Default::default()
        };

        let sep = std::path::MAIN_SEPARATOR_STR;

        let index = Index::open_with_config(&temp_dir, config)?;

        // Create 4 items to trigger 2 flushes (with flush_threshold=1)
        for i in 0..4 {
            index.insert(FilesystemEntry {
                path: PathBuf::from(format!("{}foo{}{}.txt", sep, sep, i)),
                volume: "vol1".to_string(),
                kind: Kind::File,
                last_modified: 100,
                last_accessed: 100,
                category: 0,
                volume_type: VolumeType::Local,
            })?;
            // Force wait for each flush
            std::thread::sleep(std::time::Duration::from_millis(200));
        }

        // Wait for final flush to finish
        if let Ok(mut flusher) = index.flusher.write()
            && let Some(h) = flusher.take()
        {
            let _ = h.join();
        }

        {
            let base = index.base.read().unwrap();
            assert!(
                base.segments().count() >= 2,
                "Should have at least 2 segments, got {}",
                base.segments().count()
            );
        }

        index.force_compact_all()?;

        {
            let base = index.base.read().unwrap();
            assert_eq!(base.segments().count(), 1);
        }

        let results = index.search("foo", 10, 0, SearchOptions::default())?;
        assert_eq!(results.len(), 4);

        std::fs::remove_dir_all(temp_dir)?;
        Ok(())
    }

    #[test]
    fn test_index_recent_files() -> Result<(), IndexError> {
        let temp_dir = std::env::temp_dir().join(format!("minidex_test_lib_recent_{}", rand_id()));
        std::fs::create_dir_all(&temp_dir)?;

        let sep = std::path::MAIN_SEPARATOR_STR;

        let index = Index::open(&temp_dir)?;
        index.insert(FilesystemEntry {
            path: PathBuf::from(format!("{}foo{}old.txt", sep, sep)),
            volume: "vol1".to_string(),
            kind: Kind::File,
            last_modified: 100,
            last_accessed: 100, // Very old
            category: 0,
            volume_type: VolumeType::Local,
        })?;
        let new_path = format!("{}foo{}new.txt", sep, sep);
        index.insert(FilesystemEntry {
            path: PathBuf::from(&new_path),
            volume: "vol1".to_string(),
            kind: Kind::File,
            last_modified: 1000,
            last_accessed: 1000, // Newer
            category: 0,
            volume_type: VolumeType::Local,
        })?;

        let results = index.recent_files(500, 10, 0, SearchOptions::default())?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].path, PathBuf::from(&new_path));

        std::fs::remove_dir_all(temp_dir)?;
        Ok(())
    }

    #[test]
    fn test_index_search_filters() -> Result<(), IndexError> {
        let temp_dir = std::env::temp_dir().join(format!("minidex_test_lib_filter_{}", rand_id()));
        std::fs::create_dir_all(&temp_dir)?;

        let sep = std::path::MAIN_SEPARATOR_STR;

        let index = Index::open(&temp_dir)?;
        index.insert(FilesystemEntry {
            path: PathBuf::from(format!("{}vol1{}a.txt", sep, sep)),
            volume: "vol1".to_string(),
            kind: Kind::File,
            last_modified: 100,
            last_accessed: 100,
            category: category::TEXT,
            volume_type: VolumeType::Local,
        })?;
        index.insert(FilesystemEntry {
            path: PathBuf::from(format!("{}vol2{}b.txt", sep, sep)),
            volume: "vol2".to_string(),
            kind: Kind::File,
            last_modified: 100,
            last_accessed: 100,
            category: category::IMAGE,
            volume_type: VolumeType::Local,
        })?;

        // Filter by volume
        let opts_vol1 = SearchOptions {
            volume_name: Some("vol1"),
            ..Default::default()
        };
        let res_vol1 = index.search("txt", 10, 0, opts_vol1)?;
        assert_eq!(res_vol1.len(), 1);
        assert_eq!(res_vol1[0].volume, "vol1");

        // Filter by category
        let opts_img = SearchOptions {
            category: Some(category::IMAGE),
            ..Default::default()
        };
        let res_img = index.search("txt", 10, 0, opts_img)?;
        assert_eq!(res_img.len(), 1);
        assert_eq!(res_img[0].category, category::IMAGE);

        // Filter by kind
        let opts_dir = SearchOptions {
            kind: Some(Kind::Directory),
            ..Default::default()
        };
        let res_dir = index.search("txt", 10, 0, opts_dir)?;
        assert_eq!(res_dir.len(), 0);

        std::fs::remove_dir_all(temp_dir)?;
        Ok(())
    }

    fn rand_id() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}
