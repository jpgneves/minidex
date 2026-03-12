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
use collector::*;
pub use common::{Kind, category};
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
pub use search::{ScoringConfig, SearchOptions, SearchResult};

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
    prefix_tombstones: Arc<RwLock<Vec<(String, u64)>>>,
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
            for (prefix, seq) in replay_data.tombstones {
                max_seq = max_seq.max(seq);
                prefix_tombstones.push((prefix, seq));
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
            prefix_tombstones: Arc::new(RwLock::new(prefix_tombstones)),
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
            category: item.category,
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

    pub fn delete_prefix(&self, prefix: &str) -> Result<(), IndexError> {
        let seq = self.next_op_seq.fetch_add(1, Ordering::SeqCst);
        let prefix_lower = prefix.to_lowercase();
        {
            let mut tombstones = self
                .prefix_tombstones
                .write()
                .map_err(|_| IndexError::WriteLock)?;
            tombstones.push((prefix_lower.clone(), seq));
        }

        {
            let mut wal = self.wal.write().map_err(|_| IndexError::WriteLock)?;

            wal.write_prefix_tombstone(&prefix_lower, seq)?;
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

        let required_matches = limit + offset;
        let scoring_cap = std::cmp::max(500, required_matches * 3).min(1000);

        let active_tombstones = self
            .prefix_tombstones
            .read()
            .map_err(|_| IndexError::ReadLock)?
            .clone();

        let mut collector = LsmCollector::new(&active_tombstones);

        // In-memory searches
        for (path, (volume, entry)) in mem.iter() {
            let path_bytes = path.as_bytes();

            if is_tombstoned(path_bytes, entry.opstamp.sequence(), &active_tombstones) {
                continue;
            }

            if let Some(filter) = options.volume_filter {
                if volume != filter {
                    continue;
                }
            }

            if let Some(category) = options.category {
                if entry.category & category == 0 {
                    continue;
                }
            }

            if let Some(kind) = options.kind {
                if entry.kind != kind {
                    continue;
                }
            }

            let matches_all = if path.is_ascii() {
                tokens.iter().all(|t| {
                    let token_bytes = t.as_bytes();
                    if path_bytes.len() < token_bytes.len() {
                        return false;
                    }
                    path_bytes
                        .windows(token_bytes.len())
                        .any(|window| window.eq_ignore_ascii_case(token_bytes))
                })
            } else {
                let folded_path = crate::tokenizer::fold_path(path);
                tokens.iter().all(|t| folded_path.contains(t))
            };

            if matches_all {
                collector.insert(path.to_string(), volume.clone(), *entry);
            }
        }

        // Disk searches
        let mut token_docs = Vec::new();
        let mut current_matches = Vec::new();

        for segment in segments.segments() {
            current_matches.clear();
            let mut first_token = true;
            let mut valid_matches = true;

            if let Some(vol) = options.volume_filter {
                let vol_token = crate::tokenizer::synthesize_volume_token(&vol.to_lowercase());
                let map = segment.as_ref().as_ref();
                if let Some(post_offset) = map.get(&vol_token) {
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
                        // Perform binary search if one of the arrays is much larger
                        current_matches.retain(|doc_id| token_docs.binary_search(doc_id).is_ok());
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

                    let (_, _, _, _, is_dir, doc_category) =
                        SegmentedIndex::unpack_u128(packed_val);

                    // Kind filter
                    if let Some(kind) = options.kind {
                        let is_target_dir = kind == Kind::Directory;
                        if is_dir != is_target_dir {
                            continue;
                        }
                    }

                    // Filter categories
                    if let Some(category) = options.category {
                        if doc_category != category as u16 {
                            continue;
                        }
                    }

                    enriched_docs.push(packed_val);
                }

                if enriched_docs.len() > scoring_cap {
                    // O(N) quickselect
                    enriched_docs.select_nth_unstable_by(scoring_cap, |&a, &b| {
                        let (_, a_modified_at, _, a_depth, a_dir, _) =
                            SegmentedIndex::unpack_u128(a);
                        let (_, b_modified_at, _, b_depth, b_dir, _) =
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
                    let (dat_offset, _, _, _, _, _) = SegmentedIndex::unpack_u128(packed);
                    dat_offset
                });

                for packed_val in enriched_docs {
                    let (dat_offset, _, _, _, _, _) = SegmentedIndex::unpack_u128(packed_val);

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

        let mut scored: Vec<_> = results
            .into_iter()
            .map(|(path, volume, entry)| {
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
        let active_tombstones = self.prefix_tombstones.read().unwrap().clone();

        let mut collector = LsmCollector::new(&active_tombstones);

        // 1. MemTable Scan (Fast, eager load is fine here)
        for (path, (volume, entry)) in mem.iter() {
            // FIX: >= instead of <= to get files from the last 5 days
            if entry.last_accessed >= since {
                if let Some(filter) = options.volume_filter {
                    if volume != filter {
                        continue;
                    }
                }
                if let Some(category) = options.category {
                    if entry.category & category == 0 {
                        continue;
                    }
                }
                if let Some(kind) = options.kind {
                    if entry.kind != kind {
                        continue;
                    }
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
                let (_, _, accessed, _, is_dir, doc_category) = SegmentedIndex::unpack_u128(packed);

                if accessed >= since {
                    if let Some(target_kind) = options.kind {
                        let is_target_dir = target_kind == Kind::Directory;
                        if is_dir != is_target_dir {
                            continue;
                        }
                    }

                    if let Some(category) = options.category {
                        if doc_category & category == 0 {
                            continue;
                        }
                    }

                    // DO NOT read the document yet! Just save the integer.
                    disk_candidates.push((segment, packed));
                }
            }
        }

        if disk_candidates.len() > disk_cap {
            disk_candidates.select_nth_unstable_by(disk_cap, |a, b| {
                let (_, _, a_acc, _, _, _) = SegmentedIndex::unpack_u128(a.1);
                let (_, _, b_acc, _, _, _) = SegmentedIndex::unpack_u128(b.1);
                b_acc.cmp(&a_acc) // Sort descending by access time
            });
            disk_candidates.truncate(disk_cap);
        }

        for (segment, packed) in disk_candidates {
            let (dat_offset, _, _, _, _, _) = SegmentedIndex::unpack_u128(packed);

            if let Some((path, volume, entry)) = segment.read_document(dat_offset) {
                if let Some(filter) = options.volume_filter {
                    if volume != filter {
                        continue;
                    }
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
        let tombstones = self
            .prefix_tombstones
            .read()
            .map_err(|_| IndexError::ReadLock)?;
        for (prefix, seq) in tombstones.iter() {
            wal.write_prefix_tombstone(prefix, *seq)?;
        }

        drop(tombstones);
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
        prefix_tombstones: Arc<RwLock<Vec<(String, u64)>>>,
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
                        tombstones.retain(|(_, seq)| *seq >= compactor_seq);
                        log::debug!("Compaction finished");
                    }
                    Err(e) => log::error!("Compaction failed: {}", e),
                }
            })
            .ok()
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
