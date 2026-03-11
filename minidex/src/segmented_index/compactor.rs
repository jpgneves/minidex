use std::{path::PathBuf, sync::Arc};

use crate::{entry::IndexEntry, segmented_index::SegmentedIndexError};

use super::{Segment, SegmentedIndex};

/// Configuration for compaction
pub struct CompactorConfig {
    /// Minimum number of segments required for compaction
    pub min_merge_count: usize,
    /// Minimum amount of data in memory required to flush
    pub flush_threshold: usize,
    /// Minimum amount of tombstones written to trigger compaction
    pub tombstone_threshold: usize,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        CompactorConfigBuilder::default().build()
    }
}

/// Compaction configuration builder
pub struct CompactorConfigBuilder {
    min_merge_count: usize,
    flush_threshold: usize,
    tombstone_threshold: usize,
}

impl Default for CompactorConfigBuilder {
    fn default() -> Self {
        Self {
            min_merge_count: 4,
            flush_threshold: 10_000, // Default to 10k entries
            tombstone_threshold: 50,
        }
    }
}

impl CompactorConfigBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the minimum number of live segments required to trigger
    /// compaction
    pub fn min_merge_count(self, min_merge_count: usize) -> Self {
        Self {
            min_merge_count,
            ..self
        }
    }

    /// Set the minimum number of items in memory required to trigger
    /// flushing and compaction
    pub fn flush_threshold(self, flush_threshold: usize) -> Self {
        Self {
            flush_threshold,
            ..self
        }
    }

    /// Set the minimum number of prefix tombstones required to trigger compaction
    pub fn tombstone_threshold(self, tombstone_threshold: usize) -> Self {
        Self {
            tombstone_threshold,
            ..self
        }
    }

    pub fn build(self) -> CompactorConfig {
        CompactorConfig {
            min_merge_count: self.min_merge_count,
            flush_threshold: self.flush_threshold,
            tombstone_threshold: self.tombstone_threshold,
        }
    }
}

/// Merge live segments into smaller ones.
/// Drops data that is outdated - only the latest opstamp wins.
/// Implemented via a K-Way Merge with zero allocations
/// Note: atomic replacement of old segment files is done by the caller
pub(crate) fn merge_segments(
    segments: &[Arc<Segment>],
    prefix_tombstones: Vec<(String, u64)>,
    out: PathBuf,
) -> Result<u64, SegmentedIndexError> {
    let mut iterators: Vec<_> = segments
        .iter()
        .map(|seg| seg.documents().into_iter())
        .collect();

    let mut currents: Vec<Option<(String, String, IndexEntry)>> =
        iterators.iter_mut().map(|iter| iter.next()).collect();

    let merged_iterator = std::iter::from_fn(move || {
        loop {
            // Find the index of the segment with the alphabetically smallest path
            let mut min_idx = None;

            for i in 0..currents.len() {
                if let Some((path_i, _, _)) = &currents[i] {
                    min_idx = match min_idx {
                        None => Some(i), // We're the first
                        Some(idx) => {
                            let (path_min, _, _) = currents[idx].as_ref().unwrap();
                            if path_i < path_min {
                                Some(i)
                            } else {
                                Some(idx)
                            }
                        }
                    };
                }
            }

            // If we've exhausted all iterators, merge completed.
            let target_idx = min_idx?;

            let mut best_item = currents[target_idx].take().unwrap();

            // Refill the head with the next one.
            currents[target_idx] = iterators[target_idx].next();

            // Check all other heads for the exact same path.
            // If they are the same, consume and resolve opstamp ties.
            for i in 0..currents.len() {
                while let Some((path, _, _)) = &currents[i] {
                    if *path == best_item.0 {
                        let item = currents[i].take().unwrap();

                        // Check for tombstones
                        let path_bytes = item.0.as_bytes();
                        let is_dead = prefix_tombstones.iter().any(|(prefix, stamp)| {
                            let prefix_bytes = prefix.as_bytes();
                            path_bytes.len() >= prefix_bytes.len()
                                && path_bytes[..prefix_bytes.len()]
                                    .eq_ignore_ascii_case(prefix_bytes)
                                && item.2.opstamp.sequence() < *stamp
                        });

                        if !is_dead && item.2.opstamp.sequence() > best_item.2.opstamp.sequence() {
                            best_item = item;
                        }

                        // Refill the head on the consumed iterator
                        currents[i] = iterators[i].next();
                    } else {
                        break;
                    }
                }
            }

            let best_lower = best_item.0.to_lowercase();
            let best_is_dead = prefix_tombstones.iter().any(|(prefix, stamp)| {
                best_lower.starts_with(prefix) && best_item.2.opstamp.sequence() < *stamp
            });

            if best_is_dead {
                continue;
            }

            break Some(best_item);
        }
    });

    SegmentedIndex::build_segment_files(&out, merged_iterator, true)
}
