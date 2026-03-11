use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use crate::{entry::IndexEntry, segmented_index::SegmentedIndexError};

use super::{Segment, SegmentedIndex};

#[allow(dead_code)]
/// Configuration for compaction
pub struct CompactorConfig {
    /// Minimum number of segments required for compaction
    pub min_merge_count: usize,
    max_size_ratio: f32,
    memory_threshold: usize,
    deletion_threshold: usize,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        CompactorConfigBuilder::default().build()
    }
}

/// Compaction configuration builder
// TODO: Clean up dead parameters
#[allow(dead_code)]
pub struct CompactorConfigBuilder {
    min_merge_count: usize,
    max_size_ratio: f32,
    memory_threshold: usize,
    deletion_threshold: usize,
}

impl Default for CompactorConfigBuilder {
    fn default() -> Self {
        Self {
            min_merge_count: 4,
            max_size_ratio: 1.5,
            memory_threshold: 100 * 1024 * 1024, // Default to 100MB usage
            deletion_threshold: 1000,            // Trigger compaction on 1000 deletes
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

    pub fn max_size_ratio(self, max_size_ratio: f32) -> Self {
        Self {
            max_size_ratio,
            ..self
        }
    }

    pub fn memory_threshold(self, memory_threshold: usize) -> Self {
        Self {
            memory_threshold,
            ..self
        }
    }

    pub fn deletion_threshold(self, deletion_threshold: usize) -> Self {
        Self {
            deletion_threshold,
            ..self
        }
    }

    pub fn build(self) -> CompactorConfig {
        CompactorConfig {
            min_merge_count: self.min_merge_count,
            max_size_ratio: self.max_size_ratio,
            memory_threshold: self.memory_threshold,
            deletion_threshold: self.deletion_threshold,
        }
    }
}

/// Merge live segments into smaller ones.
/// Drops data that is outdated - only the latest opstamp wins.
/// Implemented via a K-Way Merge with zero allocations
/// Note: atomic replacement of old segment files is done by the caller
pub(crate) fn merge_segments(
    segments: &[Arc<Segment>],
    out: PathBuf,
) -> Result<u64, SegmentedIndexError> {
    let mut iterators: Vec<_> = segments
        .iter()
        .map(|seg| seg.documents().into_iter())
        .collect();

    let mut currents: Vec<Option<(String, String, IndexEntry)>> =
        iterators.iter_mut().map(|iter| iter.next()).collect();

    let merged_iterator = std::iter::from_fn(move || {
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

                    if item.2.opstamp.sequence() > best_item.2.opstamp.sequence() {
                        best_item = item;
                    }

                    // Refill the head on the consumed iterator
                    currents[i] = iterators[i].next();
                } else {
                    break;
                }
            }
        }
        Some(best_item)
    });

    SegmentedIndex::build_segment_files(&out, merged_iterator, true)
}
