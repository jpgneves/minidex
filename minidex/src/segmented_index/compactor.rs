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
/// Note: atomic replacement of old segment files is done by the caller
pub(crate) fn merge_segments(
    segments: &[Arc<Segment>],
    out: PathBuf,
) -> Result<u64, SegmentedIndexError> {
    let mut survivors: BTreeMap<String, IndexEntry> = BTreeMap::new();

    for seg in segments {
        let data = seg.data.as_ref().expect("expected a loaded data map");
        let mut cursor = 0;

        while cursor < data.len() {
            if cursor + size_of::<u32>() > data.len() {
                break;
            }
            let path_len =
                u32::from_le_bytes(data[cursor..cursor + size_of::<u32>()].try_into().unwrap())
                    as usize;
            cursor += size_of::<u32>();

            if cursor + path_len > data.len() {
                break;
            }

            let path_str = std::str::from_utf8(&data[cursor..cursor + path_len])
                .unwrap_or("")
                .to_string();
            cursor += path_len;

            if cursor + IndexEntry::SIZE > data.len() {
                break;
            }
            let entry = IndexEntry::from_bytes(&data[cursor..cursor + IndexEntry::SIZE]);
            cursor += IndexEntry::SIZE;

            survivors
                .entry(path_str)
                .and_modify(|existing| {
                    if entry.opstamp.sequence() > existing.opstamp.sequence() {
                        *existing = entry;
                    }
                })
                .or_insert(entry);
        }
    }

    SegmentedIndex::build_segment_files(&out, survivors, true)
}
