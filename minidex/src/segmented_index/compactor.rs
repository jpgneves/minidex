use crate::sync::Arc;
use std::path::PathBuf;

use crate::{entry::IndexEntry, is_tombstoned, segmented_index::SegmentedIndexError};

use super::{Segment, SegmentedIndex};

/// Configuration for compaction
#[derive(Debug, Clone, Copy)]
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
            min_merge_count: 8,
            flush_threshold: 100_000,
            tombstone_threshold: 2500,
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
    prefix_tombstones: Arc<Vec<(Option<String>, String, u64)>>,
    out: PathBuf,
) -> Result<u64, SegmentedIndexError> {
    let mut iterators: Vec<_> = segments.iter().map(|seg| seg.documents()).collect();

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
                        let is_dead = is_tombstoned(
                            &item.1,
                            path_bytes,
                            item.2.opstamp.sequence(),
                            &prefix_tombstones,
                        );

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

            let best_bytes = best_item.0.as_bytes();
            let best_is_dead = is_tombstoned(
                &best_item.1,
                best_bytes,
                best_item.2.opstamp.sequence(),
                &prefix_tombstones,
            );

            if best_is_dead {
                continue;
            }

            break Some(best_item);
        }
    });

    // Strategy: reuse the dictionary from the first segment being merged.
    // In tiered compaction, segments being merged are usually from the same tier
    // or include a newly flushed segment. Reusing an existing dictionary
    // avoids expensive training on every compaction.
    let existing_dict = segments.first().and_then(|s| s.dict.as_deref());

    SegmentedIndex::build_segment_files(&out, merged_iterator, true, existing_dict)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Kind;
    use crate::VolumeType;
    use crate::opstamp::Opstamp;

    #[test]
    fn test_merge_segments_basic() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = std::env::temp_dir().join(format!("minidex_test_comp_{}", rand_id()));
        std::fs::create_dir_all(&temp_dir)?;

        let seg1_path = temp_dir.join("1");
        let entries1 = vec![
            (
                "/foo/a".to_string(),
                "vol1".to_string(),
                IndexEntry {
                    opstamp: Opstamp::insertion(1),
                    kind: Kind::File,
                    last_modified: 100,
                    last_accessed: 100,
                    category: 0,
                    volume_type: VolumeType::Local,
                },
            ),
            (
                "/foo/b".to_string(),
                "vol1".to_string(),
                IndexEntry {
                    opstamp: Opstamp::insertion(1),
                    kind: Kind::File,
                    last_modified: 100,
                    last_accessed: 100,
                    category: 0,
                    volume_type: VolumeType::Local,
                },
            ),
        ];
        SegmentedIndex::build_segment_files(&seg1_path, entries1, false, None)?;

        let seg2_path = temp_dir.join("2");
        let entries2 = vec![
            (
                "/foo/a".to_string(),
                "vol1".to_string(),
                IndexEntry {
                    opstamp: Opstamp::insertion(2), // Newer version
                    kind: Kind::File,
                    last_modified: 200,
                    last_accessed: 200,
                    category: 0,
                    volume_type: VolumeType::Local,
                },
            ),
            (
                "/foo/c".to_string(),
                "vol1".to_string(),
                IndexEntry {
                    opstamp: Opstamp::insertion(1),
                    kind: Kind::File,
                    last_modified: 100,
                    last_accessed: 100,
                    category: 0,
                    volume_type: VolumeType::Local,
                },
            ),
        ];
        SegmentedIndex::build_segment_files(&seg2_path, entries2, false, None)?;

        let s1 = Arc::new(Segment::load(seg1_path)?);
        let s2 = Arc::new(Segment::load(seg2_path)?);

        let out_path = temp_dir.join("merged");
        merge_segments(&[s1, s2], Arc::new(vec![]), out_path.clone())?;

        let merged_seg = Segment::load(out_path)?;
        let docs: Vec<_> = merged_seg.documents().collect();

        // Output should have 3 unique paths: a, b, c
        assert_eq!(docs.len(), 3);

        let mut paths: Vec<_> = docs.iter().map(|(p, _, _)| p.as_str()).collect();
        paths.sort();
        assert_eq!(paths, vec!["/foo/a", "/foo/b", "/foo/c"]);

        // Path "/foo/a" should be version 2
        let foo_a = docs.iter().find(|(p, _, _)| p == "/foo/a").unwrap();
        assert_eq!(foo_a.2.opstamp.sequence(), 2);

        std::fs::remove_dir_all(temp_dir)?;
        Ok(())
    }

    #[test]
    fn test_merge_segments_with_tombstones() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = std::env::temp_dir().join(format!("minidex_test_comp_tomb_{}", rand_id()));
        std::fs::create_dir_all(&temp_dir)?;

        let sep = std::path::MAIN_SEPARATOR;

        let seg_path = temp_dir.join("1");
        let entries = vec![
            (
                format!("/foo{}a", sep),
                "vol1".to_string(),
                IndexEntry {
                    opstamp: Opstamp::insertion(10),
                    kind: Kind::File,
                    last_modified: 100,
                    last_accessed: 100,
                    category: 0,
                    volume_type: VolumeType::Local,
                },
            ),
            (
                format!("/bar{}b", sep),
                "vol1".to_string(),
                IndexEntry {
                    opstamp: Opstamp::insertion(10),
                    kind: Kind::File,
                    last_modified: 100,
                    last_accessed: 100,
                    category: 0,
                    volume_type: VolumeType::Local,
                },
            ),
        ];
        SegmentedIndex::build_segment_files(&seg_path, entries, false, None)?;

        let s1 = Arc::new(Segment::load(seg_path)?);

        let out_path = temp_dir.join("merged");
        // Tombstone for /foo on vol1
        let tombstones = vec![(Some("vol1".to_string()), "/foo".to_string(), 50)];
        merge_segments(&[s1], Arc::new(tombstones), out_path.clone())?;

        let merged_seg = Segment::load(out_path)?;
        let docs: Vec<_> = merged_seg.documents().collect();

        // /foo/a should be gone, /bar/b should remain
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].0, format!("/bar{}b", sep));

        std::fs::remove_dir_all(temp_dir)?;
        Ok(())
    }

    fn rand_id() -> u64 {
        crate::sync::time::SystemTime::now()
            .duration_since(crate::sync::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}
