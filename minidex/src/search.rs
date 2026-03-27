use std::path::PathBuf;

use crate::{
    Kind, collector::LsmCollector, common::VolumeType, memtable::MemTable,
    segmented_index::SegmentedIndex,
};

mod scoring;
pub use scoring::*;

/// Search options, allowing filtering and custom scoring
#[derive(Debug, Default)]
pub struct SearchOptions<'a> {
    pub scoring: Option<&'a ScoringConfig>,
    pub volume_name: Option<&'a str>,
    pub category: Option<u8>,
    pub kind: Option<Kind>,
    pub volume_type: Option<&'a [VolumeType]>,
}

/// A Minidex search result, containing the found metadata for
/// the given file
#[derive(Debug, PartialEq)]
pub struct SearchResult {
    pub path: PathBuf,
    pub volume: String,
    pub volume_type: VolumeType,
    pub kind: Kind,
    pub last_modified: u64,
    pub last_accessed: u64,
    pub category: u8,
    pub score: f64,
}

impl Eq for SearchResult {}

impl Ord for SearchResult {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| other.last_modified.cmp(&self.last_modified)) // descending order
            .then_with(|| self.kind.cmp(&other.kind))
            .then_with(|| self.path.cmp(&other.path))
    }
}

impl PartialOrd for SearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[inline(always)]
pub(crate) fn evaluate_candidate(
    packed: u128,
    options: &SearchOptions,
    volume_type_mask: u8,
) -> Option<u64> {
    // Inline bitwise extraction to avoid incurring type conversion penalties
    let last_modified = ((packed >> 40) & 0x3_FFFF_FFFF) as u64;
    let last_accessed = ((packed >> 74) & 0x3_FFFF_FFFF) as u64;
    let depth = ((packed >> 108) & 0xFF) as u64;
    let is_dir = ((packed >> 116) & 1) as u64; // Yields exactly 1 or 0
    let category = ((packed >> 117) & 0xFF) as u8;
    let volume_type = ((packed >> 125) & 0b11) as u8;

    // Apply fast bitwise filters first
    if let Some(target_kind) = options.kind {
        let is_target_dir = if target_kind == crate::Kind::Directory {
            1
        } else {
            0
        };
        if is_dir != is_target_dir {
            return None;
        }
    }
    if let Some(category_filter) = options.category
        && category & category_filter == 0
    {
        return None;
    }

    if (volume_type_mask & (1 << volume_type)) == 0 {
        return None;
    }

    // Intentionally avoiding a call to `max` here, should optimize
    // to a single instruction
    let recent = if last_modified < last_accessed {
        last_accessed
    } else {
        last_modified
    };

    let sort_key = (is_dir << 63) | ((!depth & 0xFF) << 55) | (recent << 21);
    Some(sort_key)
}

#[inline(always)]
pub(crate) fn retain_top_k<T>(candidates: &mut Vec<(u64, T)>, cap: usize) {
    if candidates.len() > cap {
        candidates.select_nth_unstable_by(cap, |a, b| b.0.cmp(&a.0));
        candidates.truncate(cap);
    }
}

/// Scans a MemTable using the inverted index for text token matching
#[inline]
pub(crate) fn scan_mem_for_search<'a>(
    mem_table: &'a MemTable,
    collector: &mut LsmCollector<'a>,
    tokens: &[String],
    scoring_cap: usize,
    options: &SearchOptions<'_>,
    volume_type_mask: u8,
) {
    let mut mem_candidates: Option<Vec<u32>> = None;
    let mut mem_intersect_buf = Vec::new();

    if !tokens.is_empty() {
        for token in tokens {
            let mut end_bound = String::with_capacity(token.len() + 4);
            end_bound.push_str(token);
            end_bound.push('\u{FFFF}');

            let matching_arrays: Vec<&Vec<u32>> = mem_table
                .inverted_index
                .range::<str, _>((
                    std::ops::Bound::Included(token.as_str()),
                    std::ops::Bound::Included(end_bound.as_str()),
                ))
                .map(|(_, ids)| ids)
                .collect();

            if matching_arrays.is_empty() {
                mem_candidates = Some(Vec::new());
                break;
            }

            let current_token_ids = if matching_arrays.len() == 1 {
                std::borrow::Cow::Borrowed(matching_arrays[0])
            } else {
                let mut merged = Vec::new();
                for ids in matching_arrays {
                    merged.extend_from_slice(ids);
                }
                merged.sort_unstable();
                merged.dedup();
                std::borrow::Cow::Owned(merged)
            };

            match mem_candidates.as_mut() {
                Some(existing) => {
                    mem_intersect_buf.clear();
                    crate::simd::intersect_arrays(
                        existing,
                        &current_token_ids,
                        &mut mem_intersect_buf,
                    );
                    std::mem::swap(existing, &mut mem_intersect_buf);
                }
                None => mem_candidates = Some(current_token_ids.into_owned()),
            }

            if let Some(c) = &mem_candidates {
                if c.is_empty() {
                    break;
                }
            }
        }
    } else {
        mem_candidates = Some(mem_table.id_to_data.keys().copied().collect());
    }

    if let Some(candidates) = mem_candidates {
        let mut mem_sortable = Vec::with_capacity(candidates.len());

        for id in candidates {
            let metadata = mem_table.metadata[id as usize];
            if let Some(sort_key) = evaluate_candidate(metadata, options, volume_type_mask) {
                mem_sortable.push((sort_key, id));
            }
        }

        crate::search::retain_top_k(&mut mem_sortable, scoring_cap);

        for (_, id) in mem_sortable {
            if let Some((path, volume, entry)) = mem_table.id_to_data.get(&id) {
                if let Some(filter) = options.volume_name {
                    if volume != filter {
                        continue;
                    }
                }
                collector.insert(path.as_str(), volume.as_str(), *entry);
            }
        }
    }
}

/// Scans a MemTable chronologically using the packed u128 metadata array
#[inline]
pub(crate) fn scan_mem_for_recent<'a>(
    mem_table: &'a MemTable,
    collector: &mut LsmCollector<'a>,
    since_secs: u64,
    disk_cap: usize,
    options: &SearchOptions<'_>,
    volume_type_mask: u8,
) {
    let mut mem_candidates = Vec::new();

    for (id, &metadata) in mem_table.metadata.iter().enumerate() {
        let (_, last_modified, last_accessed, _, is_dir, doc_category, doc_volume_type) =
            SegmentedIndex::unpack_u128(metadata);

        let recent = last_modified.max(last_accessed);

        if recent >= since_secs {
            if let Some(kind) = options.kind {
                if is_dir != (kind == Kind::Directory) {
                    continue;
                }
            }
            if let Some(category) = options.category {
                if doc_category & category == 0 {
                    continue;
                }
            }
            if (volume_type_mask & (1 << doc_volume_type)) == 0 {
                continue;
            }
            mem_candidates.push((metadata, id as u32));
        }
    }

    if mem_candidates.len() > disk_cap {
        mem_candidates.select_nth_unstable_by(disk_cap, |a, b| {
            let last_modified_a = ((a.0 >> 40) & 0x3_FFFF_FFFF) as u64;
            let last_accessed_a = ((a.0 >> 74) & 0x3_FFFF_FFFF) as u64;
            let recent_a = last_modified_a.max(last_accessed_a);

            let last_modified_b = ((b.0 >> 40) & 0x3_FFFF_FFFF) as u64;
            let last_accessed_b = ((b.0 >> 74) & 0x3_FFFF_FFFF) as u64;
            let recent_b = last_modified_b.max(last_accessed_b);

            recent_b.cmp(&recent_a).then_with(|| a.1.cmp(&b.1))
        });
        mem_candidates.truncate(disk_cap);
    }

    for (_, id) in mem_candidates {
        if let Some((path, volume, entry)) = mem_table.id_to_data.get(&id) {
            if let Some(filter) = options.volume_name {
                if volume != filter {
                    continue;
                }
            }
            collector.insert(path.as_str(), volume.as_str(), *entry);
        }
    }
}
