use std::path::PathBuf;

use crate::{Kind, common::VolumeType};

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
    let volume_type = ((packed >> 124) & 0b11) as u8;

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
