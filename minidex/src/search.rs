use std::path::PathBuf;

use crate::Kind;

mod scoring;
pub use scoring::*;

/// Search options, allowing filtering and custom scoring
#[derive(Debug, Default)]
pub struct SearchOptions<'a> {
    pub scoring: Option<&'a ScoringConfig>,
    pub volume_filter: Option<&'a str>,
    pub category: Option<u16>,
    pub kind: Option<Kind>,
}

/// A Minidex search result, containing the found metadata for
/// the given file
#[derive(Debug, PartialEq)]
pub struct SearchResult {
    pub path: PathBuf,
    pub volume: String,
    pub kind: Kind,
    pub last_modified: u64,
    pub last_accessed: u64,
    pub category: u16,
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
