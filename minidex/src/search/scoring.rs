use crate::Kind;

/// Configurable weights for search result scoring.
#[derive(Debug)]
pub struct ScoringConfig {
    /// Token coverage ratio
    pub token_coverage: f64,
    /// Exact token match (not just prefix match)
    pub exact_match: f64,
    /// Query token matching in file name
    pub filename_match: f64,
    /// Penatly multiplier for path depth (applied as -weight * ln(depth))
    /// Surfaces shallower results first
    pub depth_penalty: f64,
    /// Maximum recency boost (decays logarithmically)
    pub recency_boost: f64,
    /// Directory boost (vs files).
    pub kind_dir_boost: f64,
    /// Boost for consecutive token matches
    pub consecutive_match: f64,
}

impl Default for ScoringConfig {
    fn default() -> Self {
        Self {
            token_coverage: 30.0,
            exact_match: 10.0,
            filename_match: 15.0,
            depth_penalty: 2.0,
            recency_boost: 10.0,
            kind_dir_boost: 2.0,
            consecutive_match: 20.0,
        }
    }
}

pub(crate) fn compute_score(
    config: &ScoringConfig,
    path: &str,
    query_tokens: &[String],
    last_modified: u64,
    kind: Kind,
    now_micros: f64,
) -> f64 {
    let path_tokens = crate::tokenizer::tokenize(path);
    let normalized = path.to_lowercase();

    let mut score = 0.0;

    // Calculate token coverage
    if !path_tokens.is_empty() {
        let matched = path_tokens
            .iter()
            .filter(|path_token| {
                query_tokens
                    .iter()
                    .any(|query_token| path_token.starts_with(query_token.as_str()))
            })
            .count();

        score += config.token_coverage * (matched as f64 / path_tokens.len() as f64);
    }

    // Exact token matches
    let exact = query_tokens
        .iter()
        .filter(|query_token| {
            path_tokens
                .iter()
                .any(|path_token| path_token == *query_token)
        })
        .count();

    score += config.exact_match * exact as f64;

    // Filename match: query tokens found in the last path component
    let filename_start = path
        .rfind(std::path::MAIN_SEPARATOR)
        .map(|i| i + 1)
        .unwrap_or(0);

    let filename_lower = &normalized[filename_start..];
    let filename_hits = query_tokens
        .iter()
        .filter(|query_token| filename_lower.contains(query_token.as_str()))
        .count();

    score += config.filename_match * filename_hits as f64;

    // Path depth penalty
    let depth = path
        .chars()
        .filter(|c| *c == std::path::MAIN_SEPARATOR)
        .count();
    if depth > 1 {
        score -= config.depth_penalty * (depth as f64).ln();
    }

    // Recency boost
    let age_days = (now_micros - last_modified as f64) / (1_000_000.0 * 86_400.0);
    if age_days > 0.0 {
        score += config.recency_boost / (1.0 + age_days.ln());
    } else {
        score += config.recency_boost
    }

    // Kind preference
    score += match kind {
        Kind::Directory => config.kind_dir_boost,
        Kind::File => config.kind_dir_boost * 0.5,
        Kind::Symlink => config.kind_dir_boost * 0.1,
    };

    // Consecutive token match
    let query_joined: String = query_tokens.iter().map(|s| s.as_str()).collect();
    if !query_joined.is_empty() && normalized.contains(&query_joined) {
        score += config.consecutive_match;
    }

    score
}
