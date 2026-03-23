use crate::Kind;

/// Scoring configuration
#[derive(Debug)]
pub struct ScoringConfig {
    /// Weights available for tweaking scoring
    pub weights: Option<ScoringWeights>,
    /// Scoring function
    pub scoring_fn: fn(&ScoringWeights, &ScoringInputs) -> f64,
}

impl Default for ScoringConfig {
    fn default() -> Self {
        Self {
            weights: Default::default(),
            scoring_fn: compute_score,
        }
    }
}

/// Configurable weights for search result scoring.
#[derive(Debug, Clone, Copy)]
pub struct ScoringWeights {
    /// Token coverage ratio
    pub token_coverage: f64,
    /// Exact token match (not just prefix match)
    pub exact_match: f64,
    /// Query token matching in file name
    pub filename_match: f64,
    /// Filename prefix match
    pub filename_prefix_match: f64,
    /// Boost token appearing before in path
    pub path_prefix_match: f64,
    /// Penalty for token appearing in middle of word rather than prefix
    pub midword_penalty: f64,
    /// Maximum recency boost (decays logarithmically)
    pub recency_boost: f64,
    /// Recency decay rate
    pub recency_decay: f64,
    /// File boost (vs directories).
    pub kind_file_boost: f64,
    /// Directory boost (vs files).
    pub kind_dir_boost: f64,
    /// Boost by proximity scoring
    pub proximity_bonus: f64,
    /// Boost by token ordering
    pub ordering_bonus: f64,
}

impl Default for ScoringWeights {
    fn default() -> Self {
        Self {
            token_coverage: 30.0,
            exact_match: 10.0,
            filename_match: 15.0,
            filename_prefix_match: 50.0,
            path_prefix_match: 20.0,
            midword_penalty: 30.0,
            recency_boost: 10.0,
            recency_decay: 2.0,
            kind_file_boost: 2.0,
            kind_dir_boost: 2.0,
            proximity_bonus: 20.0,
            ordering_bonus: 15.0,
        }
    }
}

#[derive(Debug)]
pub struct ScoringInputs<'a> {
    pub path: &'a str,
    pub query_tokens: &'a [String],
    pub raw_query_tokens: &'a [&'a str],
    pub last_modified: u64,
    pub last_accessed: u64,
    pub kind: Kind,
    pub now_micros: f64,
}

pub(crate) fn compute_score(weights: &ScoringWeights, inputs: &ScoringInputs) -> f64 {
    let normalized = if inputs.path.is_ascii() {
        inputs.path.to_lowercase()
    } else {
        crate::tokenizer::fold_path(inputs.path)
    };

    let trimmed_path = normalized.trim_end_matches(|c| c == std::path::MAIN_SEPARATOR);

    let file_name_start_idx = trimmed_path
        .rfind(|c| c == std::path::MAIN_SEPARATOR)
        .map(|i| i + 1)
        .unwrap_or(0);
    let mut score = 0.0;

    let mut unique_matched_indices = Vec::new();

    // Mutually exclusive bonuses
    for token in inputs.query_tokens {
        let t_str = token.as_str();

        let mut is_filename_start = false;
        let mut is_in_filename = false;
        let mut is_in_path = false;
        let mut is_exact_word = false;
        let mut has_any_match = false;

        for (idx, _) in normalized.match_indices(t_str) {
            has_any_match = true;
            unique_matched_indices.push(idx); // Track for coverage

            let start_boundary =
                idx == 0 || !normalized[..idx].chars().last().unwrap().is_alphanumeric();
            let end_boundary = idx + t_str.len() == normalized.len()
                || !normalized[idx + t_str.len()..]
                    .chars()
                    .next()
                    .unwrap()
                    .is_alphanumeric();

            if start_boundary {
                if idx == file_name_start_idx {
                    is_filename_start = true;
                } else if idx >= file_name_start_idx {
                    is_in_filename = true;
                } else {
                    is_in_path = true;
                }

                if end_boundary {
                    is_exact_word = true;
                }
            }
        }

        if is_filename_start {
            score += weights.filename_prefix_match;
        } else if is_in_filename {
            score += weights.filename_match;
        } else if is_in_path {
            score += weights.path_prefix_match;
        } else if has_any_match {
            score -= weights.midword_penalty;
        }

        if is_exact_word {
            score += weights.exact_match;
        }

        if normalized.ends_with(&format!(".{}", t_str)) {
            score -= 30.0;
        }
    }

    // Token coverage
    unique_matched_indices.sort_unstable();
    unique_matched_indices.dedup();

    let path_word_count = trimmed_path
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .count();

    if path_word_count > 0 {
        let effective_length = (path_word_count as f64).min(8.0);
        let coverage_ratio = (unique_matched_indices.len() as f64 / effective_length).min(1.0);
        score += weights.token_coverage * coverage_ratio;
    }

    // Recency and kind boosting
    let recent_date = inputs.last_modified.max(inputs.last_accessed);
    let age_days = (inputs.now_micros - recent_date as f64) / (1_000_000.0 * 86_400.0);
    score += weights.recency_boost - weights.recency_decay * (1.0 + age_days.max(0.0)).ln();

    score += match inputs.kind {
        Kind::Directory => weights.kind_dir_boost,
        Kind::File => weights.kind_file_boost,
        Kind::Symlink => weights.kind_file_boost * 0.5,
    };

    // Proximity and ordering
    if inputs.query_tokens.len() > 1 {
        let mut min_pos = usize::MAX;
        let mut max_pos = 0;
        let mut total_token_len = 0;
        let mut matched_count = 0;

        for q in inputs.query_tokens {
            if let Some(pos) = normalized.find(q.as_str()) {
                min_pos = min_pos.min(pos);
                max_pos = max_pos.max(pos + q.len());
                total_token_len += q.len();
                matched_count += 1;
            }
        }

        if matched_count > 1 && max_pos > min_pos {
            let span = max_pos - min_pos;
            let density = (total_token_len as f64 / span as f64).min(1.0);
            score += weights.proximity_bonus * density;
        }
    }

    if inputs.raw_query_tokens.len() > 1 {
        let mut last_pos = 0;
        let mut is_ordered = true;

        for raw_token in inputs.raw_query_tokens {
            if let Some(pos) = normalized[last_pos..].find(raw_token) {
                last_pos += pos + raw_token.len();
            } else {
                is_ordered = false;
                break;
            }
        }

        if is_ordered {
            score += weights.ordering_bonus;
        }
    }

    score
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_score_basic() {
        let weights = ScoringWeights::default();
        let query_tokens = vec!["abc".to_string()];
        let raw_query_tokens = vec!["abc"];
        let now = 1_000_000.0;
        let inputs1 = ScoringInputs {
            path: "abc.txt",
            query_tokens: &query_tokens,
            raw_query_tokens: &raw_query_tokens,
            last_modified: 1_000_000,
            last_accessed: 1_000_000,
            kind: Kind::File,
            now_micros: now,
        };

        let inputs2 = ScoringInputs {
            path: "other.txt",
            query_tokens: &query_tokens,
            raw_query_tokens: &raw_query_tokens,
            last_modified: 1_000_000,
            last_accessed: 1_000_000,
            kind: Kind::File,
            now_micros: now,
        };

        let score1 = compute_score(&weights, &inputs1);
        let score2 = compute_score(&weights, &inputs2);

        assert!(score1 > score2);
    }

    #[test]
    fn test_compute_score_filename_boost() {
        let config = ScoringWeights::default();
        let query_tokens = vec!["abc".to_string()];
        let raw_query_tokens = vec!["abc"];
        let now = 1_000_000.0;
        let sep = std::path::MAIN_SEPARATOR_STR;

        // "abc" is in the filename vs in the directory path
        let score1 = compute_score(
            &config,
            &ScoringInputs {
                path: &format!("{}foo{}abc{}file.txt", sep, sep, sep),
                query_tokens: &query_tokens,
                raw_query_tokens: &raw_query_tokens,
                last_modified: 1_000_000,
                last_accessed: 1_000_000,
                kind: Kind::File,
                now_micros: now,
            },
        );
        let score2 = compute_score(
            &config,
            &ScoringInputs {
                path: &format!("{}foo{}bar{}abc.txt", sep, sep, sep),
                query_tokens: &query_tokens,
                raw_query_tokens: &raw_query_tokens,
                last_modified: 1_000_000,
                last_accessed: 1_000_000,
                kind: Kind::File,
                now_micros: now,
            },
        );

        // score2 should have a higher boost since "abc" matches the filename "abc.txt"
        assert!(score2 > score1);
    }

    #[test]
    fn test_compute_score_depth_penalty() {
        let config = ScoringWeights::default();
        let query_tokens = vec!["abc".to_string()];
        let raw_query_tokens = vec!["abc"];
        let now = 1_000_000.0;

        let sep = std::path::MAIN_SEPARATOR;
        let path1 = format!("{}abc.txt", sep);
        let path2 = format!("{}foo{}bar{}baz{}abc.txt", sep, sep, sep, sep);

        let score1 = compute_score(
            &config,
            &ScoringInputs {
                path: &path1,
                query_tokens: &query_tokens,
                raw_query_tokens: &raw_query_tokens,
                last_modified: 1_000_000,
                last_accessed: 1_000_000,
                kind: Kind::File,
                now_micros: now,
            },
        );
        let score2 = compute_score(
            &config,
            &ScoringInputs {
                path: &path2,
                query_tokens: &query_tokens,
                raw_query_tokens: &raw_query_tokens,
                last_modified: 1_000_000,
                last_accessed: 1_000_000,
                kind: Kind::File,
                now_micros: now,
            },
        );

        assert!(score1 > score2); // Shallow result should be higher
    }

    #[test]
    fn test_compute_score_recency() {
        let config = ScoringWeights::default();
        let query_tokens = vec!["abc".to_string()];
        let raw_query_tokens = vec!["abc"];
        let now = 2_000_000_000_000.0; // Big "now"

        let score_recent = compute_score(
            &config,
            &ScoringInputs {
                path: "abc.txt",
                query_tokens: &query_tokens,
                raw_query_tokens: &raw_query_tokens,
                last_modified: 1_900_000_000_000,
                last_accessed: 1_900_000_000_000,
                kind: Kind::File,
                now_micros: now,
            },
        );
        let score_old = compute_score(
            &config,
            &ScoringInputs {
                path: "abc.txt",
                query_tokens: &query_tokens,
                raw_query_tokens: &raw_query_tokens,
                last_modified: 1_000_000_000_000,
                last_accessed: 1_000_000_000_000,
                kind: Kind::File,
                now_micros: now,
            },
        );

        assert!(score_recent > score_old);
    }

    #[test]
    fn test_compute_score_ordering() {
        let config = ScoringWeights::default();
        let query_tokens = vec!["foo".to_string(), "bar".to_string()];
        let raw_query_tokens = vec!["foo", "bar"];
        let now = 1_000_000.0;

        let score_ordered = compute_score(
            &config,
            &ScoringInputs {
                path: "foo_bar.txt",
                query_tokens: &query_tokens,
                raw_query_tokens: &raw_query_tokens,
                last_modified: 1_000_000,
                last_accessed: 1_000_000,
                kind: Kind::File,
                now_micros: now,
            },
        );
        let score_unordered = compute_score(
            &config,
            &ScoringInputs {
                path: "bar_foo.txt",
                query_tokens: &query_tokens,
                raw_query_tokens: &raw_query_tokens,
                last_modified: 1_000_000,
                last_accessed: 1_000_000,
                kind: Kind::File,
                now_micros: now,
            },
        );

        assert!(score_ordered > score_unordered);
    }

    #[test]
    #[cfg(windows)]
    fn test_compute_score_windows_paths() {
        let weights = ScoringWeights::default();
        let query_tokens = vec!["report".to_string()];
        let raw_query_tokens = vec!["report"];
        let now = 1_000_000.0;

        // Drive letter filename match
        let score1 = compute_score(
            &weights,
            &ScoringInputs {
                path: "C:\\Users\\joao\\report.pdf",
                query_tokens: &query_tokens,
                raw_query_tokens: &raw_query_tokens,
                last_modified: 1_000_000,
                last_accessed: 1_000_000,
                kind: Kind::File,
                now_micros: now,
            },
        );

        // UNC path filename match
        let score2 = compute_score(
            &weights,
            &ScoringInputs {
                path: "\\\\?\\D:\\Backup\\report.pdf",
                query_tokens: &query_tokens,
                raw_query_tokens: &raw_query_tokens,
                last_modified: 1_000_000,
                last_accessed: 1_000_000,
                kind: Kind::File,
                now_micros: now,
            },
        );

        // Server share filename match
        let score3 = compute_score(
            &weights,
            &ScoringInputs {
                path: "\\\\server\\share\\finance\\report.pdf",
                query_tokens: &query_tokens,
                raw_query_tokens: &raw_query_tokens,
                last_modified: 1_000_000,
                last_accessed: 1_000_000,
                kind: Kind::File,
                now_micros: now,
            },
        );

        // All should have a boost for "report" being the filename
        assert!(score1 > 50.0);
        assert!(score2 > 50.0);
        assert!(score3 > 50.0);
    }
}
