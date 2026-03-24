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
    // Base text scoring (max 1.0)
    pub base_exact_filename: f64,
    pub base_exact_stem: f64,
    pub base_filename_prefix: f64,
    pub base_in_filename: f64,
    pub base_in_path: f64,

    // Penalties
    pub penalty_extension: f64,
    pub penalty_midword: f64,

    // Final multipliers
    /// Maximum boost for brand new files
    pub mult_recency_max: f64,
    /// Recency decay rate
    pub recency_decay_rate: f64,
    /// Directory kind boost
    pub mult_dir: f64,
    /// File kind boost
    pub mult_file: f64,
    /// Maximum token proximity boost
    pub mult_proximity_max: f64,
    /// Token ordering boost
    pub mult_ordered: f64,

    // Globally applied tweaks
    /// Percentage of score immune to depth penalty
    pub coverage_immunity: f64,
}

impl Default for ScoringWeights {
    fn default() -> Self {
        Self {
            base_exact_filename: 1.0,
            base_exact_stem: 0.9,
            base_filename_prefix: 0.7,
            base_in_filename: 0.5,
            base_in_path: 0.4,

            penalty_extension: 0.5, // Cuts token score in half
            penalty_midword: 0.8,   // 20% penalty for not starting on a boundary

            mult_recency_max: 1.2, // 20% max boost for recent files
            recency_decay_rate: 0.1,

            mult_dir: 1.1, // 10% boost to directories
            mult_file: 1.0,

            mult_proximity_max: 1.15, // Up to 15% boost for tight token spacing
            mult_ordered: 1.05,       // 5% boost for correct order

            coverage_immunity: 0.5, // 50% of the text score ignores the depth penalty
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

    let trimmed_path = normalized.trim_end_matches(std::path::MAIN_SEPARATOR);
    let file_name_start_idx = trimmed_path
        .rfind(std::path::MAIN_SEPARATOR)
        .map(|i| i + 1)
        .unwrap_or(0);

    // base scoring
    let mut best_match_quality = 0.0;
    let mut exact_filename_hit = false;
    let mut unique_matched_indices = Vec::new();

    for token in inputs.query_tokens {
        let t_str = token.as_str();
        let mut token_best: f64 = 0.0;

        for (idx, _) in normalized.match_indices(t_str) {
            unique_matched_indices.push(idx);

            let start_boundary =
                idx == 0 || !normalized[..idx].chars().last().unwrap().is_alphanumeric();
            let match_val;

            if start_boundary {
                if idx == file_name_start_idx {
                    let end_idx = idx + t_str.len();
                    if end_idx <= trimmed_path.len() {
                        let remainder = &trimmed_path[end_idx..];
                        if remainder.is_empty() {
                            match_val = weights.base_exact_filename;
                            exact_filename_hit = true;
                        } else if remainder.starts_with('.') {
                            match_val = weights.base_exact_stem;
                        } else {
                            match_val = weights.base_filename_prefix;
                        }
                    } else {
                        match_val = weights.base_in_filename;
                    }
                } else if idx >= file_name_start_idx {
                    match_val = weights.base_in_filename;
                } else {
                    match_val = weights.base_in_path;
                }
            } else {
                // Midword match penalty
                let base = if idx >= file_name_start_idx {
                    weights.base_in_filename
                } else {
                    weights.base_in_path
                };
                match_val = base * weights.penalty_midword;
            }

            token_best = token_best.max(match_val);
        }

        // Extension Penalty
        let ext_str = format!(".{}", t_str);
        if normalized.ends_with(&ext_str) {
            // Did the user explicitly type the dot? (e.g. ".pdf" or "*.pdf")
            let explicit_ext = inputs
                .raw_query_tokens
                .iter()
                .any(|&r| r.ends_with(&ext_str));

            if explicit_ext {
                token_best = token_best.max(weights.base_exact_stem);
                exact_filename_hit = true; // We bypass the Token Coverage Depth
            } else if inputs.query_tokens.len() == 1 {
                token_best *= weights.penalty_extension;
            }
        }

        best_match_quality += token_best;
    }

    let mut base_text_score = if inputs.query_tokens.is_empty() {
        0.0
    } else {
        best_match_quality / inputs.query_tokens.len() as f64
    };

    // exact multi-token overrides
    let filename_str = &trimmed_path[file_name_start_idx..];
    let mut query_chars = inputs.query_tokens.iter().flat_map(|t| t.chars());
    let mut file_name_chars = filename_str.chars().filter(|c| c.is_alphanumeric());

    if query_chars
        .by_ref()
        .zip(file_name_chars.by_ref())
        .all(|(a, b)| a == b)
        && query_chars.next().is_none()
        && file_name_chars.next().is_none()
    {
        base_text_score = weights.base_exact_filename;
        exact_filename_hit = true;
    }

    // Apply multipliers
    let mut final_score = base_text_score * 100.0;

    // Token Coverage Multiplier
    if !exact_filename_hit {
        unique_matched_indices.sort_unstable();
        unique_matched_indices.dedup();
        let path_word_count = trimmed_path
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .count();

        if path_word_count > 0 {
            let coverage =
                (unique_matched_indices.len() as f64 / (path_word_count as f64).min(8.0)).min(1.0);
            let immune = weights.coverage_immunity;
            final_score *= immune + ((1.0 - immune) * coverage);
        }
    }

    // Recency Multiplier
    let recent_date = inputs.last_modified.max(inputs.last_accessed);
    let age_days = (inputs.now_micros - recent_date as f64) / (1_000_000.0 * 86_400.0);
    let recency_bonus = weights.mult_recency_max - 1.0;
    let recency_multiplier = 1.0
        + (recency_bonus
            * std::f64::consts::E.powf(-weights.recency_decay_rate * age_days.max(0.0)));

    final_score *= recency_multiplier;

    // Kind Multiplier
    final_score *= match inputs.kind {
        Kind::Directory => weights.mult_dir,
        Kind::File => weights.mult_file,
        Kind::Symlink => weights.mult_file * 0.9,
    };

    // Proximity & Ordering
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
            let prox_bonus = weights.mult_proximity_max - 1.0;
            final_score *= 1.0 + (prox_bonus * density);
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
            final_score *= weights.mult_ordered;
        }
    }

    final_score
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

        assert!(score1 > score2);
    }

    #[test]
    fn test_exact_filename_bypasses_depth_penalty() {
        let config = ScoringWeights::default();
        let query_tokens = vec!["lib".to_string(), "rs".to_string()];
        let raw_query_tokens = vec!["lib", "rs"];
        let now = 1_000_000.0;
        let sep = std::path::MAIN_SEPARATOR_STR;

        let shallow = compute_score(
            &config,
            &ScoringInputs {
                path: &format!("{}lib.rs", sep),
                query_tokens: &query_tokens,
                raw_query_tokens: &raw_query_tokens,
                last_modified: 1_000_000,
                last_accessed: 1_000_000,
                kind: Kind::File,
                now_micros: now,
            },
        );
        let deep = compute_score(
            &config,
            &ScoringInputs {
                path: &format!("{}my_app{}src{}lib.rs", sep, sep, sep),
                query_tokens: &query_tokens,
                raw_query_tokens: &raw_query_tokens,
                last_modified: 1_000_000,
                last_accessed: 1_000_000,
                kind: Kind::File,
                now_micros: now,
            },
        );

        assert_eq!(shallow, deep);
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
                last_modified: 1_900_000_000_000, // Very recent
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
                last_modified: 1_000_000_000_000, // Very old
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

        // The ordered tokens enjoy the 1.05x ordering_bonus
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

        assert!(score1 > 50.0);
        assert!(score2 > 50.0);
        assert!(score3 > 50.0);
    }
}
