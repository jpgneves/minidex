use unicode_normalization::{UnicodeNormalization, char::is_combining_mark};

/// A basic Unicode-aware tokenizer.
pub fn tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut prev_char: Option<char> = None;

    // Helper closure to fold, strip, and lowercase the token
    let push_token = |t: &mut Vec<String>, c: &mut String| {
        if !c.is_empty() {
            // Fast path normalization to avoid unicode normalization
            if c.is_ascii() {
                t.push(c.to_ascii_lowercase());
            } else {
                // NFD normalization and combining mark stripping
                let folded: String = c.nfd().filter(|ch| !is_combining_mark(*ch)).collect();
                t.push(folded.to_lowercase());
            }
            c.clear();
        }
    };

    for c in input.chars() {
        if !c.is_alphanumeric() || c == '\u{2014}' {
            push_token(&mut tokens, &mut current);
            prev_char = Some(c);
            continue;
        }

        if let Some(p) = prev_char {
            // Use camelCase transitions as token boundaries
            let is_camel = p.is_lowercase() && c.is_uppercase();

            // Transitioning from non-numeric to numeric characters
            let is_num_transition =
                (p.is_alphabetic() && c.is_numeric()) || (p.is_numeric() && c.is_alphabetic());

            // If the character falls into common CJK Unicode blocks, we split.
            // This forces Japanese/Chinese characters to be heavily fragmented,
            // allowing substring-like matching even without spaces.
            let is_cjk_transition = is_cjk(p) || is_cjk(c);

            if is_camel || is_num_transition || is_cjk_transition {
                push_token(&mut tokens, &mut current);
            }
        }

        current.push(c);
        prev_char = Some(c);
    }

    push_token(&mut tokens, &mut current);

    // Pre-process the (potential) path
    let trimmed = input.trim_end_matches(std::path::MAIN_SEPARATOR);

    // Find the last slash
    let file_name = match trimmed.rfind(std::path::MAIN_SEPARATOR) {
        Some(idx) => &trimmed[idx + 1..],
        None => trimmed,
    };

    // Extract the filename as a token if it contains an extension
    if file_name.contains('.') && file_name.starts_with(char::is_alphanumeric) {
        if file_name.is_ascii() {
            tokens.push(file_name.to_ascii_lowercase());
        } else {
            let folded: String = file_name
                .nfd()
                .filter(|ch| !is_combining_mark(*ch))
                .collect();
            tokens.push(folded.to_lowercase());
        }
    }

    tokens.sort_unstable();
    tokens.dedup();

    tokens
}

/// A fast, rough check for Chinese, Japanese, and Korean Unicode blocks.
fn is_cjk(c: char) -> bool {
    let u = c as u32;
    // Ranges cover Hiragana, Katakana, CJK Unified Ideographs, and Hangul
    (0x3040..=0x309F).contains(&u) || // Hiragana
    (0x30A0..=0x30FF).contains(&u) || // Katakana
    (0x4E00..=0x9FFF).contains(&u) || // CJK Unified Ideographs
    (0xAC00..=0xD7AF).contains(&u) // Hangul Syllables
}

/// Folds an entire path string for substring matching in the in-memory index
pub(crate) fn fold_path(input: &str) -> String {
    input
        .nfd()
        .filter(|ch| !is_combining_mark(*ch))
        .collect::<String>()
        .to_lowercase()
}

const SYNTH_PATH_TOKEN_TAG: char = '\x00';
const SYNTH_VOLUME_TOKEN_TAG: char = '\x01';
const SYNTH_EXT_TOKEN_TAG: char = '\x02';

pub(crate) fn synthesize_path_token(orig: &str) -> String {
    format!("{SYNTH_PATH_TOKEN_TAG}{orig}")
}

pub(crate) fn synthesize_volume_token(orig: &str) -> String {
    format!("{SYNTH_VOLUME_TOKEN_TAG}{orig}")
}

pub(crate) fn synthesize_ext_token(orig: &str) -> String {
    format!("{SYNTH_EXT_TOKEN_TAG}{orig}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_basic() {
        let tokens = tokenize("hello world");
        assert_eq!(tokens, vec!["hello", "world"]);
    }

    #[test]
    fn test_tokenize_camel_case() {
        let tokens = tokenize("MySuperFile");
        assert_eq!(tokens, vec!["file", "my", "super"]);
    }

    #[test]
    fn test_tokenize_numeric_transition() {
        let tokens = tokenize("report2023.txt");
        assert_eq!(tokens, vec!["2023", "report", "report2023.txt", "txt"]);
    }

    #[test]
    fn test_tokenize_normalization() {
        // "e" + combining acute accent
        let tokens = tokenize("café");
        assert_eq!(tokens, vec!["cafe"]);
    }

    #[test]
    fn test_tokenize_cjk() {
        // "日本語" (Japanese)
        let tokens = tokenize("日本語");
        // CJK characters should be fragmented
        assert_eq!(tokens, vec!["日", "本", "語"]);

        // "한국어" (Korean)
        let tokens2 = tokenize("한국어");
        // Korean characters are decomposed in NFD
        // "국", "어", "한" (sorted)
        assert_eq!(tokens2, vec!["국", "어", "한"]);
    }

    #[test]
    fn test_tokenize_cyrillic() {
        let tokens = tokenize("Документ.txt");
        // Cyrillic is usually not fragmented like CJK unless there are boundaries
        // but it should be lowercased and normalized.
        // Tokens are sorted: ["txt", "документ", "документ.txt"]
        assert_eq!(tokens, vec!["txt", "документ", "документ.txt"]);
    }

    #[test]
    fn test_fold_path() {
        assert_eq!(fold_path("Café/Report_2023"), "cafe/report_2023");
    }

    #[test]
    fn test_synthetic_tokens() {
        assert_eq!(synthesize_path_token("abc"), "\x00abc");
        assert_eq!(synthesize_volume_token("c:"), "\x01c:");
        assert_eq!(synthesize_ext_token("pdf"), "\x02pdf");
    }

    #[test]
    fn test_tokenize_case_insensitivity() {
        // Mixed case word that matches camelCase pattern (lower followed by upper)
        // "hElLo" -> h (lower) + E (upper) -> split
        // "E" (upper) + l (lower) -> no split
        // "l" (lower) + L (upper) -> split
        let tokens = tokenize("hElLo");
        assert_eq!(tokens, vec!["el", "h", "lo"]);

        let tokens2 = tokenize("Hello HELLO");
        assert_eq!(tokens2, vec!["hello"]);
    }
}
