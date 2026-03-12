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
