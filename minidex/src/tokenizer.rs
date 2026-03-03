/// A basic Unicode-aware tokenizer.
pub(crate) fn tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut prev_char: Option<char> = None;

    for c in input.chars() {
        if !c.is_alphanumeric() || c == '\u{2014}' {
            if !current.is_empty() {
                tokens.push(current.to_lowercase());
                current.clear();
            }
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

            if (is_camel || is_num_transition || is_cjk_transition) && !current.is_empty() {
                tokens.push(current.to_lowercase());
                current.clear();
            }
        }

        current.push(c);
        prev_char = Some(c);
    }

    if !current.is_empty() {
        tokens.push(current.to_lowercase());
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
