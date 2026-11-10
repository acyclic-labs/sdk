use std::borrow::Cow;

/// Borrows the complete UTF-8 prefix of a byte page. An incomplete trailing
/// character is left for the source continuation only when more bytes remain;
/// malformed bytes and an incomplete character at EOF are errors.
pub fn display_text_prefix(bytes: &[u8], complete: bool) -> Result<&str, std::str::Utf8Error> {
    match std::str::from_utf8(bytes) {
        Ok(text) => Ok(text),
        Err(error) if !complete && error.error_len().is_none() => {
            std::str::from_utf8(&bytes[..error.valid_up_to()])
        }
        Err(error) => Err(error),
    }
}

fn hidden(character: char) -> bool {
    (character.is_control() && character != '\t')
        || matches!(character, '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
}

/// Make terminal controls and bidi direction overrides visible in one display
/// line. Tabs retain the shared layout's expansion policy; graphemes and text
/// without hidden controls are borrowed unchanged.
pub fn display_text(text: &str) -> Cow<'_, str> {
    if !text.chars().any(hidden) {
        return Cow::Borrowed(text);
    }
    let mut visible = String::with_capacity(text.len());
    for character in text.chars() {
        if hidden(character) {
            visible.extend(character.escape_debug());
        } else {
            visible.push(character);
        }
    }
    Cow::Owned(visible)
}
