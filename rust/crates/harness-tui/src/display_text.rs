use std::borrow::Cow;

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
