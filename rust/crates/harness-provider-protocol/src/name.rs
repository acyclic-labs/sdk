//! Portable intersections and provider wire-name grammars (N-05).
/// ASCII grammar for a wire identifier.
#[derive(Clone, Copy)]
pub struct Name {
    /// Maximum identifier length in bytes.
    pub maximum: usize,
    /// Whether the first byte must be an ASCII letter.
    pub leading_letter: bool,
    /// Permitted punctuation in addition to ASCII alphanumerics.
    pub punctuation: &'static [u8],
}
impl Name {
    /// Check this grammar against a wire identifier.
    pub fn accepts(self, text: &str) -> bool {
        !text.is_empty()
            && text.len() <= self.maximum
            && (!self.leading_letter
                || text.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
                || text.starts_with('_'))
            && text
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || self.punctuation.contains(&byte))
    }
}
/// Tool-name grammar shared across adapters.
pub const TOOL: Name = Name {
    maximum: 64,
    leading_letter: true,
    punctuation: b"_-",
};
/// Call identifier grammar shared across adapters.
pub const CALL_ID: Name = Name {
    maximum: 40,
    leading_letter: false,
    punctuation: b"_-",
};
