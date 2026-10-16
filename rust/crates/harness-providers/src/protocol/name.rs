//! Portable intersections and provider wire-name grammars (N-05).
#[derive(Clone, Copy)]
pub(crate) struct Name {
    pub maximum: usize,
    pub leading_letter: bool,
    pub punctuation: &'static [u8],
}
impl Name {
    pub fn accepts(self, text: &str) -> bool {
        !text.is_empty()
            && text.len() <= self.maximum
            && (!self.leading_letter
                || text.as_bytes()[0].is_ascii_alphabetic()
                || text.starts_with('_'))
            && text
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || self.punctuation.contains(&byte))
    }
}
pub(crate) const TOOL: Name = Name {
    maximum: 64,
    leading_letter: true,
    punctuation: b"_-",
};
pub(crate) const CALL_ID: Name = Name {
    maximum: 40,
    leading_letter: false,
    punctuation: b"_-",
};
