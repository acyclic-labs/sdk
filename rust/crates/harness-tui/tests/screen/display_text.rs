use acyclic_harness_tui::{display_text, display_text_prefix};
use proptest::strategy::Strategy;
use std::borrow::Cow;

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    // Property: the projection contains no terminal or bidi controls, is
    // idempotent and borrows ordinary Unicode graphemes without alteration.
    #[test]
    fn terminal_text_is_visible_and_idempotent(page in 4usize..24, text in proptest::collection::vec(proptest::prop_oneof![
        proptest::char::any(),
        (0u8..32).prop_map(char::from),
        proptest::sample::select(vec!['\u{061c}', '\u{200e}', '\u{200f}', '\u{202a}', '\u{202b}', '\u{202c}', '\u{202d}', '\u{202e}', '\u{2066}', '\u{2067}', '\u{2068}', '\u{2069}'])
    ], 0..64).prop_map(|characters| characters.into_iter().collect::<String>())) {
        // Byte pages must reconstruct the exact source, including multibyte
        // characters crossing a cut. A successful nonfinal page advances by
        // at least credit minus the three possible UTF-8 suffix bytes.
        let mut reconstructed = String::with_capacity(text.len());
        let mut at = 0;
        while at < text.len() {
            let end = (at + page).min(text.len());
            let bytes = &text.as_bytes()[at..end];
            let prefix = display_text_prefix(bytes, end == text.len())?;
            proptest::prop_assert_eq!(prefix.as_ptr(), bytes.as_ptr());
            proptest::prop_assert!(!prefix.is_empty());
            if end < text.len() {
                proptest::prop_assert!(prefix.len() >= page - 3);
            }
            if !text.is_char_boundary(end) {
                // Strict EOF and a decoder without suffix retention must
                // both refuse the very same cut rather than lose bytes.
                proptest::prop_assert!(display_text_prefix(bytes, true).is_err());
                proptest::prop_assert!(std::str::from_utf8(bytes).is_err());
            }
            reconstructed.push_str(prefix);
            at += prefix.len();
        }
        proptest::prop_assert_eq!(&reconstructed, &text);
        // Invalid source bytes cannot become a successful lossy display,
        // whether the source is complete or has a continuation.
        let mut malformed = text.as_bytes().to_vec();
        malformed.push(0xff);
        proptest::prop_assert!(display_text_prefix(&malformed, false).is_err());
        proptest::prop_assert!(display_text_prefix(&malformed, true).is_err());
        let visible = display_text(&text);
        for character in visible.chars() {
            proptest::prop_assert!(!character.is_control() || character == '\t');
            proptest::prop_assert!(!matches!(character, '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'), "hidden character: {:?}", character);
        }
        let again = display_text(&visible);
        proptest::prop_assert_eq!(again.as_ref(), visible.as_ref());
        if matches!(visible, Cow::Borrowed(_)) {
            proptest::prop_assert_eq!(visible.as_ref(), text.as_str());
        }
    }
}

#[test]
fn ordinary_emoji_and_combining_clusters_are_borrowed() {
    let text = "日本語 é 👩‍💻\t";
    assert!(matches!(display_text(text), Cow::Borrowed(value) if value == text));
}
