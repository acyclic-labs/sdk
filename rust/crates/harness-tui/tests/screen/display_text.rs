use acyclic_harness_tui::display_text;
use proptest::strategy::Strategy;
use std::borrow::Cow;

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    // Property: the projection contains no terminal or bidi controls, is
    // idempotent and borrows ordinary Unicode graphemes without alteration.
    #[test]
    fn terminal_text_is_visible_and_idempotent(text in proptest::collection::vec(proptest::prop_oneof![
        proptest::char::any(),
        (0u8..32).prop_map(char::from),
        proptest::sample::select(vec!['\u{061c}', '\u{200e}', '\u{200f}', '\u{202a}', '\u{202b}', '\u{202c}', '\u{202d}', '\u{202e}', '\u{2066}', '\u{2067}', '\u{2068}', '\u{2069}'])
    ], 0..64).prop_map(|characters| characters.into_iter().collect::<String>())) {
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
