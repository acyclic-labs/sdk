use acyclic_harness_tui::key_event::{KeyCode, KeyEvent, KeyModifiers};
use acyclic_harness_tui::{
    ClipboardContent, Editor, InputKeymap, KEYMAP, PromptInput, editor_command, format_history,
    parse_history, prepare_image,
};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig { cases: 32, ..ProptestConfig::default() })]
    #[test]
    fn history_file_and_keymap_overrides_round_trip(
        text in "[a-z界\r\n]{0,48}", owner in "[a-z]{1,8}", target in 0usize..KEYMAP.len()
    ) {
        let raw = serde_json::to_string(&vec![format!("{owner}:{text}")])?;
        let history = parse_history(&raw)?;
        let expected = format_history(&history)?;
        prop_assert_eq!(format_history(&parse_history(&expected)?)?, expected);
        prop_assert_eq!(&history.entries()[0], &format!("{owner}:{}", text.replace("\r\n", "\n").replace('\r', "\n")));
        prop_assert!(history.entries().iter().all(|entry| !entry.contains('\r')));
        let json = serde_json::to_string(&std::collections::BTreeMap::from([("Ctrl+l", KEYMAP[target].chord)]))?;
        let map = InputKeymap::from_json(&json)?;
        prop_assert_eq!(InputKeymap::from_json(&map.to_json()?)?, map);
    }

    #[test]
    fn editor_argument_quoting_preserves_original_arguments(args in prop::collection::vec("[a-zA-Z0-9 _/.-]{0,12}", 0..6)) {
        let words: Vec<&str> = std::iter::once("editor").chain(args.iter().map(String::as_str)).collect();
        let command = shell_words::join(words);
        let (program, restored) = editor_command(&command)?;
        prop_assert_eq!(program, std::ffi::OsString::from("editor"));
        prop_assert_eq!(restored, args.iter().map(std::ffi::OsString::from).collect::<Vec<_>>());

    }

    #[test]
    fn clipboard_image_encoding_round_trip_preserves_dimensions(width in 1u32..12, height in 1u32..12, color in any::<[u8;4]>()) {
        let pixels = color.repeat((width * height) as usize);
        let ClipboardContent::Image { jpeg, width: encoded_width, height: encoded_height } = prepare_image(width, height, pixels)? else {
            return Err(TestCaseError::fail("expected image"));
        };
        let image = image::load_from_memory(&jpeg)?;
        prop_assert_eq!((encoded_width, encoded_height), (width, height));
        prop_assert_eq!((image.width(), image.height()), (width, height));
    }

    #[test]
    fn arbitrary_input_sequences_preserve_grapheme_invariants(data in prop::collection::vec(any::<u8>(), 0..128)) {
        super::sequences::exercise(&data);
    }

    #[test]
    fn expired_chord_prefixes_preserve_text(text in "[a-z]{0,32}") {
        let mut input = PromptInput::default();
        input.keymap = InputKeymap::from_json(r#"{"g e":"Ctrl+x Ctrl+e"}"#)?;
        for (index, character) in text.chars().enumerate() {
            let at = index as u64 * 1000;
            input.key(KeyEvent::new(KeyCode::Char(character), KeyModifiers::NONE), at, 80);
            input.tick(at + 750);
        }
        prop_assert_eq!(input.editor.text(), text);
    }
}
