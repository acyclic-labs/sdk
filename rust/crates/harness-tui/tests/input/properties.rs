use acyclic_harness_tui_core::key_event::{KeyCode, KeyEvent, KeyModifiers};
use acyclic_harness_tui::{ClipboardContent, InputEffect, PromptInput, prepare_image, unix_editor_command};
use acyclic_harness_tui_core::{InputKeymap, KEYMAP};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig { cases: 32, ..ProptestConfig::default() })]
    #[test]
    fn history_snapshots_and_keymap_overrides_round_trip(
        text in "[a-z界\r\n]{0,48}", owner in "[a-z]{1,8}", target in prop::sample::select(KEYMAP)
    ) {
        let mut input = PromptInput::default();
        let effects = input.key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE), 0, 80);
        let [InputEffect::History { request, .. }] = effects.as_slice() else { return Err(TestCaseError::fail("history request missing")); };
        let recalled = format!("{owner}:{text}");
        prop_assert!(input.resolve_history(*request, "", Some(&recalled), true, 0));
        prop_assert_eq!(input.editor.text(), &format!("{owner}:{}", text.replace("\r\n", "\n").replace('\r', "\n")));
        prop_assert!(!input.editor.text().contains('\r'));
        let json = serde_json::json!({"bindings":{"Ctrl+l":target.action.name()}}).to_string();
        let map = InputKeymap::parse(&json).unwrap();
        prop_assert_eq!(InputKeymap::parse(&map.encode()).unwrap(), map);
    }

    #[test]
    fn editor_argument_quoting_preserves_original_arguments(args in prop::collection::vec("[a-zA-Z0-9 _/.-]{0,12}", 0..6)) {
        let words: Vec<&str> = std::iter::once("editor").chain(args.iter().map(String::as_str)).collect();
        let command = shell_words::join(words);
        let (program, restored) = unix_editor_command(&command)?;
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
        input.keymap = InputKeymap::parse(r#"{"bindings":{"g e":"external-editor"}}"#).unwrap();
        for (index, character) in text.chars().enumerate() {
            let at = index as u64 * 1000;
            input.key(KeyEvent::new(KeyCode::Char(character), KeyModifiers::NONE), at, 80);
            input.tick(at + 750);
        }
        prop_assert_eq!(input.editor.text(), text);
    }
    #[test]
    fn edited_and_normal_submissions_keep_the_original_query_contract(
        submissions in prop::collection::vec((any::<bool>(), any::<bool>(), "[a-z界\r\n]{0,16}"), 0..24)
    ) {
        use acyclic_harness_tui_core::Editor;
        let mut input = PromptInput::new(Editor::new("draft"));
        for (step, (edit, cancel, raw)) in submissions.into_iter().enumerate() {
            let text = format!("entry:{raw}");
            let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
            if edit { input.edit_message("message".into(), &text); }
            else { input.editor.set_text(&text, step as u64); }
            if edit && cancel {
                input.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE), step as u64, 80);
            } else {
                let effects = input.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), step as u64, 80);
                prop_assert_eq!(effects, vec![InputEffect::Submit { text: normalized, queued: false, edit: edit.then(|| "message".into()) }]);
            }
        }
    }

    #[test]
    fn cancelling_search_restores_history_navigation_and_draft(raw in "[a-z界\r\n]{0,24}", matches in any::<bool>()) {
        use acyclic_harness_tui_core::Editor;
        let draft = format!("draft:{raw}");
        let expected = draft.replace("\r\n", "\n").replace('\r', "\n");
        let mut input = PromptInput::new(Editor::new(&draft));
        input.editor.set_cursor(0, false);
        let effects = input.key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE), 0, 80);
        let [InputEffect::History { request, .. }] = effects.as_slice() else { return Err(TestCaseError::fail("history request missing")); };
        prop_assert!(input.resolve_history(*request, &expected, Some("newer"), true, 1));
        input.editor.set_cursor(5, false);
        input.key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::CONTROL), 2, 80);
        let effects = input.committed_text(if matches { "older" } else { "missing" }, 3);
        let [InputEffect::History { request, .. }] = effects.as_slice() else { return Err(TestCaseError::fail("search request missing")); };
        prop_assert!(input.resolve_history(*request, "newer", matches.then_some("older"), true, 4));
        input.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE), 5, 80);
        prop_assert_eq!((input.editor.text(), input.editor.cursor()), ("newer", 5));
        let effects = input.key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE), 6, 80);
        let [InputEffect::History { request, .. }] = effects.as_slice() else { return Err(TestCaseError::fail("draft request missing")); };
        prop_assert!(input.resolve_history(*request, "newer", Some(&expected), false, 7));
        prop_assert_eq!(input.editor.text(), expected);
    }

    #[test]
    fn replacing_history_source_invalidates_in_flight_replies(
        draft in "[a-z界\r\n]{0,24}", stale in "[a-z界]{0,12}", reloads in 1usize..8
    ) {
        use acyclic_harness_tui_core::Editor;
        let mut input = PromptInput::new(Editor::new(&draft));
        input.editor.set_cursor(0, false);
        let expected = input.editor.text().to_owned();
        let effects = input.key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE), 0, 80);
        let [InputEffect::History { request, .. }] = effects.as_slice() else { return Err(TestCaseError::fail("history request missing")); };
        for _ in 0..reloads { input.reset_history_source(); }
        prop_assert!(!input.resolve_history(*request, &expected, Some(&stale), true, 1));
        prop_assert_eq!(input.editor.text(), expected);
    }
}
