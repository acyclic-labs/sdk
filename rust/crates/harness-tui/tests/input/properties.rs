use acyclic_harness_tui::key_event::{KeyCode, KeyEvent, KeyModifiers};
use acyclic_harness_tui::{
    ClipboardContent, InputKeymap, KEYMAP, PromptInput, format_history, parse_history,
    prepare_image, unix_editor_command,
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
        let json = serde_json::json!({"bindings":{"Ctrl+l":KEYMAP[target].action.name()}}).to_string();
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
    fn edited_and_normal_submissions_share_one_history_projection(
        submissions in prop::collection::vec((any::<bool>(), any::<bool>(), "[a-z界\r\n]{0,16}"), 0..24)
    ) {
        use acyclic_harness_tui::{Editor, InputEffect};
        let mut input = PromptInput::new(Editor::new("draft"));
        let mut expected: Vec<String> = vec![];
        for (step, (edit, cancel, raw)) in submissions.into_iter().enumerate() {
            let text = format!("entry:{raw}");
            let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
            if edit { input.edit_message("message".into(), &text); }
            else { input.editor.set_text(&text, step as u64); }
            // Live snapshots replace the projection, never a saved editor's history.
            input.replace_history(parse_history(&format_history(input.history())?)?);
            if edit && cancel {
                input.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE), step as u64, 80);
            } else {
                let effects = input.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), step as u64, 80);
                prop_assert_eq!(effects, vec![InputEffect::Submit { text: normalized.clone(), queued: false, edit: edit.then(|| "message".into()) }]);
                if expected.last() != Some(&normalized) { expected.push(normalized); }
            }
            prop_assert_eq!(input.history().entries(), expected.as_slice());
            let restored = parse_history(&format_history(input.history())?)?;
            prop_assert_eq!(restored.entries(), expected.as_slice());
        }
    }

    #[test]
    fn cancelling_search_restores_history_navigation_and_draft(raw in "[a-z界\r\n]{0,24}", matches in any::<bool>()) {
        use acyclic_harness_tui::Editor;
        let draft = format!("draft:{raw}");
        let expected = draft.replace("\r\n", "\n").replace('\r', "\n");
        let mut input = PromptInput::new(Editor::new(&draft));
        let mut history = acyclic_harness_tui::PromptHistory::default();
        history.push("older".into()); history.push("newer".into());
        input.replace_history(history);
        for (at, code, mods) in [(0, KeyCode::Home, KeyModifiers::CONTROL), (1, KeyCode::Up, KeyModifiers::NONE),
            (2, KeyCode::End, KeyModifiers::NONE), (3, KeyCode::Char('r'), KeyModifiers::CONTROL)] {
            input.key(KeyEvent::new(code, mods), at, 80);
        }
        input.committed_text(if matches { "older" } else { "missing" }, 4);
        input.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE), 5, 80);
        prop_assert_eq!((input.editor.text(), input.editor.cursor()), ("newer", 5));
        input.key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE), 6, 80);
        prop_assert_eq!(input.editor.text(), expected);
    }

    #[test]
    fn restored_navigation_never_survives_a_history_snapshot_reload(
        search in any::<bool>(), replacements in prop::collection::vec("[a-z界\r\n]{0,12}", 0..8),
        reloads in 1usize..8
    ) {
        use acyclic_harness_tui::Editor;
        let mut input = PromptInput::new(Editor::new("draft"));
        input.replace_history(parse_history(r#"["first","second","third"]"#)?);
        for (at, code, mods) in [(0, KeyCode::Home, KeyModifiers::CONTROL),
            (1, KeyCode::Up, KeyModifiers::NONE), (2, KeyCode::Up, KeyModifiers::NONE),
            (3, KeyCode::End, KeyModifiers::CONTROL)] {
            input.key(KeyEvent::new(code, mods), at, 80);
        }
        if search { input.key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::CONTROL), 4, 80); }
        else { input.edit_message("message".into(), "editing"); }
        let json = serde_json::to_string(&replacements)?;
        let before = input.history().generation();
        for _ in 0..reloads { input.replace_history(parse_history(&json)?); }
        prop_assert_eq!(input.history().generation(), before + reloads as u64);
        let entries = input.history().entries().to_vec();
        input.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE), 5, 80);
        input.key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE), 6, 80);
        prop_assert_eq!((input.editor.text(), input.editor.cursor()), ("second", 6));
        prop_assert_eq!(input.history().entries(), entries);
    }

}
