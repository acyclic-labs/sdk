use acyclic_harness_tui::{Editor, Motion, PromptHistory};
use proptest::prelude::*;

fn incoming_text() -> impl Strategy<Value = String> {
    prop::collection::vec(prop_oneof![Just('\r'), Just('\n'), any::<char>()], 0..48)
        .prop_map(|characters| characters.into_iter().collect())
}

fn state(editor: &Editor) -> (String, usize, usize) {
    (editor.text().to_owned(), editor.cursor(), editor.anchor())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    #[test]
    fn every_text_entry_path_keeps_carriage_returns_out_of_the_buffer(raw in incoming_text()) {
        let expected = raw.replace("\r\n", "\n").replace('\r', "\n");
        // Construction is also the load path for persisted draft text.
        let mut editor = Editor::new(&raw);
        prop_assert_eq!(editor.text(), &expected);
        for path in 0..4 {
            editor.select_all();
            match path {
                0 => editor.insert(&raw, 0),
                1 => editor.paste(&raw, 1),
                2 => editor.replace_selection(&raw, 2),
                _ => editor.set_text(&raw, 3),
            }
            prop_assert_eq!(editor.text(), &expected);
            prop_assert!(!editor.text().contains('\r'));
            let mut probe = editor.clone();
            if probe.undo() {
                prop_assert!(!probe.text().contains('\r'));
                prop_assert!(probe.redo());
                prop_assert!(!probe.text().contains('\r'));
            }
        }

        let mut history = PromptHistory::default();
        history.push(format!("saved:{raw}"));
        prop_assert_eq!(history.entries(), &[format!("saved:{expected}")]);
        prop_assert_eq!(history.previous(&raw), Some(format!("saved:{expected}")));
        prop_assert_eq!(history.next_entry(), Some(expected.clone()));
        // Adjacent duplicates are compared after normalization.
        history.push(format!("saved:{expected}"));
        prop_assert_eq!(history.entries().len(), 1);

        history.push(format!("saved:{raw}"));
        editor.set_cursor(0, false);
        prop_assert!(editor.recall(&mut history, true, 4));
        prop_assert_eq!(editor.text(), format!("saved:{expected}"));
        editor.move_cursor(Motion::End, false, 80);
        prop_assert!(editor.recall(&mut history, false, 5));
        prop_assert_eq!(editor.text(), expected);
    }

    #[test]
    fn recall_round_trip_restores_the_final_cursor_and_allows_another_up(
        raw in incoming_text(), draft in incoming_text()
    ) {
        let mut editor = Editor::new(&draft);
        let mut history = PromptHistory::default();
        history.push(format!("older:{raw}"));
        history.push(format!("newer:{raw}"));
        editor.set_cursor(0, false);
        let before = state(&editor);
        prop_assert!(editor.recall(&mut history, true, 0));
        let after = state(&editor);
        prop_assert_eq!((after.1, after.2), (0, 0));
        prop_assert!(editor.undo());
        prop_assert_eq!(state(&editor), before);
        prop_assert!(editor.redo());
        prop_assert_eq!(state(&editor), after);
        prop_assert!(editor.recall(&mut history, true, 1));
        prop_assert!(editor.text().starts_with("older:"));
        prop_assert!(!editor.text().contains('\r'));
    }

    #[test]
    fn recalled_multiline_prompts_have_correct_line_motion_and_line_kills(
        first in "[a-z界]{1,12}", second in "[a-z界]{1,12}", third in "[a-z界]{1,12}",
        separator in prop_oneof![Just("\r\n"), Just("\r"), Just("\n")]
    ) {
        let mut editor = Editor::default();
        let mut history = PromptHistory::default();
        history.push(format!("{first}{separator}{second}{separator}{third}"));
        prop_assert!(editor.recall(&mut history, true, 0));
        let expected = format!("{first}\n{second}\n{third}");
        prop_assert_eq!(editor.text(), &expected);
        prop_assert_eq!(editor.layout(usize::MAX).rows.len(), 3);
        editor.move_cursor(Motion::LogicalDown, false, 80);
        prop_assert_eq!(editor.cursor(), first.len() + 1);
        editor.move_cursor(Motion::LineEnd, false, 80);
        prop_assert_eq!(editor.cursor(), first.len() + 1 + second.len());
        editor.kill(Motion::LineStart, 1);
        prop_assert_eq!(editor.text(), format!("{first}\n\n{third}"));
        prop_assert!(editor.undo());
        prop_assert_eq!(editor.text(), &expected);
        editor.move_cursor(Motion::LineStart, false, 80);
        editor.kill(Motion::LineEnd, 2);
        prop_assert_eq!(editor.text(), format!("{first}\n\n{third}"));
        prop_assert!(editor.undo());
        editor.set_cursor(first.len(), false);
        editor.kill(Motion::LineEnd, 3);
        prop_assert_eq!(editor.text(), format!("{first}{second}\n{third}"));
        prop_assert!(!editor.text().contains('\r'));
    }
}
