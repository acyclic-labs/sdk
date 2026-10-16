//! Completion request parsing, provider contracts and popup snapshots.
#![cfg(feature = "tui")]

use acyclic_harness_tui::InputKeymap;
use acyclic_harness_tui::key_event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use acyclic_harness_tui::ratatui::{
    Terminal, backend::TestBackend, buffer::Buffer, layout::Rect, widgets::Widget,
};
use acyclic_harness_tui::{
    Completion, CompletionKind, CompletionMenu, CompletionPopup, CompletionProvider,
    CompletionRequest, Editor, completion_request, fuzzy_score,
};
use proptest::prelude::*;

#[path = "support/completion_sequences.rs"]
mod sequences;

#[test]
fn completion_regression_corpus() {
    for seed in [
        include_bytes!("../fuzz/corpus/completion_events/slash").as_slice(),
        include_bytes!("../fuzz/corpus/completion_events/mention").as_slice(),
        include_bytes!("../fuzz/corpus/completion_events/quotes").as_slice(),
        include_bytes!("../fuzz/corpus/completion_events/unicode").as_slice(),
    ] {
        assert!(sequences::exercise(seed).is_ok());
    }
}

#[test]
#[should_panic(expected = "completion preserves the mention marker")]
fn fuzz_negative_control_rejects_a_dropped_mention_marker() {
    assert!(sequences::exercise_broken(b"negative control").is_ok());
}

struct Files;
impl CompletionProvider for Files {
    fn complete(&self, request: &CompletionRequest) -> Vec<Completion> {
        if request.kind != CompletionKind::Mention {
            return vec![];
        }
        ["src/readme.md", "src/a b.rs", "界/👩‍💻.rs", "@memo.rs"]
            .into_iter()
            .map(|path| Completion {
                label: path.into(),
                value: path.into(),
                description: String::new(),
                tool: false,
                append_space: true,
            })
            .collect()
    }
}

#[test]
fn file_completion_is_fuzzy_keeps_at_and_quotes_spaces() {
    for (text, expected) in [
        ("read @srm", "read @src/readme.md "),
        ("@ab", "@\"src/a b.rs\" "),
        ("@a", "@\"src/a b.rs\" "),
        ("@@m", "@@memo.rs "),
        ("@\u{1f469}", "@\u{754c}/\u{1f469}\u{200d}\u{1f4bb}.rs "),
    ] {
        let mut editor = Editor::new(text);
        let mut menu = CompletionMenu::default();
        menu.refresh(&editor, &[&Files]);
        assert!(menu.active(&editor));
        assert!(menu.accept(&mut editor, 0));
        assert_eq!(editor.text(), expected);
        editor.undo();
        assert_eq!(editor.text(), text);
        assert_eq!(editor.cursor(), text.len());
        assert!(editor.selection().is_empty());
    }
    let editor = Editor::new("@zz");
    let mut menu = CompletionMenu::default();
    menu.refresh(&editor, &[&Files]);
    assert!(!menu.active(&editor));
    for text in [
        "a@b.com",
        "hi a@b",
        "\\@src",
        "bareword",
        " /diff",
        "@\"src\"tail",
        "@\"a @src\"tail",
    ] {
        assert_eq!(completion_request(&Editor::new(text)), None);
    }
}

#[test]
fn quoted_and_mid_token_requests_replace_the_whole_token() {
    let mut editor = Editor::new("read @src/readme old");
    editor.set_cursor(9, false);
    let request = completion_request(&editor);
    assert_eq!(request.as_ref().map(|r| r.query.as_str()), Some("src"));
    assert_eq!(request.as_ref().map(|r| r.range.clone()), Some(5..16));
    let mut menu = CompletionMenu::default();
    menu.refresh(&editor, &[&Files]);
    assert!(menu.accept(&mut editor, 0));
    assert_eq!(editor.text(), "read @src/readme.md old");
    assert_eq!((editor.cursor(), editor.anchor()), (20, 20));
    assert!(editor.undo());
    assert_eq!(
        (editor.text(), editor.cursor(), editor.anchor()),
        ("read @src/readme old", 9, 9)
    );
    let mut editor = Editor::new("@\"src/a b\"");
    assert_eq!(
        completion_request(&editor).map(|r| r.query),
        Some("src/a b".into())
    );
    let mut menu = CompletionMenu::default();
    menu.refresh(&editor, &[&Files]);
    assert!(menu.accept(&mut editor, 0));
    assert_eq!(editor.text(), "@\"src/a b.rs\" ");
}

struct Commands;
impl CompletionProvider for Commands {
    fn complete(&self, request: &CompletionRequest) -> Vec<Completion> {
        if request.kind != CompletionKind::Command {
            return vec![];
        }
        [("theme", false), ("think", false), ("thread", true)]
            .into_iter()
            .filter(|(name, _)| name.starts_with(&request.query))
            .map(|(name, tool)| Completion {
                label: format!("/{name}"),
                value: name.into(),
                description: String::new(),
                tool,
                append_space: false,
            })
            .collect()
    }
}

#[test]
fn prompt_controller_accepts_completion_before_submit_and_prioritizes_escape() {
    use acyclic_harness_tui::{Action, InputEffect, PromptInput};
    let mut input = PromptInput::new(Editor::new("/th"));
    input.refresh_completions(&[&Commands]);
    assert!(
        input
            .key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), 0, 80)
            .is_empty()
    );
    assert_eq!(input.editor.text(), "/thread ");
    assert_eq!(
        input.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), 1, 80),
        vec![InputEffect::Submit {
            text: "/thread ".into(),
            queued: false,
            edit: None
        }]
    );

    input.editor.set_text("@srm", 2);
    input.refresh_completions(&[&Files]);
    assert!(
        input
            .key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE), 3, 80)
            .is_empty()
    );
    assert_eq!(input.editor.text(), "@src/readme.md ");
    assert!(input.editor.undo());
    assert_eq!(input.editor.text(), "@srm");

    input.busy = true;
    input.refresh_completions(&[&Files]);
    assert!(
        input
            .key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE), 4, 80)
            .is_empty()
    );
    assert_eq!(
        input.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE), 5, 80),
        vec![InputEffect::Host(Action::Escape)]
    );
    input.edit_message("old".into(), "/th");
    input.refresh_completions(&[&Commands]);
    assert!(
        input
            .key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE), 6, 80)
            .is_empty()
    );
    assert_eq!(input.editor.text(), "@srm");
    assert_eq!(input.editing_message(), None);
    assert!(!input.completion.active(&input.editor));
}

#[test]
fn popup_keys_accept_without_submission_and_use_the_effective_keymap()
-> Result<(), Box<dyn std::error::Error>> {
    for code in [KeyCode::Tab, KeyCode::Enter] {
        let mut editor = Editor::new("/th");
        let mut menu = CompletionMenu::default();
        menu.refresh(&editor, &[&Commands]);
        let mut key = KeyEvent::new(code, KeyModifiers::NONE);
        key.kind = KeyEventKind::Release;
        assert!(!menu.key(&mut editor, key, &InputKeymap::default(), 0));
        key.kind = KeyEventKind::Press;
        assert!(menu.key(&mut editor, key, &InputKeymap::default(), 1));
        assert_eq!(editor.text(), "/thread ");
        assert!(!menu.active(&editor));
    }
    let mut editor = Editor::new("/th");
    let mut menu = CompletionMenu::default();
    menu.refresh(&editor, &[&Commands]);
    let map = InputKeymap::from_json(r#"{"Ctrl+j":"Enter"}"#)?;
    assert!(menu.key(
        &mut editor,
        KeyEvent::new(KeyCode::Up, KeyModifiers::NONE),
        &map,
        0
    ));
    assert_eq!(menu.selected, 2);
    assert!(menu.key(
        &mut editor,
        KeyEvent::new(KeyCode::Char('j'), KeyModifiers::CONTROL),
        &map,
        1
    ));
    assert_eq!(editor.text(), "/think");
    Ok(())
}

#[test]
fn tools_sort_first_navigation_wraps_and_acceptance_is_one_undo_step() {
    let mut editor = Editor::new("/th");
    let mut menu = CompletionMenu::default();
    menu.refresh(&editor, &[&Commands]);
    assert!(menu.items[0].tool);
    menu.cycle(true);
    assert_eq!(menu.selected, 2);
    menu.refresh(&editor, &[&Commands]);
    assert_eq!(menu.selected, 2);
    menu.cycle(false);
    assert_eq!(menu.selected, 0);
    assert!(menu.accept(&mut editor, 0));
    assert_eq!(editor.text(), "/thread ");
    editor.undo();
    assert_eq!(editor.text(), "/th");
    let mut theme = Editor::new("/them");
    menu.refresh(&theme, &[&Commands]);
    assert!(menu.accept(&mut theme, 1));
    assert_eq!(theme.text(), "/theme ");
}

#[test]
fn stale_results_dismissals_and_folded_pastes_do_not_accept_completion() {
    let mut editor = Editor::new("@src");
    let mut menu = CompletionMenu::default();
    menu.refresh(&editor, &[&Files]);
    editor.insert("x", 0);
    assert!(!menu.accept(&mut editor, 1));
    assert_eq!(editor.text(), "@srcx");
    editor = Editor::new("@src");
    menu.refresh(&editor, &[&Files]);
    menu.dismiss(&editor);
    menu.refresh(&editor, &[&Files]);
    assert!(!menu.active(&editor));
    editor.insert("/", 2);
    menu.refresh(&editor, &[&Files]);
    assert!(menu.active(&editor));
    editor.select_all();
    assert_eq!(completion_request(&editor), None);
    editor.paste(&format!("@{}", "a".repeat(1100)), 3);
    assert_eq!(completion_request(&editor), None);
}

#[test]
fn popup_snapshot_and_geometry_stay_within_terminal_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let editor = Editor::new("/th");
    let mut menu = CompletionMenu::default();
    menu.refresh(&editor, &[&Commands]);
    let mut terminal = Terminal::new(TestBackend::new(30, 5))?;
    terminal.draw(|frame| {
        let mut popup = CompletionPopup::new(&menu, &editor);
        popup.selected_style = Default::default();
        frame.render_widget(popup, frame.area());
    })?;
    assert_eq!(
        terminal.backend().buffer(),
        &Buffer::with_lines([
            "┌Completions─────────────────┐",
            "│/thread [tool]              │",
            "│/theme                      │",
            "│/think                      │",
            "└────────────────────────────┘",
        ])
    );
    for width in 1..=80 {
        for height in 1..=24 {
            let screen = Rect::new(0, 0, width, height);
            let popup = CompletionPopup::new(&menu, &editor);
            let area = popup.area(
                Rect::new(0, height.saturating_sub(3), width, 3.min(height)),
                screen,
            );
            assert!(area.right() <= screen.right() && area.bottom() <= screen.bottom());
            assert!(area.width <= 60 && area.height <= 12);
            let mut buffer = Buffer::empty(screen);
            popup.render(area, &mut buffer);
        }
    }
    Ok(())
}

#[test]
fn unicode_popup_rows_are_clipped_by_display_cells() -> Result<(), Box<dyn std::error::Error>> {
    struct Unicode;
    impl CompletionProvider for Unicode {
        fn complete(&self, _: &CompletionRequest) -> Vec<Completion> {
            vec![Completion {
                label: "\u{754c}\u{1f469}\u{200d}\u{1f4bb}e\u{301}".into(),
                value: "file".into(),
                description: String::new(),
                tool: false,
                append_space: true,
            }]
        }
    }
    let editor = Editor::new("@f");
    let mut menu = CompletionMenu::default();
    menu.refresh(&editor, &[&Unicode]);
    for width in 1u16..=12 {
        let mut terminal = Terminal::new(TestBackend::new(width, 3))?;
        terminal.draw(|frame| {
            let mut popup = CompletionPopup::new(&menu, &editor);
            popup.selected_style = Default::default();
            frame.render_widget(popup, frame.area());
        })?;
        if width < 3 {
            assert_eq!(
                terminal.backend().buffer(),
                &Buffer::empty(Rect::new(0, 0, width, 3))
            );
            continue;
        }
        let inner = width - 2;
        let (visible, cells) = match inner {
            1 => ("", 0),
            2 | 3 => ("\u{754c}", 2),
            4 => ("\u{754c}\u{1f469}\u{200d}\u{1f4bb}", 4),
            _ => ("\u{754c}\u{1f469}\u{200d}\u{1f4bb}e\u{301}", 5),
        };
        let expected = Buffer::with_lines([format!(
            "│{visible}{}│",
            " ".repeat(usize::from(inner - cells))
        )]);
        for x in 0..width {
            assert_eq!(
                &terminal.backend().buffer()[(x, 1)],
                &expected[(x, 0)],
                "width {width}, column {x}"
            );
        }
    }
    Ok(())
}

#[test]
fn acceptance_reuses_an_existing_separator_and_rejects_invalid_values() {
    let mut editor = Editor::new("read @srm rest");
    editor.set_cursor(9, false);
    let mut menu = CompletionMenu::default();
    menu.refresh(&editor, &[&Files]);
    assert!(menu.accept(&mut editor, 0));
    assert_eq!(editor.text(), "read @src/readme.md rest");
    for value in ["", "bad\nvalue", "bad\"value", "bad\u{1b}value"] {
        struct Invalid<'a>(&'a str);
        impl CompletionProvider for Invalid<'_> {
            fn complete(&self, _: &CompletionRequest) -> Vec<Completion> {
                vec![Completion {
                    label: self.0.into(),
                    value: self.0.into(),
                    description: String::new(),
                    tool: false,
                    append_space: false,
                }]
            }
        }
        let mut editor = Editor::new("@");
        menu.refresh(&editor, &[&Invalid(value)]);
        assert!(!menu.accept(&mut editor, 1));
        assert_eq!((editor.text(), editor.cursor()), ("@", 1));
    }
}

#[test]
fn completing_before_whitespace_starts_the_next_argument_and_preserves_pastes() {
    use acyclic_harness_tui::PromptInput;
    let mut input = PromptInput::new(Editor::new("@srm old"));
    input.editor.set_cursor(4, false);
    input.refresh_completions(&[&Files]);
    assert!(
        input
            .key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE), 0, 80)
            .is_empty()
    );
    assert_eq!((input.editor.cursor(), input.editor.anchor()), (15, 15));
    input.committed_text("new ", 1);
    assert_eq!(input.editor.text(), "@src/readme.md new old");
    assert!(input.editor.undo());
    assert_eq!(input.editor.text(), "@src/readme.md old");
    assert!(input.editor.undo());
    assert_eq!(
        (input.editor.text(), input.editor.cursor()),
        ("@srm old", 4)
    );
    assert!(input.editor.redo());
    assert_eq!(
        (input.editor.text(), input.editor.cursor()),
        ("@src/readme.md old", 15)
    );

    for (tail, separator) in [
        ("\told", "\t"),
        (" \u{301}old", " \u{301}"),
        ("\u{a0}old", "\u{a0}"),
        ("\nold", " "),
    ] {
        let mut editor = Editor::new(&format!("@srm{tail}"));
        editor.set_cursor(4, false);
        let mut menu = CompletionMenu::default();
        menu.refresh(&editor, &[&Files]);
        assert!(menu.accept(&mut editor, 0));
        assert_eq!(editor.cursor(), "@src/readme.md".len() + separator.len());
        let expected = if tail.starts_with('\n') {
            format!("@src/readme.md {tail}")
        } else {
            format!("@src/readme.md{tail}")
        };
        assert_eq!(editor.text(), expected);
    }
    let mut editor = Editor::new("@srm");
    let paste = " ".repeat(1100);
    editor.paste(&paste, 0);
    editor.set_cursor(4, false);
    let mut menu = CompletionMenu::default();
    menu.refresh(&editor, &[&Files]);
    assert!(menu.accept(&mut editor, 1));
    assert_eq!(editor.text(), format!("@src/readme.md {paste}"));
    assert_eq!(editor.collapsed_pastes().len(), 1);
    assert_eq!(
        (editor.cursor(), editor.collapsed_pastes()[0].start),
        (15, 15)
    );
    assert!(editor.undo());
    assert_eq!(editor.text(), format!("@srm{paste}"));
    assert_eq!(
        (editor.cursor(), editor.collapsed_pastes()[0].start),
        (4, 4)
    );
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]
    #[test]
    fn completion_round_trip_preserves_original_text_and_cursor(path in prop::collection::vec(
        any::<char>().prop_filter("path syntax", |c| !c.is_control() && *c != '"'), 1..24
    ).prop_map(|chars| chars.into_iter().collect::<String>()), suffix in "[a-z]{0,12}") {
        use unicode_segmentation::UnicodeSegmentation;
        let original = format!("before @x{suffix} after");
        let mut editor = Editor::new(&original);
        editor.set_cursor(8, false);
        struct Single(String);
        impl CompletionProvider for Single {
            fn complete(&self, _: &CompletionRequest) -> Vec<Completion> {
                vec![Completion { label: self.0.clone(), value: self.0.clone(), description: String::new(), tool: false, append_space: true }]
            }
        }
        let provider = Single(path.clone());
        let mut menu = CompletionMenu::default(); menu.refresh(&editor, &[&provider]);
        prop_assert!(menu.accept(&mut editor, 0));
        let token = if path.chars().any(char::is_whitespace) { format!("@\"{path}\"") } else { format!("@{path}") };
        let accepted = format!("before {token} after");
        let desired = 7 + token.len() + 1;
        let cursor = accepted.grapheme_indices(true).map(|(i, _)| i).chain([accepted.len()]).find(|i| *i >= desired).unwrap_or(accepted.len());
        prop_assert_eq!(editor.text(), &accepted);
        prop_assert_eq!((editor.cursor(), editor.anchor()), (cursor, cursor));
        let request = completion_request(&Editor::new(&token));
        prop_assert_eq!(request.map(|r| r.query), Some(path));
        prop_assert!(editor.undo());
        prop_assert_eq!(editor.text(), original);
        prop_assert_eq!((editor.cursor(), editor.anchor()), (8, 8));
        prop_assert!(editor.redo()); prop_assert_eq!(editor.text(), accepted);
        prop_assert_eq!((editor.cursor(), editor.anchor()), (cursor, cursor));
    }

    #[test]
    fn fuzzy_matching_agrees_with_a_subsequence_reference(query in ".{0,16}", path in ".{0,32}") {
        let lowered: Vec<_> = path.chars().flat_map(char::to_lowercase).collect();
        let mut remaining = lowered.as_slice();
        let mut matched = true;
        for character in query.chars().flat_map(char::to_lowercase) {
            match remaining.iter().position(|c| *c == character) {
                Some(index) => remaining = &remaining[index + 1..],
                None => { matched = false; break; }
            }
        }
        prop_assert_eq!(fuzzy_score(&query, &path).is_some(), matched);
    }

    #[test]
    fn arbitrary_completion_requests_have_grapheme_aligned_ranges(text in ".{0,64}", cursor in 0usize..128) {
        let mut editor = Editor::new(&text); editor.set_cursor(cursor, false);
        if let Some(request) = completion_request(&editor) {
            use unicode_segmentation::UnicodeSegmentation;
            let points: Vec<_> = editor.text().grapheme_indices(true).map(|(i, _)| i).chain([editor.text().len()]).collect();
            prop_assert!(points.contains(&request.range.start) && points.contains(&request.range.end));
            prop_assert!(request.range.start <= editor.cursor() && editor.cursor() <= request.range.end);
        }
    }

    #[test]
    fn popup_geometry_handles_a_prompt_outside_the_screen(
        x in any::<u16>(), y in any::<u16>(), width in any::<u16>(), height in any::<u16>()
    ) {
        let editor = Editor::new("/th");
        let mut menu = CompletionMenu::default(); menu.refresh(&editor, &[&Commands]);
        let screen = Rect::new(3, 4, 80, 24);
        let area = CompletionPopup::new(&menu, &editor).area(Rect::new(x, y, width, height), screen);
        prop_assert!(area.x >= screen.x && area.y >= screen.y);
        prop_assert!(area.right() <= screen.right() && area.bottom() <= screen.bottom());
    }

    #[test]
    fn arbitrary_completion_sequences_preserve_graphemes_and_atomic_undo(data in prop::collection::vec(any::<u8>(), 0..128)) {
        sequences::exercise(&data)?;
    }
}
