use acyclic_harness_tui::key_event::{
    InputEvent, KeyCode, KeyEvent, KeyModifiers as M, MouseButton, MouseEvent, MouseEventKind,
};
use acyclic_harness_tui::ratatui::layout::Rect;
use acyclic_harness_tui::{Action, Editor, InputEffect, InputKeymap, PromptInput, Viewport};

fn press(input: &mut PromptInput, code: KeyCode, mods: M, at: u64) -> Vec<InputEffect> {
    input.key(KeyEvent::new(code, mods), at, 80)
}

#[test]
fn copy_interrupt_and_double_quit_do_not_consume_other_shortcuts() {
    let mut input = PromptInput::new(Editor::new("draft"));
    input.editor.select_all();
    assert_eq!(
        press(&mut input, KeyCode::Char('c'), M::CONTROL, 0),
        vec![InputEffect::Copy("draft".into())]
    );
    assert_eq!(input.editor.text(), "draft");
    input.editor.set_cursor(5, false);
    input.transcript_selection = Some("**transcript**".into());
    assert_eq!(
        press(&mut input, KeyCode::Char('c'), M::CONTROL, 0),
        vec![InputEffect::Copy("**transcript**".into())]
    );
    input.transcript_selection = None;
    input.editor.set_cursor(5, false);
    assert!(press(&mut input, KeyCode::Char('c'), M::CONTROL, 1).is_empty());
    assert_eq!(input.editor.text(), "");
    input.committed_text("new", 2);
    assert!(press(&mut input, KeyCode::Char('c'), M::CONTROL, 3).is_empty());
    assert_eq!(
        press(&mut input, KeyCode::Char('c'), M::CONTROL, 4),
        vec![InputEffect::Quit]
    );
    assert_eq!(
        press(&mut input, KeyCode::Char('d'), M::CONTROL, 1000),
        vec![InputEffect::Quit]
    );
    input.committed_text("keep draft", 1001);
    input.busy = true;
    assert_eq!(
        press(&mut input, KeyCode::Char('c'), M::CONTROL, 1002),
        vec![InputEffect::Interrupt]
    );
    assert_eq!(input.editor.text(), "keep draft");
    assert_eq!(
        press(&mut input, KeyCode::PageUp, M::NONE, 1003),
        vec![InputEffect::Host(Action::TranscriptUp)]
    );
    assert_eq!(
        press(&mut input, KeyCode::Char(';'), M::CONTROL, 1004),
        vec![InputEffect::Host(Action::Voice)]
    );
}

#[test]
fn cut_chord_preserves_selection_and_ordinary_cut_timing() {
    let mut input = PromptInput::new(Editor::new("draft"));
    input.editor.select_all();
    assert!(press(&mut input, KeyCode::Char('x'), M::CONTROL, 0).is_empty());
    assert_eq!(
        press(&mut input, KeyCode::Char('e'), M::CONTROL, 10),
        vec![InputEffect::ExternalEditor]
    );
    assert_eq!(input.editor.selected_text(), "draft");
    input.external_result(Some("edited\r\ntext"), 11);
    assert_eq!(input.editor.text(), "edited\ntext");
    input.editor.undo();
    assert_eq!(input.editor.selected_text(), "draft");
    press(&mut input, KeyCode::Char('x'), M::SUPER, 100);
    assert_eq!(
        press(&mut input, KeyCode::Char('e'), M::SUPER, 101),
        vec![InputEffect::ExternalEditor]
    );
    input.external_result(None, 102);
    assert_eq!(input.editor.text(), "draft");
    press(&mut input, KeyCode::Char('x'), M::CONTROL, 200);
    assert!(input.tick(949).is_empty());
    assert_eq!(input.tick(950), vec![InputEffect::Copy("draft".into())]);
    assert_eq!(input.editor.text(), "");
    input.editor = Editor::new("original");
    input.editor.set_cursor(2, false);
    input.external_result(Some("external"), 1000);
    assert!(input.editor.undo());
    assert_eq!(input.editor.text(), "original");
    assert_eq!((input.editor.cursor(), input.editor.anchor()), (2, 2));
}

#[test]
fn configured_chords_defer_and_replay_the_prefix_action() -> Result<(), Box<dyn std::error::Error>>
{
    let mut input = PromptInput::default();
    input.keymap = InputKeymap::parse(
        r#"{"bindings":{"g e":"external-editor","Ctrl+q Ctrl+e":"external-editor","Ctrl+q":null}}"#,
    )
    .unwrap();
    assert!(press(&mut input, KeyCode::Char('g'), M::NONE, 0).is_empty());
    assert_eq!(input.editor.text(), "");
    assert!(input.tick(749).is_empty());
    assert!(input.tick(750).is_empty());
    assert_eq!(input.editor.text(), "g");
    press(&mut input, KeyCode::Char('g'), M::NONE, 800);
    assert_eq!(
        press(&mut input, KeyCode::Char('e'), M::NONE, 801),
        vec![InputEffect::ExternalEditor]
    );
    assert_eq!(input.editor.text(), "g");
    press(&mut input, KeyCode::Char('g'), M::NONE, 900);
    press(&mut input, KeyCode::Char('z'), M::NONE, 901);
    assert_eq!(input.editor.text(), "ggz");
    press(&mut input, KeyCode::Char('q'), M::CONTROL, 1000);
    assert_eq!(
        press(&mut input, KeyCode::Char('e'), M::CONTROL, 1001),
        vec![InputEffect::ExternalEditor]
    );
    assert!(input.keymap.help().contains("g e\t"));
    press(&mut input, KeyCode::Char('g'), M::NONE, 1100);
    assert!(input.external_result(None, 1101).is_empty());
    assert_eq!(input.editor.text(), "ggzg");
    Ok(())
}

#[test]
fn reverse_search_is_a_reversible_preview_not_a_series_of_edits() {
    let mut input = PromptInput::new(Editor::new("draft"));
    let mut history = acyclic_harness_tui::PromptHistory::default();
    for text in ["first foo", "bar", "second foo"] {
        history.push(text.into());
    }
    input.replace_history(history);
    press(&mut input, KeyCode::Char('r'), M::CONTROL, 0);
    input.committed_text("foo", 1);
    assert_eq!(input.editor.text(), "second foo");
    press(&mut input, KeyCode::Char('r'), M::CONTROL, 2);
    assert_eq!(input.editor.text(), "first foo");
    press(&mut input, KeyCode::Esc, M::NONE, 3);
    assert_eq!(input.editor.text(), "draft");
    press(&mut input, KeyCode::Char('r'), M::CONTROL, 4);
    input.committed_text("bar", 5);
    assert_eq!(input.editor.text(), "bar");
    assert!(press(&mut input, KeyCode::Enter, M::NONE, 6).is_empty());
    assert_eq!(input.editor.text(), "bar");
    input.editor.undo();
    assert_eq!(input.editor.text(), "draft");
}

#[test]
fn event_paste_and_ime_commits_preserve_order_and_submission_semantics() {
    let mut input = PromptInput::default();
    let area = Rect::new(0, 0, 80, 4);
    let mut viewport = Viewport::default();
    input.event(
        InputEvent::Key(KeyEvent::new(KeyCode::Char('a'), M::NONE)),
        area,
        &mut viewport,
        0,
    );
    input.event(
        InputEvent::CommittedText("界👩‍💻".repeat(200)),
        area,
        &mut viewport,
        1,
    );
    assert!(input.editor.text().starts_with('a'));
    assert!(input.editor.collapsed_pastes().is_empty());
    input.question = true;
    input.event(InputEvent::Paste("x\r\ny".into()), area, &mut viewport, 2);
    assert!(input.comment_mode);
    assert!(input.editor.text().ends_with("x\ny"));
    input.busy = true;
    let text = input.editor.text().to_owned();
    assert_eq!(
        input.event(
            InputEvent::Key(KeyEvent::new(KeyCode::Enter, M::NONE)),
            area,
            &mut viewport,
            3
        ),
        vec![InputEffect::Submit {
            text,
            queued: true,
            edit: None
        }]
    );
}

#[test]
fn message_edit_and_dock_routing_restore_live_drafts() {
    let mut input = PromptInput::new(Editor::new("draft"));
    input.edit_message("message-1".into(), "old");
    input.committed_text("!", 0);
    press(&mut input, KeyCode::Esc, M::NONE, 1);
    assert_eq!(input.editor.text(), "draft");
    input.edit_message("message-1".into(), "old");
    assert_eq!(
        press(&mut input, KeyCode::Enter, M::NONE, 2),
        vec![InputEffect::Submit {
            text: "old".into(),
            queued: false,
            edit: Some("message-1".into())
        }]
    );
    assert_eq!(input.editor.text(), "draft");
    input.editor = Editor::default();
    input.dock_items = true;
    assert_eq!(
        press(&mut input, KeyCode::Up, M::NONE, 3),
        vec![InputEffect::Host(Action::PreviousDock)]
    );
    assert_eq!(
        press(&mut input, KeyCode::Tab, M::NONE, 4),
        vec![InputEffect::Host(Action::Tab)]
    );
    input.dock_items = false;
    input.committed_text("界", 5);
    press(&mut input, KeyCode::Tab, M::NONE, 6);
    assert_eq!(input.editor.text(), "界  ");
}

#[test]
fn escape_cancels_an_edit_then_prioritizes_host_pause_over_selection() {
    let mut input = PromptInput::new(Editor::new("draft"));
    input.busy = true;
    input.edit_message("old".into(), "edit");
    assert!(press(&mut input, KeyCode::Esc, M::NONE, 0).is_empty());
    assert_eq!(input.editor.text(), "draft");
    input.editor.select_all();
    assert_eq!(
        press(&mut input, KeyCode::Esc, M::NONE, 1),
        vec![InputEffect::Host(Action::Escape)]
    );
    assert_eq!(input.editor.selected_text(), "draft");
    input.busy = false;
    assert!(press(&mut input, KeyCode::Esc, M::NONE, 2).is_empty());
    assert!(input.editor.selection().is_empty());
}

#[test]
fn mouse_click_drag_double_triple_and_cursor_animation() {
    let mut input = PromptInput::new(Editor::new("界 word\nnext"));
    let area = Rect::new(2, 3, 12, 3);
    let mut viewport = Viewport::default();
    let mouse = |kind, column, row| MouseEvent {
        kind,
        column,
        row,
        modifiers: M::NONE,
    };
    input.mouse(
        mouse(MouseEventKind::Down(MouseButton::Left), 2, 3),
        area,
        &mut viewport,
        0,
    );
    input.mouse(
        mouse(MouseEventKind::Drag(MouseButton::Left), 5, 3),
        area,
        &mut viewport,
        1,
    );
    assert_eq!(input.editor.selected_text(), "界 ");
    input.mouse(
        mouse(MouseEventKind::Down(MouseButton::Left), 5, 3),
        area,
        &mut viewport,
        1000,
    );
    input.mouse(
        mouse(MouseEventKind::Down(MouseButton::Left), 5, 3),
        area,
        &mut viewport,
        1100,
    );
    assert_eq!(input.editor.selected_text(), "word");
    input.mouse(
        mouse(MouseEventKind::Down(MouseButton::Left), 5, 3),
        area,
        &mut viewport,
        1200,
    );
    assert_eq!(input.editor.selected_text(), "界 word\n");
    assert_eq!(input.cursor_opacity(1200, true), 255);
    assert_eq!(input.cursor_opacity(2200, true), 0);
    assert_eq!(input.cursor_opacity(2200, false), 0);
}

#[test]
fn shift_click_extends_selection_and_release_survives_a_zero_size_area() {
    let mut input = PromptInput::new(Editor::new("abcd"));
    let mut viewport = Viewport::default();
    let area = Rect::new(0, 0, 8, 1);
    let mut mouse = MouseEvent {
        kind: MouseEventKind::Down(MouseButton::Left),
        column: 1,
        row: 0,
        modifiers: M::NONE,
    };
    input.mouse(mouse, area, &mut viewport, 0);
    mouse.column = 3;
    mouse.modifiers = M::SHIFT;
    input.mouse(mouse, area, &mut viewport, 1);
    assert_eq!(input.editor.selected_text(), "bc");
    mouse.kind = MouseEventKind::Up(MouseButton::Left);
    input.mouse(mouse, Rect::default(), &mut viewport, 2);
    mouse.kind = MouseEventKind::Drag(MouseButton::Left);
    mouse.column = 7;
    input.mouse(mouse, area, &mut viewport, 3);
    assert_eq!(input.editor.selected_text(), "bc");
}

#[test]
fn cancelling_reverse_search_preserves_down_to_the_original_draft() {
    let mut input = PromptInput::new(Editor::new("working draft"));
    let mut history = acyclic_harness_tui::PromptHistory::default();
    history.push("older".into());
    history.push("newer".into());
    input.replace_history(history);
    press(&mut input, KeyCode::Home, M::NONE, 0);
    press(&mut input, KeyCode::Up, M::NONE, 1);
    assert_eq!(input.editor.text(), "newer");
    press(&mut input, KeyCode::End, M::NONE, 2);
    press(&mut input, KeyCode::Char('r'), M::CONTROL, 3);
    input.committed_text("older", 4);
    assert_eq!(input.editor.text(), "older");
    press(&mut input, KeyCode::Esc, M::NONE, 5);
    assert_eq!((input.editor.text(), input.editor.cursor()), ("newer", 5));
    press(&mut input, KeyCode::Down, M::NONE, 6);
    assert_eq!(input.editor.text(), "working draft");
}

#[test]
fn edited_submissions_append_to_the_shared_history_file() -> Result<(), Box<dyn std::error::Error>>
{
    use acyclic_harness_tui::{load_history, save_history};
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("input-history.json");
    std::fs::write(&path, r#"["first"]"#)?;
    let mut input = PromptInput::new(Editor::new("working draft"));
    input.replace_history(load_history(&path)?);
    input.edit_message("old-message".into(), "original");
    // A live-query snapshot changes history while the prior message is edited.
    std::fs::write(&path, r#"["first","external"]"#)?;
    input.replace_history(load_history(&path)?);
    input.external_result(Some("edited\r\nmessage"), 0);
    assert_eq!(
        press(&mut input, KeyCode::Enter, M::NONE, 1),
        vec![InputEffect::Submit {
            text: "edited\nmessage".into(),
            queued: false,
            edit: Some("old-message".into())
        }]
    );
    assert_eq!(input.editor.text(), "working draft");
    save_history(&path, input.history())?;
    input.replace_history(load_history(&path)?);
    assert_eq!(
        input.history().entries(),
        &["first", "external", "edited\nmessage"]
    );
    press(&mut input, KeyCode::Home, M::NONE, 2);
    press(&mut input, KeyCode::Up, M::NONE, 3);
    assert_eq!(input.editor.text(), "edited\nmessage");
    input.edit_message("other".into(), "cancelled");
    press(&mut input, KeyCode::Esc, M::NONE, 4);
    press(&mut input, KeyCode::End, M::CONTROL, 5);
    press(&mut input, KeyCode::Down, M::NONE, 6);
    assert_eq!(input.editor.text(), "working draft");
    assert_eq!(load_history(&path)?.entries(), input.history().entries());
    Ok(())
}

#[test]
fn history_reload_invalidates_navigation_saved_by_edit_or_search()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_harness_tui::load_history;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("history.json");
    for search in [false, true] {
        for replacement in [
            r#"["replacement"]"#,
            "",
            "[]",
            r#"["first","second","third","fourth","fifth"]"#,
        ] {
            std::fs::write(&path, r#"["first","second","third","fourth","fifth"]"#)?;
            let mut input = PromptInput::new(Editor::new("working draft"));
            input.replace_history(load_history(&path)?);
            press(&mut input, KeyCode::Home, M::CONTROL, 0);
            press(&mut input, KeyCode::Up, M::NONE, 1);
            press(&mut input, KeyCode::Up, M::NONE, 2);
            press(&mut input, KeyCode::End, M::CONTROL, 3);
            assert_eq!(input.editor.text(), "fourth");
            if search {
                press(&mut input, KeyCode::Char('r'), M::CONTROL, 4);
                input.committed_text("first", 5);
            } else {
                input.edit_message("message".into(), "edited message");
            }
            let generation = input.history().generation();
            std::fs::write(&path, replacement)?;
            let snapshot = load_history(&path)?;
            assert!(
                snapshot.generation() > 0,
                "every file load has a generation"
            );
            let entries = snapshot.entries().to_vec();
            input.replace_history(snapshot);
            assert!(input.history().generation() > generation);
            press(&mut input, KeyCode::Esc, M::NONE, 6);
            assert_eq!((input.editor.text(), input.editor.cursor()), ("fourth", 6));
            let mut recall = input.clone();
            press(&mut recall, KeyCode::Home, M::CONTROL, 7);
            press(&mut recall, KeyCode::Up, M::NONE, 8);
            assert_eq!(
                recall.editor.text(),
                entries.last().map_or("fourth", String::as_str),
                "Up must immediately reach the reloaded history"
            );
            press(&mut input, KeyCode::Down, M::NONE, 7);
            assert_eq!(
                input.editor.text(),
                "fourth",
                "stale navigation must be reset"
            );
            assert_eq!(input.history().entries(), entries);
            press(&mut input, KeyCode::Home, M::CONTROL, 8);
            press(&mut input, KeyCode::Up, M::NONE, 9);
            assert_eq!(
                input.editor.text(),
                entries.last().map_or("fourth", String::as_str)
            );
        }
    }
    Ok(())
}
