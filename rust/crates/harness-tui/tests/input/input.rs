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
    input.keymap = InputKeymap::from_json(
        r#"{"g e":"Ctrl+x Ctrl+e","Ctrl+q Ctrl+e":"Ctrl+x Ctrl+e","Ctrl+q":""}"#,
    )?;
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
    for text in ["first foo", "bar", "second foo"] {
        input.editor.history().push(text.into());
    }
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
