//! Composed-screen parity: layout, prompt routing, persistence and resize.
#![cfg(feature = "tui")]
use acyclic_harness_tui::ratatui::{Terminal, backend::TestBackend, layout::Rect, text::Line};
use acyclic_harness_tui::{Editor, PromptInput, Screen, Viewport};
use proptest::prelude::*;

#[path = "screen/wrapping.rs"]
mod wrapping;

fn visible(terminal: &Terminal<TestBackend>) -> String {
    terminal
        .backend()
        .buffer()
        .content
        .iter()
        .map(|cell| cell.symbol())
        .collect()
}

#[test]
fn composition_renders_followed_panels_prompt_and_status() -> Result<(), Box<dyn std::error::Error>>
{
    let transcript = [Line::from("streaming assistant")];
    let terminal_rows = [Line::from("shell output")];
    let diff = [Line::from("+new line")];
    let goal = [Line::from("finish rewrite")];
    let plans = [Line::from("implement layout")];
    let notices = [Line::from("task completed")];
    let screen = Screen {
        header: Line::from("parent / ● current"),
        transcript: &transcript,
        terminal: &terminal_rows,
        diff: &diff,
        goal: &goal,
        plans: &plans,
        notifications: &notices,
        status: Line::from("Esc · model · tokens"),
        ..Screen::default()
    };
    let input = PromptInput::new(Editor::new("draft"));
    let mut viewport = Viewport::default();
    let mut terminal = Terminal::new(TestBackend::new(100, 30))?;
    terminal.draw(|frame| {
        screen.render(frame, &input, &mut viewport, 0, 1000);
    })?;
    let text = visible(&terminal);
    for expected in [
        "parent / ● current",
        "streaming assistant",
        "shell output",
        "+new line",
        "GOAL.md",
        "finish rewrite",
        "plans/",
        "implement layout",
        "task completed",
        "draft",
        "Esc · model · tokens",
    ] {
        assert!(text.contains(expected), "missing {expected}");
    }
    assert!(viewport.cursor.is_some());
    Ok(())
}

#[test]
fn completion_overlays_above_prompt_and_resize_preserves_input()
-> Result<(), Box<dyn std::error::Error>> {
    let screen = Screen::default();
    let mut input = PromptInput::new(Editor::new("界\n@sec"));
    input
        .completion
        .set_file("workspace/second", Some(b"source"))?;
    input.refresh_completions();
    let mut viewport = Viewport::default();
    let mut terminal = Terminal::new(TestBackend::new(100, 20))?;
    terminal.draw(|frame| {
        let areas = screen.render(frame, &input, &mut viewport, 0, 1000);
        assert_eq!(areas.completions.bottom(), areas.prompt.y);
        assert_eq!(areas.completions.width, 60);
    })?;
    assert!(visible(&terminal).contains("Completions"));
    terminal.backend_mut().resize(20, 10);
    terminal.resize(Rect::new(0, 0, 20, 10))?;
    terminal.draw(|frame| {
        screen.render(frame, &input, &mut viewport, 0, 1000);
    })?;
    assert!(visible(&terminal).contains("second"));
    assert_eq!(input.editor.text(), "界\n@sec");
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]
    // Property: every visible region stays inside the supplied viewport,
    // primary regions never overlap, and prompt growth stays bounded.
    #[test]
    fn layout_is_bounded_at_every_size(width in 0u16..160, height in 0u16..80, x in 0u16..10, y in 0u16..10, count in 0usize..100) {
        let rows = vec![Line::from("row"); count];
        let screen = Screen { transcript: &rows, terminal: &rows, diff: &rows, goal: &rows, plans: &rows, notifications: &rows, ..Screen::default() };
        let mut input = PromptInput::new(Editor::new(&format!("{}@row", "line\n".repeat(count))));
        input.completion.set_file("workspace/row", Some(b"source")).unwrap();
        input.refresh_completions();
        let outer = Rect::new(x, y, width, height);
        let areas = screen.areas(outer, &input);
        let scroll = screen.tail_scroll(outer, &input);
        prop_assert_eq!(scroll, count.saturating_sub(usize::from(areas.transcript.height)));
        let primary = [areas.header, areas.transcript, areas.side, areas.terminal, areas.notifications, areas.prompt, areas.status];
        for area in primary.into_iter().chain([areas.completions]) {
            prop_assert!(area.x >= outer.x && area.y >= outer.y);
            prop_assert!(area.right() <= outer.right() && area.bottom() <= outer.bottom());
        }
        for (i, a) in primary.iter().enumerate() {
            for b in primary.iter().skip(i + 1) {
                prop_assert!(a.intersection(*b).is_empty());
            }
        }
        prop_assert!(areas.prompt.height <= height / 2);
    }
}

#[test]
fn mouse_hit_testing_uses_the_composed_prompt_area_and_copies_a_whole_grapheme()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_harness_tui::ratatui::style::Modifier;
    use acyclic_harness_tui::{
        InputEffect, PromptBox,
        key_event::{
            KeyCode, KeyEvent, KeyModifiers as M, MouseButton, MouseEvent, MouseEventKind,
        },
    };
    let screen = Screen::default();
    let mut input = PromptInput::new(Editor::new("abc界d"));
    let mut viewport = Viewport::default();
    let mut terminal = Terminal::new(TestBackend::new(24, 10))?;
    let mut area = Rect::default();
    terminal.draw(|frame| {
        area = PromptBox::input_area(screen.render(frame, &input, &mut viewport, 0, 1000).prompt);
    })?;
    input.mouse(
        MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: area.x + 3,
            row: area.y,
            modifiers: M::NONE,
        },
        area,
        &mut viewport,
        1000,
    );
    input.key(
        KeyEvent::new(KeyCode::Right, M::SHIFT),
        1001,
        usize::from(area.width),
    );
    assert_eq!(
        input.key(
            KeyEvent::new(KeyCode::Char('c'), M::CONTROL),
            1002,
            usize::from(area.width)
        ),
        vec![InputEffect::Copy("界".into())]
    );
    terminal.draw(|frame| {
        screen.render(frame, &input, &mut viewport, 0, 1003);
    })?;
    assert_eq!(
        terminal.backend().buffer()[(area.x + 3, area.y)].symbol(),
        "界"
    );
    assert!(
        terminal.backend().buffer()[(area.x + 3, area.y)]
            .modifier
            .contains(Modifier::REVERSED)
    );
    Ok(())
}

#[test]
fn multiline_prompt_growth_does_not_capture_transcript_page_keys()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_harness_tui::{
        Action, InputEffect,
        key_event::{KeyCode, KeyEvent, KeyModifiers as M},
    };
    let rows: Vec<_> = (0..20)
        .map(|index| Line::from(format!("message {index}")))
        .collect();
    let screen = Screen {
        transcript: &rows,
        ..Screen::default()
    };
    let text = (0..10)
        .map(|index| format!("line {index}"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut input = PromptInput::new(Editor::new(&text));
    let mut viewport = Viewport::default();
    let mut terminal = Terminal::new(TestBackend::new(30, 10))?;
    terminal.draw(|frame| {
        let areas = screen.render(frame, &input, &mut viewport, 0, 1000);
        assert_eq!(areas.prompt.height, 5);
    })?;
    let input_scroll = viewport.scroll;
    assert!(input_scroll > 0);
    assert_eq!(
        input.key(KeyEvent::new(KeyCode::PageUp, M::NONE), 1001, 28),
        vec![InputEffect::Host(Action::TranscriptUp)]
    );
    terminal.draw(|frame| {
        screen.render(frame, &input, &mut viewport, 4, 1002);
    })?;
    assert_eq!(viewport.scroll, input_scroll);
    assert_eq!(input.editor.text(), text);
    assert!(visible(&terminal).contains("message 4"));
    assert!(!visible(&terminal).contains("message 0"));
    Ok(())
}

#[test]
fn large_paste_is_compact_in_layout_and_expands_in_queued_submission()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_harness_tui::{
        InputEffect,
        key_event::{KeyCode, KeyEvent, KeyModifiers as M},
    };
    let pasted = (0..12)
        .map(|index| format!("pasted line {index}"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut input = PromptInput::default();
    input.busy = true;
    input.paste(&pasted, 1000);
    let mut terminal = Terminal::new(TestBackend::new(40, 12))?;
    let mut viewport = Viewport::default();
    terminal.draw(|frame| {
        Screen::default().render(frame, &input, &mut viewport, 0, 1001);
    })?;
    assert!(visible(&terminal).contains("[Pasted 12 lines]"));
    assert_eq!(
        input.key(KeyEvent::new(KeyCode::Enter, M::NONE), 1002, 38),
        vec![InputEffect::Submit {
            text: pasted,
            queued: true,
            edit: None
        }]
    );
    Ok(())
}

#[test]
fn composed_prompt_restores_each_item_draft_and_shared_history_from_files()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_harness_tui::{
        key_event::{KeyCode, KeyEvent, KeyModifiers as M},
        load_draft, load_history, save_draft, save_history,
    };
    let directory = tempfile::tempdir()?;
    let first = directory.path().join("first-draft");
    let second = directory.path().join("second-draft");
    let history = directory.path().join("history.json");
    save_draft(&first, &Editor::new("first item draft"))?;
    save_draft(&second, &Editor::new("second item draft"))?;
    let mut input = PromptInput::new(Editor::new("earlier prompt"));
    input.key(KeyEvent::new(KeyCode::Enter, M::NONE), 1, 38);
    save_history(&history, input.history())?;
    let mut terminal = Terminal::new(TestBackend::new(40, 12))?;
    let mut viewport = Viewport::default();
    for (path, expected) in [(&first, "first item draft"), (&second, "second item draft")] {
        let input = PromptInput::new(load_draft(path)?);
        terminal.draw(|frame| {
            Screen::default().render(frame, &input, &mut viewport, 0, 1000);
        })?;
        assert!(visible(&terminal).contains(expected));
    }
    let mut restored = PromptInput::default();
    restored.replace_history(load_history(&history)?);
    restored.key(KeyEvent::new(KeyCode::Up, M::NONE), 1001, 38);
    terminal.draw(|frame| {
        Screen::default().render(frame, &restored, &mut viewport, 0, 1002);
    })?;
    assert!(visible(&terminal).contains("earlier prompt"));
    Ok(())
}

#[path = "screen/display_text.rs"]
mod display_text;

#[test]
fn feedback_owns_the_viewport_while_a_task_is_busy() -> Result<(), Box<dyn std::error::Error>> {
    let transcript = vec![Line::from("retained transcript"); 40];
    let screen = Screen {
        transcript: &transcript,
        feedback: true,
        ..Screen::default()
    };
    let mut input = PromptInput::default();
    input.busy = true;
    let mut terminal = Terminal::new(TestBackend::new(80, 24))?;
    terminal.draw(|frame| {
        assert_eq!(screen.tail_scroll(frame.area(), &input), 0);
        screen.render(frame, &input, &mut Viewport::default(), 0, 1000);
    })?;
    let text = visible(&terminal);
    assert!(text.contains("Feedback"));
    assert!(!text.contains("retained transcript"));
    assert!(!text.contains("Thinking…"));
    Ok(())
}
