use acyclic_harness_tui::ratatui::{
    Terminal,
    backend::TestBackend,
    buffer::Buffer,
    style::{Color, Style},
};
use acyclic_harness_tui::{Editor, PromptBox, PromptInput, Viewport};

#[test]
fn editing_message_border_title_and_text_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = PromptInput::new(Editor::new("draft"));
    input.edit_message("old-message".into(), "界abc");
    let mut terminal = Terminal::new(TestBackend::new(24, 3))?;
    let mut viewport = Viewport::default();
    terminal.draw(|frame| {
        let mut widget = PromptBox::new(&input, 1000);
        widget.editing_style = Style::default();
        frame.render_stateful_widget(widget, frame.area(), &mut viewport);
    })?;
    assert_eq!(
        terminal.backend().buffer(),
        &Buffer::with_lines([
            "┌Editing message…──────┐",
            "│界abc                 │",
            "└──────────────────────┘",
        ])
    );
    terminal.draw(|frame| {
        frame.render_stateful_widget(PromptBox::new(&input, 1000), frame.area(), &mut viewport)
    })?;
    assert_eq!(terminal.backend().buffer()[(0, 0)].fg, Color::Yellow);
    assert_eq!(viewport.cursor, Some((6, 1)));
    Ok(())
}

#[test]
fn attachment_recording_activity_and_half_screen_height() -> Result<(), Box<dyn std::error::Error>>
{
    let mut input = PromptInput::default();
    input.images = 2;
    input.recording = true;
    input.committed_text("a\nb\nc\nd\ne\nf", 100);
    let widget = PromptBox::new(&input, 100);
    assert_eq!(widget.height(40, 10), 5);
    assert_eq!(widget.height(1, 1), 0);
    let mut terminal = Terminal::new(TestBackend::new(40, 5))?;
    let mut viewport = Viewport::default();
    terminal.draw(|frame| {
        frame.render_stateful_widget(PromptBox::new(&input, 100), frame.area(), &mut viewport)
    })?;
    let title: String = (0..40)
        .map(|x| terminal.backend().buffer()[(x, 0)].symbol())
        .collect();
    assert!(title.contains("2 images attached · ● REC · ●"));
    assert_eq!(viewport.scroll, 3);
    assert_eq!(viewport.cursor, Some((2, 3)));
    // Titles truncate safely at every narrow width, including zero inner area.
    for width in 1..=12 {
        let mut narrow = Terminal::new(TestBackend::new(width, 3))?;
        narrow.draw(|frame| {
            frame.render_stateful_widget(PromptBox::new(&input, 100), frame.area(), &mut viewport)
        })?;
    }
    Ok(())
}
