use acyclic_harness_tui::{Editor, PromptWidget, Viewport};
use ratatui::{Terminal, backend::TestBackend, buffer::Buffer, layout::Rect};

#[test]
fn exact_width_before_newline_has_a_visible_insertion_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    let mut editor = Editor::new("abcd\nx");
    editor.set_cursor(4, false);
    let mut viewport = Viewport::default();
    let mut terminal = Terminal::new(TestBackend::new(4, 3))?;
    terminal.draw(|frame| {
        frame.render_stateful_widget(PromptWidget::new(&editor), frame.area(), &mut viewport)
    })?;
    assert_eq!(
        terminal.backend().buffer(),
        &Buffer::with_lines(["abcd", "    ", "x   "])
    );
    assert_eq!(viewport.cursor, Some((0, 1)));
    editor.set_cursor(5, false);
    terminal.draw(|frame| {
        frame.render_stateful_widget(PromptWidget::new(&editor), frame.area(), &mut viewport)
    })?;
    assert_eq!(viewport.cursor, Some((0, 2)));
    Ok(())
}

#[test]
fn unicode_snapshots_width_one_through_twelve() -> Result<(), Box<dyn std::error::Error>> {
    let editor = Editor::new("界👩‍💻e\u{301}");
    for width in 1..=12 {
        let expected: Vec<&str> = match width {
            1 => vec!["�", "�", "e\u{301}", ""],
            2 => vec!["界", "👩‍💻", "e\u{301}", ""],
            3 => vec!["界", "👩‍💻e\u{301}", "", ""],
            4 => vec!["界👩‍💻", "e\u{301}", "", ""],
            _ => vec!["界👩‍💻e\u{301}", "", "", ""],
        };
        let padded: Vec<String> = expected
            .into_iter()
            .map(|line| {
                use unicode_width::UnicodeWidthStr;
                format!("{line}{}", " ".repeat(usize::from(width) - line.width()))
            })
            .collect();
        let mut terminal = Terminal::new(TestBackend::new(width, 4))?;
        let mut viewport = Viewport::default();
        terminal.draw(|frame| {
            frame.render_stateful_widget(PromptWidget::new(&editor), frame.area(), &mut viewport)
        })?;
        assert_eq!(
            terminal.backend().buffer(),
            &Buffer::with_lines(padded),
            "width {width}"
        );
        assert!(viewport.cursor.is_some());
    }
    Ok(())
}

#[test]
fn growth_scroll_placeholder_and_selection_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let mut terminal = Terminal::new(TestBackend::new(4, 2))?;
    let mut editor = Editor::new("a\nb\nc\nd");
    let mut viewport = Viewport::default();
    terminal.draw(|frame| {
        frame.render_stateful_widget(PromptWidget::new(&editor), frame.area(), &mut viewport)
    })?;
    assert_eq!(
        terminal.backend().buffer(),
        &Buffer::with_lines(["c   ", "d   "])
    );
    assert_eq!(viewport.cursor, Some((1, 1)));
    assert_eq!(viewport.scroll, 2);
    editor.set_cursor(0, false);
    terminal.draw(|frame| {
        frame.render_stateful_widget(PromptWidget::new(&editor), frame.area(), &mut viewport)
    })?;
    assert_eq!(
        terminal.backend().buffer(),
        &Buffer::with_lines(["a   ", "b   "])
    );
    editor.select_all();
    terminal.draw(|frame| {
        frame.render_stateful_widget(PromptWidget::new(&editor), frame.area(), &mut viewport)
    })?;
    assert!(
        terminal.backend().buffer()[(0, 0)]
            .modifier
            .contains(ratatui::style::Modifier::REVERSED)
    );
    let empty = Editor::default();
    terminal.draw(|frame| {
        let mut widget = PromptWidget::new(&empty);
        widget.placeholder = "ask";
        widget.placeholder_style = ratatui::style::Style::default();
        frame.render_stateful_widget(widget, Rect::new(0, 0, 4, 2), &mut viewport);
    })?;
    assert_eq!(
        terminal.backend().buffer(),
        &Buffer::with_lines(["ask ", "    "])
    );
    Ok(())
}
