use crate::config::{ThemeRole, ThemeStyles};
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    text::Line,
    widgets::{Block, Borders, Paragraph, Widget, Wrap},
};

/// A local report form; the prompt and its file-write binding own the input.
pub struct Feedback<'a>(pub &'a ThemeStyles);

impl Widget for Feedback<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        Paragraph::new(vec![
            Line::from("Type your report in the prompt below."),
            Line::from("Enter saves locally in this session."),
            Line::from("Escape cancels."),
        ])
        .style(self.0.style(ThemeRole::Text))
        .wrap(Wrap { trim: false })
        .block(
            Block::default()
                .title("Feedback")
                .borders(Borders::ALL)
                .border_style(self.0.style(ThemeRole::Border)),
        )
        .render(area, buffer);
    }
}
