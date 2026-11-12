use crate::config::{ThemeRole, ThemeStyles};
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    text::Line,
    widgets::{Block, Borders, Paragraph, Widget},
};

/// A local report form; the prompt and its file-write binding own the input.
pub struct Feedback<'a>(pub &'a ThemeStyles, pub &'a [Line<'a>], pub usize);

impl Feedback<'_> {
    /// Original session-file text, laid out by the shared query page.
    pub fn lines(reports: &[String]) -> Vec<Line<'static>> {
        [
            Line::from("Type your report in the prompt below."),
            Line::from("Enter saves locally in this session."),
            Line::from("Escape cancels."),
        ]
        .into_iter()
        .chain(reports.iter().rev().flat_map(|report| {
            [Line::from(""), Line::from("Saved report:")]
                .into_iter()
                .chain(report.lines().map(|line| Line::from(line.to_owned())))
        }))
        .collect()
    }
}

impl Widget for Feedback<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        let lines = self
            .1
            .get(self.2..)
            .unwrap_or_default()
            .iter()
            .take(usize::from(area.height.saturating_sub(2)))
            .cloned()
            .collect::<Vec<_>>();
        Paragraph::new(lines)
            .style(self.0.style(ThemeRole::Text))
            .block(
                Block::default()
                    .title("Feedback")
                    .borders(Borders::ALL)
                    .border_style(self.0.style(ThemeRole::Border)),
            )
            .render(area, buffer);
    }
}
