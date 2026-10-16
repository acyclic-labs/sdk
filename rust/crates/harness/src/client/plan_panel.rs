//! Read-only panel over the accepted private plans/ query.

use super::FileQueryView;
use acyclic_harness_tui::ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier, Style},
    widgets::{Block, Widget},
};

/// A plan panel owns no plan state; its rows come from the shared file-query fold.
pub struct PlanPanel<'a> {
    /// Current private-volume query view.
    pub files: &'a FileQueryView,
}

impl Widget for PlanPanel<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        let block = Block::bordered().title("Plans");
        let body = block.inner(area);
        block.render(area, buffer);
        for (y, line) in (body.y..body.bottom()).zip(self.files.plan_rows()) {
            let style = if line.starts_with("plans/") {
                Style::default().add_modifier(Modifier::BOLD)
            } else if line.starts_with("[~]") {
                Style::default().fg(Color::Yellow)
            } else if line.starts_with("[x]") {
                Style::default().fg(Color::Green)
            } else {
                Style::default()
            };
            buffer.set_stringn(body.x, y, line, usize::from(body.width), style);
        }
    }
}
