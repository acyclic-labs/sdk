use super::{GoalFileView, GoalQueryView};
use crate::goal::GoalStatus;
use acyclic_harness_tui::ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Style},
    widgets::{Block, Widget},
};

/// Goal presentation borrowing only the shared ClientViews fold.
pub struct GoalPanel<'a> {
    /// Status and count at their independent filesystem publication cuts.
    pub view: GoalQueryView<'a>,
}

impl Widget for GoalPanel<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        self.render_counted(area, buffer);
    }
}

impl GoalPanel<'_> {
    // Counts source scalars actually traversed. Publication-time line offsets
    // let redraw skip clipped tails without searching for their newlines.
    fn render_counted(self, area: Rect, buffer: &mut Buffer) -> usize {
        let block = Block::bordered().title("Goal");
        let body = block.inner(area);
        block.render(area, buffer);
        if body.is_empty() {
            return 0;
        }
        let (status, reason, color) = match self.view.goal {
            GoalFileView::Missing => ("missing", None, Color::DarkGray),
            GoalFileView::Invalid { reason } => ("invalid", Some(reason.as_str()), Color::Red),
            GoalFileView::Present { goal } => (
                match goal.status {
                    GoalStatus::Active => "active",
                    GoalStatus::Blocked => "blocked",
                    GoalStatus::Done => "done",
                },
                goal.reason.as_deref(),
                match goal.status {
                    GoalStatus::Active => Color::Yellow,
                    GoalStatus::Blocked => Color::Red,
                    GoalStatus::Done => Color::Green,
                },
            ),
        };
        let count = self.view.progress.map_or_else(
            || {
                if status == "blocked" {
                    "pending"
                } else {
                    "unknown"
                }
                .into()
            },
            |progress| progress.continuations.to_string(),
        );
        let title = format!("Status: {status} | Continuations: {count}");
        let style = Style::default().fg(color);
        let width = usize::from(body.width);
        let mut work = 0;
        let mut y = body.y;
        for text in [
            Some(("", title.as_str())),
            reason.map(|text| ("Reason: ", text)),
        ]
        .into_iter()
        .flatten()
        {
            if y == body.bottom() {
                return work;
            }
            let safe = escaped(text.0.chars().chain(text.1.chars()).take(width), &mut work);
            buffer.set_stringn(body.x, y, safe, width, style);
            y += 1;
        }
        for (y, line) in (y..body.bottom()).zip(self.view.lines()) {
            let safe = escaped(line.chars().take(width), &mut work);
            buffer.set_stringn(body.x, y, safe, width, style);
        }
        work
    }
}

fn escaped(chars: impl Iterator<Item = char>, work: &mut usize) -> String {
    let mut text = String::new();
    for ch in chars {
        *work += 1;
        escape(ch, &mut text);
    }
    text
}

fn escape(ch: char, text: &mut String) {
    if ch.is_control() {
        text.extend(ch.escape_debug());
    } else {
        text.push(ch);
    }
}

#[cfg(test)]
mod tests;
