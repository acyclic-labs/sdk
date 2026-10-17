use crate::config::{ThemeRole, ThemeStyles};
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Modifier,
    text::{Line, Span},
    widgets::Widget,
};

/// Ephemeral animation over the followed task's busy state.
pub struct Thinking<'a> {
    theme: &'a ThemeStyles,
    now_ms: u64,
    reduced_motion: bool,
}

impl<'a> Thinking<'a> {
    /// Minimum delay between animation-only frames; idle tasks need no timer.
    pub const FRAME_MS: u64 = 120;

    /// Uses the same monotonic frame clock as the prompt cursor.
    pub fn new(theme: &'a ThemeStyles, now_ms: u64) -> Self {
        Self {
            theme,
            now_ms,
            reduced_motion: false,
        }
    }

    /// Disable the moving highlight while retaining the activity label.
    pub fn reduced_motion(mut self, reduced: bool) -> Self {
        self.reduced_motion = reduced;
        self
    }

    /// A stable label with a moving themed highlight, including monochrome.
    pub fn line(&self) -> Line<'static> {
        if self.reduced_motion {
            return Line::styled("Thinking…", self.theme.style(ThemeRole::Placeholder));
        }
        let parts = ["T", "h", "i", "n", "k", "i", "n", "g", "…"];
        let phase = (self.now_ms / Self::FRAME_MS) % parts.len() as u64;
        Line::from(
            parts
                .into_iter()
                .enumerate()
                .map(|(index, part)| {
                    let style = if index as u64 == phase {
                        self.theme
                            .style(ThemeRole::Heading)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        self.theme.style(ThemeRole::Placeholder)
                    };
                    Span::styled(part, style)
                })
                .collect::<Vec<_>>(),
        )
    }
}

impl Widget for Thinking<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        self.line().render(area, buffer);
    }
}
