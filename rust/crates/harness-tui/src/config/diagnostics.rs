use super::{ConfigView, ThemeRole};
use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};

/// Current typed diagnostics, rendered as bounded, sanitized terminal cells.
pub struct ConfigDiagnostics<'a> {
    /// Current file projection.
    pub view: &'a ConfigView,
    /// Resolved diagnostic style.
    pub style: ratatui::style::Style,
}
impl<'a> ConfigDiagnostics<'a> {
    /// Bind diagnostics to the followed input theme.
    pub fn new(view: &'a ConfigView, input: &crate::PromptInput) -> Self {
        Self {
            view,
            style: input.theme.style(ThemeRole::Diagnostic),
        }
    }
}
impl Widget for ConfigDiagnostics<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        let style = self.style;
        for (row, diagnostic) in self
            .view
            .diagnostics()
            .take(usize::from(area.height))
            .enumerate()
        {
            let mut x = area.x;
            for text in [
                diagnostic.file.path(),
                " ",
                diagnostic.path.as_str(),
                ": ",
                diagnostic.detail.as_str(),
            ] {
                crate::completion_popup::render_part(
                    text,
                    &mut x,
                    area.right(),
                    area.y + row as u16,
                    style,
                    buffer,
                );
            }
        }
    }
}
