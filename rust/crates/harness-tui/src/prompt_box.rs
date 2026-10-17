use crate::{PromptInput, PromptWidget, Viewport};
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Style,
    widgets::{Block, Borders, StatefulWidget, Widget},
};

/// Input chrome with editing, recording and attachment status. All colors can
/// be supplied by the host theme; no application state is cloned per frame.
pub struct PromptBox<'a> {
    /// Borrowed input controller.
    pub input: &'a PromptInput,
    /// Contextual empty-input hint.
    pub placeholder: &'a str,
    /// Normal border/text style.
    pub style: Style,
    /// Editing-message border style.
    pub editing_style: Style,
    /// Current animation time in monotonic milliseconds.
    pub now_ms: u64,
}

impl<'a> PromptBox<'a> {
    /// Build a prompt box with host-overridable semantic colors.
    pub fn new(input: &'a PromptInput, now_ms: u64) -> Self {
        Self {
            input,
            placeholder: "",
            style: input.theme.style(crate::config::ThemeRole::Text),
            editing_style: input.theme.style(crate::config::ThemeRole::Editing),
            now_ms,
        }
    }

    /// Grow to at most half the screen, including border and status title.
    pub fn height(&self, width: u16, screen_height: u16) -> u16 {
        let maximum = (u32::from(screen_height)
            * self.input.editor.settings().max_input_height_percent as u32
            / 100) as u16;
        PromptWidget::new(&self.input.editor)
            .height(width.saturating_sub(2), maximum.saturating_sub(2))
            .saturating_add(2)
            .min(maximum)
    }

    /// Inner area used for shared mouse hit testing.
    pub fn input_area(area: Rect) -> Rect {
        Block::default().borders(Borders::ALL).inner(area)
    }
}

impl StatefulWidget for PromptBox<'_> {
    type State = Viewport;

    fn render(self, area: Rect, buffer: &mut Buffer, viewport: &mut Viewport) {
        let mut labels = vec![];
        if self.input.transcript_search {
            labels.push("Find · Enter next · Shift+Enter previous · Escape closes".to_owned());
        }
        if self.input.editing_message().is_some() {
            labels.push("Editing message…".to_owned());
        }
        if let Some((query, matched)) = self.input.search_status() {
            let query: String = query
                .chars()
                .map(|c| if c.is_control() { '�' } else { c })
                .collect();
            labels.push(format!(
                "{}reverse search: {query}",
                if matched { "" } else { "failing " }
            ));
        }
        if self.input.images > 0 {
            labels.push(format!("{} images attached", self.input.images));
        }
        if self.input.recording {
            labels.push("● REC".to_owned());
        }
        if self.now_ms.saturating_sub(self.input.last_activity())
            < self.input.editor.settings().activity_ms as u64
        {
            labels.push("●".to_owned());
        }
        let style = if self.input.editing_message().is_some() {
            self.style.patch(self.editing_style)
        } else {
            self.style
                .patch(self.input.theme.style(crate::config::ThemeRole::Border))
        };
        let block = Block::default()
            .style(self.style)
            .borders(Borders::ALL)
            .border_style(style)
            .title(labels.join(" · "));
        let inner = block.inner(area);
        block.render(area, buffer);
        let mut widget = PromptWidget::new(&self.input.editor);
        widget.placeholder = self.placeholder;
        widget.style = self.style;
        widget.selection_style = self.input.theme.style(crate::config::ThemeRole::Selection);
        widget.placeholder_style = self
            .input
            .theme
            .style(crate::config::ThemeRole::Placeholder);
        widget.render(inner, buffer, viewport);
    }
}
