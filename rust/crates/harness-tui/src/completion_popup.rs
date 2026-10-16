use crate::{CompletionMenu, Editor};
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Modifier, Style},
    widgets::{Block, Borders, Clear, Widget},
};

/// Command and file-completion popup. Geometry is capped at 60 columns and 12
/// rows and text is clipped by display width, including on narrow terminals.
pub struct CompletionPopup<'a> {
    /// Current file-query results and selected row.
    pub menu: &'a CompletionMenu,
    /// Editor used to reject stale popup contents.
    pub editor: &'a Editor,
    /// Theme text and border style.
    pub style: Style,
    /// Theme selected-row style.
    pub selected_style: Style,
}

impl<'a> CompletionPopup<'a> {
    /// Construct a popup without fixed theme colors.
    pub fn new(menu: &'a CompletionMenu, editor: &'a Editor) -> Self {
        Self {
            menu,
            editor,
            style: Style::default(),
            selected_style: Style::default().add_modifier(Modifier::REVERSED),
        }
    }

    /// Prefer the area above the prompt, falling back below when it has more room.
    pub fn area(&self, prompt: Rect, screen: Rect) -> Rect {
        let width = screen
            .width
            .min(60)
            .min(screen.right().saturating_sub(screen.x));
        let wanted = self.menu.items().len().saturating_add(2).min(12) as u16;
        let top = prompt.y.clamp(screen.y, screen.bottom());
        let bottom = prompt.bottom().clamp(screen.y, screen.bottom());
        let above = top - screen.y;
        let below = screen.bottom() - bottom;
        let use_above = above >= wanted || above >= below;
        let height = wanted.min(if use_above { above } else { below });
        if height < 3 || width < 3 {
            return Rect::new(screen.x, screen.y, 0, 0);
        }
        let x = prompt
            .x
            .clamp(screen.x, screen.right().saturating_sub(width));
        let y = if use_above {
            top.saturating_sub(height)
        } else {
            bottom
        };
        Rect::new(x, y, width, height)
    }
}

impl Widget for CompletionPopup<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        let area = Rect::new(area.x, area.y, area.width.min(60), area.height.min(12));
        if area.width < 3 || area.height < 3 || !self.menu.active(self.editor) {
            return;
        }
        Clear.render(area, buffer);
        let block = Block::default()
            .borders(Borders::ALL)
            .title("Completions")
            .style(self.style);
        let inner = block.inner(area);
        block.render(area, buffer);
        let first = self
            .menu
            .selected
            .saturating_sub(usize::from(inner.height) - 1);
        for (index, item) in self
            .menu
            .items()
            .enumerate()
            .skip(first)
            .take(usize::from(inner.height))
        {
            let text = format!(
                "{}{}  {}",
                item.label,
                if item.tool { " [tool]" } else { "" },
                item.description
            );
            let text: String = text
                .chars()
                .map(|c| if c.is_control() { '�' } else { c })
                .collect();
            let style = if index == self.menu.selected {
                self.style.patch(self.selected_style)
            } else {
                self.style
            };
            buffer.set_stringn(
                inner.x,
                inner.y + (index - first) as u16,
                text,
                usize::from(inner.width),
                style,
            );
        }
    }
}
