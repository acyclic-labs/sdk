use crate::{CompletionMenu, Editor, buffer::slice, widget::cells};
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Style,
    widgets::{Block, Borders, Clear, Widget},
};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

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
            style: crate::config::default_style(crate::config::ThemeRole::Popup),
            selected_style: crate::config::default_style(crate::config::ThemeRole::PopupSelected),
        }
    }

    /// Apply followed semantic roles without allocating per frame.
    pub fn theme(mut self, theme: &crate::config::ThemeStyles) -> Self {
        self.style = theme.style(crate::config::ThemeRole::Popup);
        self.selected_style = theme.style(crate::config::ThemeRole::PopupSelected);
        self
    }

    /// Prefer the area above the prompt, falling back below when it has more room.
    pub fn area(&self, prompt: Rect, screen: Rect) -> Rect {
        let width = screen
            .width
            .min(cells(self.editor.settings().completion_max_width))
            .min(screen.right().saturating_sub(screen.x));
        let wanted = cells(
            self.menu
                .items()
                .len()
                .saturating_add(2)
                .min(self.editor.settings().completion_max_height),
        );
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
        let area = Rect::new(
            area.x,
            area.y,
            area.width
                .min(cells(self.editor.settings().completion_max_width)),
            area.height
                .min(cells(self.editor.settings().completion_max_height)),
        );
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
            let style = if index == self.menu.selected {
                self.style.patch(self.selected_style)
            } else {
                self.style
            };
            let mut x = inner.x;
            for part in [
                item.label.as_str(),
                if item.tool { " [tool]" } else { "" },
                "  ",
                item.description.as_str(),
            ] {
                render_part(
                    part,
                    &mut x,
                    inner.right(),
                    inner.y + cells(index - first),
                    style,
                    buffer,
                );
            }
        }
    }
}

// Borrow only a cell-bounded prefix. A Unicode grapheme can contain arbitrarily
// many zero-width scalars: cap inspection at 64 UTF-8 bytes per available cell,
// replacing a possibly incomplete final cluster rather than rendering half of it.
pub(crate) fn render_part(
    text: &str,
    x: &mut u16,
    right: u16,
    y: u16,
    style: Style,
    buffer: &mut Buffer,
) {
    if *x >= right {
        return;
    }
    let mut end = text.len().min(usize::from(right - *x) * 64);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    let prefix = slice(text, ..end);
    for (at, grapheme) in prefix.grapheme_indices(true) {
        if *x >= right {
            break;
        }
        if end < text.len() && at + grapheme.len() == end {
            render_symbol("�", x, right, y, style, buffer);
        } else if grapheme.chars().any(char::is_control) {
            for character in grapheme.chars() {
                let mut utf8 = [0; 4];
                let symbol = if character.is_control() {
                    "�"
                } else {
                    character.encode_utf8(&mut utf8)
                };
                render_symbol(symbol, x, right, y, style, buffer);
            }
        } else {
            render_symbol(grapheme, x, right, y, style, buffer);
        }
    }
}

fn render_symbol(symbol: &str, x: &mut u16, right: u16, y: u16, style: Style, buffer: &mut Buffer) {
    let width = symbol.width();
    if width == 0 || *x >= right {
        return;
    }
    if width > usize::from(right - *x) {
        *x = right;
        return;
    }
    buffer.set_stringn(*x, y, symbol, width, style);
    *x += cells(width);
}

#[cfg(test)]
mod tests;
