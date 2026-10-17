use crate::Editor;
use ratatui::{buffer::Buffer, layout::Rect, style::Style, widgets::StatefulWidget};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

/// Rendering state, including the insertion cursor for host cursor/fade hooks.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Viewport {
    /// First displayed visual row.
    pub scroll: usize,
    /// Horizontal cell offset when wrapping is disabled by configuration.
    pub horizontal: usize,
    /// Absolute terminal cursor cell, if visible.
    pub cursor: Option<(u16, u16)>,
}

/// Ratatui adapter; the editor itself has no terminal dependency.
pub struct PromptWidget<'a> {
    /// Prompt state to display.
    pub editor: &'a Editor,
    /// Empty-buffer hint.
    pub placeholder: &'a str,
    /// Text style.
    pub style: Style,
    /// Selected text style.
    pub selection_style: Style,
    /// Empty-buffer hint style.
    pub placeholder_style: Style,
}

impl<'a> PromptWidget<'a> {
    /// Construct a widget; hosts decide area and maximum height.
    pub fn new(editor: &'a Editor) -> Self {
        Self {
            editor,
            placeholder: "",
            style: Style::default(),
            selection_style: crate::config::default_style(crate::config::ThemeRole::Selection),
            placeholder_style: crate::config::default_style(crate::config::ThemeRole::Placeholder),
        }
    }

    /// Height requested by the input, bounded by the host's maximum height.
    pub fn height(&self, width: u16, maximum: u16) -> u16 {
        self.editor
            .layout(usize::from(width))
            .rows
            .len()
            .min(usize::from(maximum)) as u16
    }
}

impl StatefulWidget for PromptWidget<'_> {
    type State = Viewport;

    fn render(self, area: Rect, buffer: &mut Buffer, state: &mut Viewport) {
        state.cursor = None;
        if area.width == 0 || area.height == 0 {
            return;
        }
        buffer.set_style(area, self.style);
        let layout = self.editor.layout(usize::from(area.width));
        let (row, column) = layout.position(self.editor.cursor());
        let height = usize::from(area.height);
        state.horizontal = if self.editor.settings().wrap {
            0
        } else {
            state
                .horizontal
                .min(column)
                .max(column.saturating_sub(usize::from(area.width) - 1))
        };
        state.scroll = state.scroll.min(row).max(row.saturating_sub(height - 1));
        state.scroll = state.scroll.min(layout.rows.len().saturating_sub(height));
        let selection = self.editor.selection();
        for (y, row) in layout
            .rows
            .iter()
            .skip(state.scroll)
            .take(height)
            .enumerate()
        {
            for cell in &row.cells {
                let style =
                    if cell.source.start < selection.end && cell.source.end > selection.start {
                        self.style.patch(self.selection_style)
                    } else {
                        self.style
                    };
                let right = state.horizontal + usize::from(area.width);
                if cell.column + cell.width <= state.horizontal || cell.column >= right {
                    continue;
                }
                // A collapsed paste marker is several graphemes in one source cell.
                // Clip its visible graphemes without dropping the whole marker.
                let mut column = cell.column;
                for symbol in cell.symbol.graphemes(true) {
                    let width = UnicodeWidthStr::width(symbol);
                    if column >= state.horizontal && column + width <= right {
                        buffer.set_string(
                            area.x + (column - state.horizontal) as u16,
                            area.y + y as u16,
                            symbol,
                            style,
                        );
                    }
                    column += width;
                    if column >= right {
                        break;
                    }
                }
            }
        }
        if self.editor.text().is_empty() {
            buffer.set_stringn(
                area.x,
                area.y,
                self.placeholder,
                usize::from(area.width),
                self.placeholder_style,
            );
        }
        if row >= state.scroll
            && row < state.scroll + height
            && column.saturating_sub(state.horizontal) < usize::from(area.width)
        {
            state.cursor = Some((
                area.x + (column - state.horizontal) as u16,
                area.y + (row - state.scroll) as u16,
            ));
        }
    }
}
