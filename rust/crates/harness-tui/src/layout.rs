use std::ops::Range;
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

use crate::{Editor, buffer::slice};
#[cfg(test)]
thread_local! { pub(crate) static BUILDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

/// One displayed grapheme, expanded tab cell, or collapsed paste marker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cell {
    /// Original text byte range.
    pub source: Range<usize>,
    /// Printable grapheme (controls never reach the terminal).
    pub symbol: String,
    /// Display column.
    pub column: usize,
    /// Occupied display cells.
    pub width: usize,
}

/// A visual row, excluding the line terminator.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Row {
    /// Byte offset of the first grapheme.
    pub start: usize,
    /// Byte offset after the final grapheme.
    pub end: usize,
    /// Graphemes and their display positions.
    pub cells: Vec<Cell>,
}

/// Shared wrapping and hit testing for rendering, movement and mouse selection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Layout {
    /// Wrapped rows, always containing at least one row.
    pub rows: Vec<Row>,
    /// Effective display width (at least one).
    pub width: usize,
}

impl Default for Layout {
    fn default() -> Self {
        Self {
            rows: vec![Row::default()],
            width: 1,
        }
    }
}

impl Layout {
    /// Wrap without splitting a grapheme. Oversized glyphs become a one-cell
    /// replacement glyph; the original text and hit-test range are preserved.
    pub fn new(editor: &Editor, width: usize) -> Self {
        #[cfg(test)]
        BUILDS.with(|builds| builds.set(builds.get() + 1));
        let width = width.max(1);
        let mut rows = vec![];
        let mut row = Row::default();
        let mut column = 0;
        let mut offset = 0;
        while offset < editor.text().len() {
            let fold = editor.collapsed_pastes().iter().find(|r| r.start == offset);
            let (end, symbol) = if let Some(range) = fold {
                (
                    range.end,
                    format!(
                        "[Pasted {} lines]",
                        slice(editor.text(), range.clone()).split('\n').count()
                    ),
                )
            } else if let Some(grapheme) = slice(editor.text(), offset..).graphemes(true).next() {
                (offset + grapheme.len(), grapheme.to_owned())
            } else {
                break;
            };
            if symbol == "\n" {
                row.end = offset;
                rows.push(row);
                if column == width {
                    rows.push(Row {
                        start: offset,
                        end: offset,
                        cells: vec![],
                    });
                }
                row = Row {
                    start: end,
                    end,
                    cells: vec![],
                };
                column = 0;
            } else {
                let symbol = if symbol == "\t" {
                    " ".repeat(
                        (editor.settings().tab_width - column % editor.settings().tab_width)
                            .min(width),
                    )
                } else if symbol.chars().any(char::is_control) {
                    "�".to_owned()
                } else {
                    symbol
                };
                let mut cell_width = UnicodeWidthStr::width(symbol.as_str());
                let symbol = if cell_width > width {
                    cell_width = 1;
                    "�".to_owned()
                } else if cell_width == 0 {
                    cell_width = 1;
                    // A leading combining mark needs a printable base.
                    format!("◌{symbol}")
                } else {
                    symbol
                };
                if column + cell_width > width {
                    row.end = offset;
                    rows.push(row);
                    row = Row {
                        start: offset,
                        end: offset,
                        cells: vec![],
                    };
                    column = 0;
                }
                row.cells.push(Cell {
                    source: offset..end,
                    symbol,
                    column,
                    width: cell_width,
                });
                column += cell_width;
                row.end = end;
            }
            offset = end;
        }
        rows.push(row);
        // A full final row needs a following cell for the insertion cursor.
        if column == width {
            rows.push(Row {
                start: offset,
                end: offset,
                cells: vec![],
            });
        }
        Self { rows, width }
    }

    /// Map a byte offset to a visual row and display column. At soft wraps,
    /// the insertion cursor belongs to the following row.
    pub fn position(&self, offset: usize) -> (usize, usize) {
        let row_index = self
            .rows
            .iter()
            .rposition(|r| r.start <= offset)
            .unwrap_or(0);
        let Some(row) = self.rows.get(row_index) else {
            return (0, 0);
        };
        for cell in &row.cells {
            if offset < cell.source.end {
                return (row_index, cell.column);
            }
        }
        (
            row_index,
            row.cells.last().map_or(0, |cell| cell.column + cell.width),
        )
    }

    /// Map a display position to the nearest grapheme boundary on that row.
    pub fn hit_test(&self, row: usize, column: usize) -> usize {
        let index = row.min(self.rows.len().saturating_sub(1));
        let Some(row) = self.rows.get(index) else {
            return 0;
        };
        for cell in &row.cells {
            if column < cell.column + cell.width {
                return cell.source.start;
            }
        }
        if self
            .rows
            .get(index + 1)
            .is_some_and(|next| next.start == row.end)
        {
            row.cells.last().map_or(row.end, |cell| cell.source.start)
        } else {
            row.end
        }
    }
}
