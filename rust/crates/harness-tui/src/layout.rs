use std::{borrow::Cow, ops::Range};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

use crate::{
    DEFAULT_SETTINGS, Editor,
    buffer::{normalize, slice},
};

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
        Self::build(
            editor.text(),
            editor.collapsed_pastes(),
            editor.settings().tab_width,
            width,
            &mut || {},
        )
    }

    /// Wrap read-only text with the default tab width and no collapsed pastes.
    /// Carriage returns normalize as in an editor; source offsets refer to that
    /// normalized text. Other input is borrowed without building editor state.
    pub fn from_text(text: &str, width: usize) -> Self {
        let text = if text.contains('\r') {
            Cow::Owned(normalize(text))
        } else {
            Cow::Borrowed(text)
        };
        Self::build(&text, &[], DEFAULT_SETTINGS.tab_width, width, &mut || {})
    }

    fn build(
        text: &str,
        folds: &[Range<usize>],
        tab_width: usize,
        width: usize,
        visit_fold: &mut impl FnMut(),
    ) -> Self {
        let width = width.max(1);
        let mut rows = vec![];
        let mut row = Row::default();
        let mut column = 0;
        let mut offset = 0;
        // Buffer keeps disjoint folds in source order. Layout walks that same
        // order, so only the next fold can match the current source boundary.
        let mut folds = folds.iter().peekable();
        while offset < text.len() {
            let fold = folds.next_if(|range| {
                visit_fold();
                range.start == offset
            });
            let (end, symbol) = if let Some(range) = fold {
                (
                    range.end,
                    format!(
                        "[Pasted {} lines]",
                        slice(text, range.clone()).split('\n').count()
                    ),
                )
            } else if let Some(grapheme) = slice(text, offset..).graphemes(true).next() {
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
                    " ".repeat((tab_width - column % tab_width).min(width))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_layout_preserves_editor_projection_without_editor_allocations() {
        for text in [
            "",
            "plain text",
            "界e\u{301}👩‍💻\ttext\nlast",
            "a\r\nb\rc",
            "\u{301}\x07\n",
        ] {
            for width in [0, 1, 7, 32] {
                let (_, [before, after]) = acyclic_test_support::differential(
                    || Layout::new(&Editor::new(text), width),
                    || Layout::from_text(text, width),
                );
                // Restoring the temporary editor fails this same cost witness.
                assert!(
                    after.count_total < before.count_total,
                    "{text:?}, width {width}: {before:?} -> {after:?}"
                );
                assert!(
                    after.bytes_total < before.bytes_total,
                    "{text:?}, width {width}: {before:?} -> {after:?}"
                );
            }
        }
    }

    #[test]
    fn folded_layout_visits_only_the_next_paste_and_preserves_source_hits() {
        for count in [1, 8, 64] {
            let mut editor = Editor::default();
            let mut settings = *editor.settings();
            settings.paste_collapse_bytes = 4;
            assert!(editor.set_settings(settings));
            for index in 0..count {
                editor.insert("界e\u{301}\t", index);
                editor.paste("one\n👩‍💻", index);
                editor.insert(" tail\n", index);
            }
            assert_eq!(editor.collapsed_pastes().len(), count as usize);
            // Undo/redo restores the original ordered fold metadata as well as
            // text; layout must use the restored source, not a private index.
            let source = editor.text().to_owned();
            assert!(editor.undo());
            assert!(editor.redo());
            assert_eq!(editor.text(), source);

            for width in [1, 7, 32] {
                let mut checks = 0;
                let layout = Layout::build(
                    editor.text(),
                    editor.collapsed_pastes(),
                    editor.settings().tab_width,
                    width,
                    &mut || checks += 1,
                );
                let cells = layout.rows.iter().flat_map(|row| &row.cells);
                let mut folded = 0;
                for cell in cells {
                    assert!(cell.column + cell.width <= width);
                    if editor.collapsed_pastes().contains(&cell.source) {
                        folded += 1;
                        assert_eq!(
                            cell.symbol,
                            if width < "[Pasted 2 lines]".len() {
                                "�"
                            } else {
                                "[Pasted 2 lines]"
                            }
                        );
                        let (row, column) = layout.position(cell.source.start);
                        assert_eq!(layout.hit_test(row, column), cell.source.start);
                        assert_eq!(layout.position(cell.source.end - 1), (row, column));
                    }
                }
                assert_eq!(folded, count as usize);
                // Each gap has nine graphemes and each paste is one atom.
                // Restarting a search over all folds exceeds this linear bound.
                assert!(checks <= 10 * count as usize, "{checks} fold checks");
            }
        }
    }
}
