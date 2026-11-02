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
        let mut rows = Vec::new();
        let result = visit_rows(text, folds, tab_width, width, visit_fold, |row| {
            rows.push(row);
            Ok::<_, std::convert::Infallible>(())
        });
        match result {
            Ok(()) => Self {
                rows,
                width: width.max(1),
            },
            Err(never) => match never {},
        }
    }

    /// Visit wrapped plain-text rows while retaining only the current row.
    /// Uses the same grapheme, control, tab and display-width rules as the editor.
    ///
    /// # Errors
    /// Stops immediately on the visitor's error.
    pub fn visit_text_rows<E>(
        text: &str,
        width: usize,
        visit: impl FnMut(Row) -> Result<(), E>,
    ) -> Result<(), E> {
        visit_rows(
            text,
            &[],
            crate::DEFAULT_SETTINGS.tab_width,
            width,
            &mut || {},
            visit,
        )
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

fn visit_rows<E>(
    text: &str,
    folds: &[Range<usize>],
    tab_width: usize,
    width: usize,
    visit_fold: &mut impl FnMut(),
    mut visit: impl FnMut(Row) -> Result<(), E>,
) -> Result<(), E> {
    let width = width.max(1);
    let mut row = Row::default();
    let mut column = 0;
    let mut offset = 0;
    let mut folds = folds.iter().peekable();
    while offset < text.len() {
        let fold = folds.next_if(|range| {
            visit_fold();
            range.start == offset
        });
        let (end, symbol) = if let Some(range) = fold {
            (
                range.end,
                Cow::Owned(format!(
                    "[Pasted {} lines]",
                    slice(text, range.clone()).split('\n').count()
                )),
            )
        } else if let Some(grapheme) = slice(text, offset..).graphemes(true).next() {
            (offset + grapheme.len(), Cow::Borrowed(grapheme))
        } else {
            break;
        };
        if symbol == "\n" {
            row.end = offset;
            visit(row)?;
            if column == width {
                visit(Row {
                    start: offset,
                    end: offset,
                    cells: vec![],
                })?;
            }
            row = Row {
                start: end,
                end,
                cells: vec![],
            };
            column = 0;
        } else {
            let symbol = if symbol == "\t" {
                Cow::Owned(" ".repeat((tab_width - column % tab_width).min(width)))
            } else if symbol.chars().any(char::is_control) {
                Cow::Borrowed("�")
            } else {
                symbol
            };
            let mut cell_width = UnicodeWidthStr::width(symbol.as_ref());
            let symbol = if cell_width > width {
                cell_width = 1;
                Cow::Borrowed("�")
            } else if cell_width == 0 {
                cell_width = 1;
                // A leading combining mark needs a printable base.
                Cow::Owned(format!("◌{symbol}"))
            } else {
                symbol
            };
            if column + cell_width > width {
                row.end = offset;
                visit(row)?;
                row = Row {
                    start: offset,
                    end: offset,
                    cells: vec![],
                };
                column = 0;
            }
            row.cells.push(Cell {
                source: offset..end,
                symbol: symbol.into_owned(),
                column,
                width: cell_width,
            });
            column += cell_width;
            row.end = end;
        }
        offset = end;
    }
    visit(row)?;
    // A full final row needs a following cell for the insertion cursor.
    if column == width {
        visit(Row {
            start: offset,
            end: offset,
            cells: vec![],
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_substitutions_allocate_only_the_final_cell_text() {
        for (source, displayed) in [
            ("\t", " "),
            ("\x07", "�"),
            ("界", "�"),
            ("\u{301}", "◌\u{301}"),
        ] {
            let (mut original, mut substituted) = (None, None);
            let expected = acyclic_test_support::measure(|| {
                original = Some(Layout::from_text(displayed, 1));
            });
            let actual = acyclic_test_support::measure(|| {
                substituted = Some(Layout::from_text(source, 1));
            });
            let original = original.unwrap();
            let substituted = substituted.unwrap();
            assert_eq!(original.rows.len(), substituted.rows.len());
            assert_eq!(
                original.rows[0].cells[0].symbol,
                substituted.rows[0].cells[0].symbol
            );
            assert_eq!(substituted.rows[0].cells[0].source, 0..source.len());
            // Reinstating a source String before substitution adds an allocation.
            assert_eq!(actual.count_total, expected.count_total, "{source:?}");
        }
    }

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
