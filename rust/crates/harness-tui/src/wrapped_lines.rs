use crate::{Editor, Layout};
use ratatui::{
    layout::Alignment,
    style::{Modifier, Style},
    text::{Line, Span},
};
use std::ops::Range;

struct Source {
    offset: usize,
    layout: Layout,
    styles: Vec<(usize, Style)>,
    style: Style,
    alignment: Option<Alignment>,
}

/// One cached wrapping projection for ordinary and highlighted transcript rows.
#[derive(Default)]
pub struct WrappedLines {
    width: Option<u16>,
    starts: Vec<usize>,
    sources: Vec<Source>,
    text: String,
    ranges: Vec<Range<usize>>,
    rows: Vec<Line<'static>>,
}

impl WrappedLines {
    /// Reflow changed rows, or the whole projection after a width change.
    pub fn update(&mut self, source: &[Line<'_>], changed: usize, width: u16) {
        let changed = if self.width == Some(width) {
            changed.min(source.len()).min(self.starts.len())
        } else {
            self.width = Some(width);
            0
        };
        let row = self.starts.get(changed).copied().unwrap_or(self.rows.len());
        self.rows.truncate(row);
        self.ranges.truncate(row);
        self.text.truncate(
            self.sources
                .get(changed)
                .map_or(self.text.len(), |line| line.offset),
        );
        self.starts.truncate(changed);
        self.sources.truncate(changed);
        for line in &source[changed..] {
            self.starts.push(self.rows.len());
            let offset = self.text.len();
            let mut styles = Vec::new();
            let mut carriage_return = false;
            for span in &line.spans {
                let normalized = crate::buffer::normalize(&span.content);
                self.text
                    .push_str(if carriage_return && span.content.starts_with('\n') {
                        normalized.strip_prefix('\n').unwrap_or_default()
                    } else {
                        &normalized
                    });
                if !span.content.is_empty() {
                    carriage_return = span.content.ends_with('\r');
                }
                styles.push((self.text.len() - offset, span.style));
            }
            let layout = Layout::new(&Editor::new(&self.text[offset..]), usize::from(width));
            self.text.push('\n');
            let source = Source {
                offset,
                layout,
                styles,
                style: line.style,
                alignment: line.alignment,
            };
            for row in visible_rows(&source.layout) {
                self.ranges.push(offset + row.start..offset + row.end);
            }
            paint(&source, &[], None, Style::default(), &mut self.rows);
            self.sources.push(source);
        }
    }

    /// Normalized logical text, shared by wrapping and Unicode search.
    pub fn text(&self) -> &str {
        &self.text[..self.text.len().saturating_sub(1)]
    }

    /// Repaint cached rows with source-byte highlights, without another layout.
    pub fn highlight(
        &mut self,
        matches: &[Range<usize>],
        selected: Option<&Range<usize>>,
        style: Style,
    ) {
        self.rows.clear();
        for source in &self.sources {
            paint(source, matches, selected, style, &mut self.rows);
        }
    }

    /// Wrapped row containing a logical byte offset.
    pub fn row_at(&self, offset: usize) -> usize {
        self.ranges
            .partition_point(|range| range.end <= offset)
            .min(self.rows.len().saturating_sub(1))
    }

    /// Borrow the last projection. No per-frame transcript clone is needed.
    pub fn rows(&self) -> &[Line<'static>] {
        &self.rows
    }
}

fn visible_rows(layout: &Layout) -> impl Iterator<Item = &crate::layout::Row> {
    layout
        .rows
        .iter()
        .enumerate()
        .filter(|(index, row)| {
            !row.cells.is_empty()
                || index.checked_sub(1).is_none_or(|previous| {
                    let previous = &layout.rows[previous];
                    previous.end != row.start
                        || previous
                            .cells
                            .last()
                            .is_none_or(|cell| cell.column + cell.width != layout.width)
                })
        })
        .map(|(_, row)| row)
}

// Ordinary wrapping and search use this single style/coalescing path.
fn paint(
    source: &Source,
    matches: &[Range<usize>],
    selected: Option<&Range<usize>>,
    selection: Style,
    output: &mut Vec<Line<'static>>,
) {
    let mut next = matches.partition_point(|range| range.end <= source.offset);
    for row in visible_rows(&source.layout) {
        let mut spans = Vec::with_capacity(usize::from(!row.cells.is_empty()));
        let mut group = 0;
        let mut previous = None;
        for (index, cell) in row.cells.iter().enumerate() {
            let at = source.offset + cell.source.start;
            while matches.get(next).is_some_and(|range| range.end <= at) {
                next += 1;
            }
            let style_index = source
                .styles
                .partition_point(|(end, _)| *end <= cell.source.start);
            let mut style = source
                .styles
                .get(style_index)
                .map_or(Style::default(), |(_, style)| *style);
            if matches.get(next).is_some_and(|range| {
                range.start < source.offset + cell.source.end && range.end > at
            }) {
                style = style.patch(selection).add_modifier(Modifier::UNDERLINED);
            }
            if selected.is_some_and(|range| {
                range.start < source.offset + cell.source.end && range.end > at
            }) {
                style = style.add_modifier(Modifier::BOLD | Modifier::REVERSED);
            }
            if previous.is_some_and(|previous| previous != style) {
                spans.push(span(&row.cells[group..index], previous.unwrap()));
                group = index;
            }
            previous = Some(style);
        }
        if let Some(style) = previous {
            spans.push(span(&row.cells[group..], style));
        }
        let mut line = Line::from(spans).style(source.style);
        line.alignment = source.alignment;
        output.push(line);
    }
}

fn span(cells: &[crate::layout::Cell], style: Style) -> Span<'static> {
    let mut text = String::with_capacity(cells.iter().map(|cell| cell.symbol.len()).sum());
    for cell in cells {
        text.push_str(&cell.symbol);
    }
    Span::styled(text, style)
}
