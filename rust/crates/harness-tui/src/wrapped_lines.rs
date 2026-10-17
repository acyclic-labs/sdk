use crate::{Editor, Layout};
use ratatui::{
    layout::Alignment,
    style::{Modifier, Style},
    text::{Line, Span},
};
use std::ops::Range;
#[cfg(test)]
thread_local! { static WORK: std::cell::Cell<(usize,usize,usize)> = const { std::cell::Cell::new((0,0,0)) }; }

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
    origins: Vec<(usize, usize)>,
    revision: u64,
    pub(crate) index: crate::transcript_search::index::Index,
    pub(crate) folded_rows: Vec<Range<usize>>,
    pub(crate) source_rows: Vec<usize>,
}

impl WrappedLines {
    /// Reflow changed rows, or the whole projection after a width change.
    pub fn update(&mut self, source: &[Line<'_>], changed: usize, width: u16) {
        let data_changed = changed < source.len() || source.len() != self.sources.len();
        let reflow = self.width != Some(width);
        let changed = if !reflow {
            changed.min(source.len()).min(self.starts.len())
        } else {
            self.width = Some(width);
            0
        };
        if !reflow && changed == source.len() && source.len() == self.sources.len() {
            return;
        }
        if reflow || changed < source.len() || source.len() < self.sources.len() {
            self.revision = self
                .revision
                .checked_add(1)
                .expect("projection revision overflow");
        }
        let at = self
            .sources
            .get(changed)
            .map_or(self.text.len(), |line| line.offset);
        let row = self.starts.get(changed).copied().unwrap_or(self.rows.len());
        self.rows.truncate(row);
        self.ranges.truncate(row);
        self.origins.truncate(row);
        self.text.truncate(
            self.sources
                .get(changed)
                .map_or(self.text.len(), |line| line.offset),
        );
        self.starts.truncate(changed);
        self.sources.truncate(changed);
        self.folded_rows.truncate(row);
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
            #[cfg(test)]
            WORK.with(|work| {
                let (rows, cells, layouts) = work.get();
                work.set((rows, cells, layouts + 1));
            });
            let layout = Layout::new(&Editor::new(&self.text[offset..]), usize::from(width));
            self.text.push('\n');
            let source = Source {
                offset,
                layout,
                styles,
                style: line.style,
                alignment: line.alignment,
            };
            for (index, row) in visible_rows(&source.layout) {
                self.origins.push((self.sources.len(), index));
                self.ranges.push(offset + row.start..offset + row.end);
            }
            paint(&source, &mut self.rows);
            self.sources.push(source);
        }
        let text = &self.text[..self.text.len().saturating_sub(1)];
        if data_changed {
            self.index.replace_suffix(text, at.min(text.len()));
        }
        self.source_rows.resize(text.len() + 1, 0);
        for (row_index, range) in self.ranges.iter().enumerate().skip(row) {
            let end = self
                .ranges
                .get(row_index + 1)
                .map_or(text.len() + 1, |next| next.start);
            self.source_rows[range.start..end].fill(row_index);
        }
        self.folded_rows.extend(
            self.ranges[row..]
                .iter()
                .map(|range| self.index.folded_range(range)),
        );
    }

    /// Normalized logical text, shared by wrapping and Unicode search.
    pub fn text(&self) -> &str {
        &self.text[..self.text.len().saturating_sub(1)]
    }

    /// Changes only when source rows or wrapping change, never on frame ticks.
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Source offsets for the existing wrapped rows.
    pub fn ranges(&self) -> &[Range<usize>] {
        &self.ranges
    }

    pub(crate) fn render_ranges(
        &self,
        row: usize,
        source_offset: usize,
        matches: &[Range<usize>],
        selected: Option<&Range<usize>>,
        selection: Style,
        area: ratatui::layout::Rect,
        buffer: &mut ratatui::buffer::Buffer,
    ) {
        let area = area.intersection(buffer.area);
        if area.is_empty() {
            return;
        }
        let base = &self.rows[row];
        buffer.set_style(area, base.style);
        let (source, index) = self.origins[row];
        let source = &self.sources[source];
        let cells = &source.layout.rows[index].cells;
        crate::transcript_search::view::charge(cells.len(), false);
        #[cfg(test)]
        WORK.with(|work| {
            let (rows, count, layouts) = work.get();
            work.set((rows + 1, count + cells.len(), layouts));
        });
        let mut next = 0;
        let (mut displayed, mut base_span, mut base_end) = (0, 0, 0);
        let inset = match source.alignment.unwrap_or(ratatui::layout::Alignment::Left) {
            ratatui::layout::Alignment::Left => 0,
            ratatui::layout::Alignment::Center => {
                area.width.saturating_sub(self.rows[row].width() as u16) / 2
            }
            ratatui::layout::Alignment::Right => {
                area.width.saturating_sub(self.rows[row].width() as u16)
            }
        };
        for cell in cells {
            let at = source_offset + source.offset + cell.source.start;
            while matches.get(next).is_some_and(|range| range.end <= at) {
                next += 1;
            }
            while base
                .spans
                .get(base_span)
                .is_some_and(|span| displayed >= base_end + span.content.len())
            {
                base_end += base.spans[base_span].content.len();
                base_span += 1;
            }
            let mut style = base.style.patch(
                base.spans
                    .get(base_span)
                    .map_or(Style::default(), |span| span.style),
            );
            displayed += cell.symbol.len();
            if matches.get(next).is_some_and(|range| {
                range.start < source_offset + source.offset + cell.source.end && range.end > at
            }) {
                style = style.patch(selection).add_modifier(Modifier::UNDERLINED);
            }
            if selected.is_some_and(|range| {
                range.start < source_offset + source.offset + cell.source.end && range.end > at
            }) {
                style = style.add_modifier(Modifier::BOLD | Modifier::REVERSED);
            }
            let x = area
                .x
                .saturating_add(inset)
                .saturating_add(cell.column as u16);
            if x < area.right() && area.height > 0 {
                if buffer[(x, area.y)].symbol() == cell.symbol {
                    buffer[(x, area.y)].set_style(style);
                } else {
                    buffer.set_stringn(
                        x,
                        area.y,
                        &cell.symbol,
                        usize::from(area.right() - x),
                        style,
                    );
                }
            }
        }
    }

    /// Borrow the last projection. No per-frame transcript clone is needed.
    pub fn rows(&self) -> &[Line<'static>] {
        &self.rows
    }
}

fn visible_rows(layout: &Layout) -> impl Iterator<Item = (usize, &crate::layout::Row)> {
    layout.rows.iter().enumerate().filter(|(index, row)| {
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
}

// Cache normal display spans once; search borrows their styles and symbols.
fn paint(source: &Source, output: &mut Vec<Line<'static>>) {
    for (_, row) in visible_rows(&source.layout) {
        #[cfg(test)]
        WORK.with(|work| {
            let (rows, cells, layouts) = work.get();
            work.set((rows + 1, cells + row.cells.len(), layouts));
        });
        let mut spans = Vec::with_capacity(usize::from(!row.cells.is_empty()));
        let mut group = 0;
        let mut previous = None;
        for (index, cell) in row.cells.iter().enumerate() {
            let style_index = source
                .styles
                .partition_point(|(end, _)| *end <= cell.source.start);
            let style = source
                .styles
                .get(style_index)
                .map_or(Style::default(), |(_, style)| *style);
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

#[cfg(test)]
#[path = "wrapped_lines/tests.rs"]
mod tests;
