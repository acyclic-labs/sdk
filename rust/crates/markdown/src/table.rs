use crate::{Line, Style, render, style::MUTED};
use pulldown_cmark::{Alignment, Event, Tag, TagEnd};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

pub(crate) struct Table {
    pub(super) alignments: Vec<Alignment>,
    pub(super) rows: Vec<Vec<Line>>,
    cell: Vec<Event<'static>>,
    style: Style,
    cell_width: usize,
}

impl Table {
    pub(crate) fn new(mut alignments: Vec<Alignment>, style: Style, render_width: u16) -> Self {
        let available = usize::from(render_width.max(1));
        // Each visible column needs a content cell, two spaces and a separator.
        alignments.truncate(available.saturating_sub(1) / 4);
        let columns = alignments.len();
        let cell_width = available.saturating_sub(3 * columns + 1) / columns.max(1);
        Self {
            alignments,
            rows: Vec::new(),
            cell: Vec::new(),
            style,
            cell_width,
        }
    }

    pub(crate) fn event(&mut self, event: &Event<'_>, references: Option<&crate::References>) {
        match event {
            Event::Start(Tag::TableHead | Tag::TableRow) => self.rows.push(Vec::new()),
            Event::Start(Tag::TableCell) => self.cell.clear(),
            Event::End(TagEnd::TableCell) => {
                if self
                    .rows
                    .last()
                    .is_none_or(|row| row.len() >= self.alignments.len())
                {
                    return;
                }
                let lines = render::render_styled_in(&self.cell, self.style, references);
                if let Some(row) = self.rows.last_mut() {
                    let mut cell = Line::default();
                    for (index, line) in lines.into_iter().enumerate() {
                        if index > 0 {
                            cell.push(" ↵ ", self.style, None);
                        }
                        cell.spans.append_shared(line.spans);
                    }
                    row.push(cell);
                }
            }
            Event::End(TagEnd::TableHead | TagEnd::TableRow) => {}
            _ => {
                if self
                    .rows
                    .last()
                    .is_none_or(|row| row.len() >= self.alignments.len())
                {
                    return;
                }
                #[cfg(test)]
                crate::work::event(event);
                self.cell.push(event.clone().into_static());
            }
        }
    }

    pub(super) fn widths(&self) -> Vec<usize> {
        (0..self.alignments.len())
            .map(|column| {
                self.rows
                    .iter()
                    .filter_map(|row| row.get(column))
                    .map(width)
                    .max()
                    .unwrap_or(0)
                    .max(3)
                    .min(self.cell_width)
            })
            .collect()
    }

    pub(super) fn cell_width(&self, cell: &Line) -> usize {
        width(cell).min(self.cell_width)
    }

    pub(super) fn render_rows(&self, start: usize, widths: &[usize]) -> Vec<Line> {
        self.rows
            .iter()
            .enumerate()
            .skip(start)
            .map(|(index, cells)| row(cells.clone(), index == 0, widths, &self.alignments))
            .collect()
    }

    pub(crate) fn render(self) -> Vec<Line> {
        let widths = self.widths();
        let mut lines = vec![border('┌', '┬', '┐', &widths)];
        for (index, cells) in self.rows.into_iter().enumerate() {
            lines.push(row(cells, index == 0, &widths, &self.alignments));
            if index == 0 {
                lines.push(border('├', '┼', '┤', &widths));
            }
        }
        lines.push(border('└', '┴', '┘', &widths));
        lines
    }
}

pub(super) fn width(line: &Line) -> usize {
    let mut spans = line.spans.iter();
    if let Some(span) = spans.next()
        && spans.next().is_none()
    {
        #[cfg(test)]
        crate::work::scan(span.text.len());
        UnicodeWidthStr::width(span.text.as_str().as_ref())
    } else {
        let text = cell_text(line);
        #[cfg(test)]
        crate::work::scan(text.len());
        UnicodeWidthStr::width(text.as_str())
    }
}

fn cell_text(line: &Line) -> String {
    let length = line.spans.iter().map(|span| span.text.len()).sum();
    #[cfg(test)]
    crate::work::copy(length);
    let mut text = String::with_capacity(length);
    for span in &line.spans {
        text.push_str(span.text.as_str().as_ref());
    }
    text
}

pub(super) fn border(left: char, middle: char, right: char, widths: &[usize]) -> Line {
    if widths.is_empty() {
        return Line::plain("…", MUTED);
    }
    let sections: Vec<_> = widths.iter().map(|width| "─".repeat(width + 2)).collect();
    Line::plain(
        &format!("{left}{}{right}", sections.join(&middle.to_string())),
        MUTED,
    )
}

fn row(cells: Vec<Line>, header: bool, widths: &[usize], alignments: &[Alignment]) -> Line {
    if widths.is_empty() {
        return Line::plain("…", MUTED);
    }
    let mut line = Line::plain("│", MUTED);
    let mut row = cells.into_iter();
    for (column, &size) in widths.iter().enumerate() {
        let cell = trim(row.next().unwrap_or_default(), size);
        let padding = size.saturating_sub(width(&cell));
        let left = match alignments.get(column) {
            Some(Alignment::Right) => padding,
            Some(Alignment::Center) => padding / 2,
            _ => 0,
        };
        line.push(&" ".repeat(left + 1), Style::default(), None);
        // Share the cell's runs; a header only adds bold to each of them.
        let style = Style {
            bold: header,
            ..Style::default()
        };
        line.spans.append_boundary(cell.spans, style);
        line.push(&" ".repeat(padding - left + 1), Style::default(), None);
        line.push("│", MUTED, None);
    }
    line
}

fn trim(mut cell: Line, size: usize) -> Line {
    if width(&cell) <= size {
        return cell;
    }
    let limit = size.saturating_sub(1);
    let text = cell_text(&cell);
    let mut used = 0;
    let mut remaining = 0;
    for (offset, cluster) in text.grapheme_indices(true) {
        #[cfg(test)]
        crate::work::scan(cluster.len());
        let columns = UnicodeWidthStr::width(cluster);
        if used + columns > limit {
            break;
        }
        used += columns;
        remaining = offset + cluster.len();
    }
    // Materialize only the runs that remain visible.
    let mut spans = Vec::new();
    for span in cell.spans.iter() {
        if remaining == 0 {
            break;
        }
        let mut span = span.owned();
        let end = remaining.min(span.text.len());
        remaining -= end;
        span.text.truncate(end);
        spans.push(span);
    }
    cell.spans = spans.into();
    cell.push("…", Style::default(), None);
    cell
}

#[cfg(test)]
#[path = "../tests/table_width.rs"]
mod tests;
