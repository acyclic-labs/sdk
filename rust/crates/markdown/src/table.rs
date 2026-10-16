use crate::{Line, Style, render, style::MUTED};
use pulldown_cmark::{Alignment, Event, Tag, TagEnd};
use unicode_width::UnicodeWidthStr;

pub(crate) struct Table {
    alignments: Vec<Alignment>,
    rows: Vec<Vec<Line>>,
    cell: Vec<Event<'static>>,
    style: Style,
}

impl Table {
    pub(crate) fn new(alignments: Vec<Alignment>, style: Style) -> Self {
        Self {
            alignments,
            rows: Vec::new(),
            cell: Vec::new(),
            style,
        }
    }

    pub(crate) fn event(&mut self, event: &Event<'_>, references: Option<&crate::References>) {
        match event {
            Event::Start(Tag::TableHead | Tag::TableRow) => self.rows.push(Vec::new()),
            Event::Start(Tag::TableCell) => self.cell.clear(),
            Event::End(TagEnd::TableCell) => {
                let lines = render::render_styled_in(&self.cell, self.style, references);
                if let Some(row) = self.rows.last_mut() {
                    let mut cell = Line::default();
                    for (index, line) in lines.into_iter().enumerate() {
                        if index > 0 {
                            cell.push(" ↵ ", self.style, None);
                        }
                        cell.spans.extend(line.spans);
                    }
                    row.push(cell);
                }
            }
            Event::End(TagEnd::TableHead | TagEnd::TableRow) => {}
            _ => {
                #[cfg(test)]
                crate::work::event(event);
                self.cell.push(event.clone().into_static());
            }
        }
    }

    pub(crate) fn render(self) -> Vec<Line> {
        let widths: Vec<usize> = (0..self.alignments.len())
            .map(|column| {
                self.rows
                    .iter()
                    .filter_map(|row| row.get(column))
                    .map(width)
                    .max()
                    .unwrap_or(0)
                    .max(3)
            })
            .collect();
        let mut lines = vec![border('┌', '┬', '┐', &widths)];
        for (index, row) in self.rows.into_iter().enumerate() {
            let mut line = Line::plain("│", MUTED);
            let mut row = row.into_iter();
            for (column, &size) in widths.iter().enumerate() {
                let cell = row.next().unwrap_or_default();
                let padding = size.saturating_sub(width(&cell));
                let left = match self.alignments.get(column) {
                    Some(Alignment::Right) => padding,
                    Some(Alignment::Center) => padding / 2,
                    _ => 0,
                };
                line.push(&" ".repeat(left + 1), Style::default(), None);
                for mut span in cell.spans {
                    span.style.bold |= index == 0;
                    line.spans.push(span);
                }
                line.push(&" ".repeat(padding - left + 1), Style::default(), None);
                line.push("│", MUTED, None);
            }
            lines.push(line);
            if index == 0 {
                lines.push(border('├', '┼', '┤', &widths));
            }
        }
        lines.push(border('└', '┴', '┘', &widths));
        lines
    }
}

fn width(line: &Line) -> usize {
    line.spans
        .iter()
        .map(|span| UnicodeWidthStr::width(span.text.as_str()))
        .sum()
}

fn border(left: char, middle: char, right: char, widths: &[usize]) -> Line {
    let sections: Vec<_> = widths.iter().map(|width| "─".repeat(width + 2)).collect();
    Line::plain(
        &format!("{left}{}{right}", sections.join(&middle.to_string())),
        MUTED,
    )
}
