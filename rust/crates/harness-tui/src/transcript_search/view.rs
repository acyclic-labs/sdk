use super::transcript_matches;
use crate::{config::ThemeRole, config::default_style};
use ratatui::{
    style::Modifier,
    text::{Line, Span},
};

/// Disposable highlighted transcript rows for the followed search selection.
#[derive(Default)]
pub struct TranscriptSearch {
    rows: Vec<Line<'static>>,
    locations: Vec<usize>,
}

impl TranscriptSearch {
    /// Rebuild after transcript content, wrapping, or the query changes.
    /// Match offsets refer to displayed UTF-8, including across styled spans.
    pub fn update(&mut self, source: &[Line<'_>], query: &str, width: u16) {
        self.rows.clear();
        self.locations.clear();
        let text = source
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");
        let matches = transcript_matches(&text, query);
        let mut offset = 0;
        let mut next = 0;
        let mut located = 0;
        for line in source {
            let length = line
                .spans
                .iter()
                .map(|span| span.content.len())
                .sum::<usize>();
            let input = &text[offset..offset + length];
            let layout = crate::Layout::new(&crate::Editor::new(input), usize::from(width));
            let mut styles = Vec::new();
            let mut end = 0;
            for span in &line.spans {
                end += span.content.len();
                styles.push((end, span.style));
            }
            for (index, row) in layout.rows.iter().enumerate() {
                if row.cells.is_empty() && index > 0 && layout.rows[index - 1].end == row.start {
                    continue;
                }
                let mut spans = Vec::new();
                for cell in &row.cells {
                    let at = offset + cell.source.start;
                    while matches.get(next).is_some_and(|range| range.end <= at) {
                        next += 1;
                    }
                    let selected = matches.get(next).is_some_and(|range| {
                        range.start < offset + cell.source.end && range.end > at
                    });
                    let style = styles
                        .get(styles.partition_point(|(end, _)| *end <= cell.source.start))
                        .map_or(ratatui::style::Style::default(), |(_, style)| *style);
                    let style = if selected {
                        style
                            .patch(default_style(ThemeRole::Selection))
                            .add_modifier(Modifier::UNDERLINED)
                    } else {
                        style
                    };
                    spans.push(Span::styled(cell.symbol.clone(), style));
                }
                while matches.get(located).is_some_and(|range| {
                    range.start < offset + row.end || range.start == offset + row.start
                }) {
                    self.locations.push(self.rows.len());
                    located += 1;
                }
                self.rows.push(Line::from(spans).style(line.style));
            }
            offset += length + 1;
        }
    }

    /// Highlighted rows, preserving the original Markdown span styles.
    pub fn rows(&self) -> &[Line<'static>] {
        &self.rows
    }

    /// Number of matches in transcript order.
    pub fn count(&self) -> usize {
        self.locations.len()
    }

    /// The selected row, wrapping signed next/previous navigation.
    pub fn selected_row(&self, index: i64) -> usize {
        if self.locations.is_empty() {
            0
        } else {
            self.locations[index.rem_euclid(self.locations.len() as i64) as usize]
        }
    }
}
