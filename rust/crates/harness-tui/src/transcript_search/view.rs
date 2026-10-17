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
    pub fn update(&mut self, source: &[Line<'_>], query: &str) {
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
        for (row, line) in source.iter().enumerate() {
            let mut spans = Vec::new();
            for span in &line.spans {
                let mut local = 0;
                while local < span.content.len() {
                    let at = offset + local;
                    while next < matches.len() && matches[next].end <= at {
                        next += 1;
                    }
                    let matched = matches
                        .get(next)
                        .filter(|range| range.start <= at && at < range.end);
                    let end = if let Some(range) = matched {
                        range.end.min(offset + span.content.len())
                    } else {
                        matches
                            .get(next)
                            .map_or(offset + span.content.len(), |range| {
                                range.start.min(offset + span.content.len())
                            })
                    };
                    let style = if matched.is_some() {
                        span.style
                            .patch(default_style(ThemeRole::Selection))
                            .add_modifier(Modifier::UNDERLINED)
                    } else {
                        span.style
                    };
                    spans.push(Span::styled(
                        span.content[local..end - offset].to_owned(),
                        style,
                    ));
                    local = end - offset;
                }
                offset += span.content.len();
            }
            while matches
                .get(located)
                .is_some_and(|range| range.start <= offset)
            {
                self.locations.push(row);
                located += 1;
            }
            offset += 1;
            self.rows.push(Line::from(spans).style(line.style));
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
