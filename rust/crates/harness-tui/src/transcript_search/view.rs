use super::transcript_matches;
use crate::WrappedLines;
use ratatui::{style::Style, text::Line};
use std::ops::Range;

/// Query fold and navigation metadata; painting touches only visible rows.
#[derive(Default)]
pub struct TranscriptSearch {
    query: String,
    revision: Option<u64>,
    matches: Vec<Range<usize>>,
    locations: Vec<usize>,
    cuts: Vec<Range<usize>>,
    index: i64,
    style: Style,
}

impl TranscriptSearch {
    /// Fold a changed query/projection once. Selection and theme changes are O(1).
    pub fn update(&mut self, projection: &WrappedLines, query: &str, index: i64, style: Style) {
        self.index = index;
        self.style = style;
        if self.query == query && self.revision == Some(projection.revision()) {
            return;
        }
        self.query.clear();
        self.query.push_str(query);
        self.revision = Some(projection.revision());
        if query.is_empty() {
            self.matches.clear();
            self.locations.clear();
            self.cuts.clear();
            return;
        }
        self.matches = transcript_matches(projection.text(), query);
        self.locations.clear();
        self.cuts.clear();
        let (mut first, mut end, mut located) = (0, 0, 0);
        for (row, range) in projection.ranges().iter().enumerate() {
            while self
                .matches
                .get(first)
                .is_some_and(|matched| matched.end <= range.start)
            {
                first += 1;
            }
            end = end.max(first);
            while self
                .matches
                .get(end)
                .is_some_and(|matched| matched.start < range.end)
            {
                end += 1;
            }
            self.cuts.push(first..end);
            while self
                .matches
                .get(located)
                .is_some_and(|matched| matched.start < range.end)
            {
                self.locations.push(row);
                located += 1;
            }
        }
        self.locations.resize(
            self.matches.len(),
            projection.rows().len().saturating_sub(1),
        );
    }

    /// Paint a visible row without visiting any other transcript row.
    pub fn row(&self, projection: &WrappedLines, row: usize) -> Line<'static> {
        if self.matches.is_empty() {
            return projection.rows()[row].clone();
        }
        let selected = self
            .position(self.index)
            .checked_sub(1)
            .and_then(|index| self.matches.get(index));
        projection.highlighted_row(
            row,
            &self.matches[self.cuts[row].clone()],
            selected,
            self.style,
        )
    }

    /// Number of matches in transcript order.
    pub fn count(&self) -> usize {
        self.matches.len()
    }

    /// One-based selected occurrence, or zero for an empty result.
    pub fn position(&self, index: i64) -> usize {
        if self.matches.is_empty() {
            0
        } else {
            index.rem_euclid(self.matches.len() as i64) as usize + 1
        }
    }

    /// The selected row, wrapping signed next/previous navigation.
    pub fn selected_row(&self, index: i64) -> usize {
        self.position(index)
            .checked_sub(1)
            .map_or(0, |index| self.locations[index])
    }
}
