use super::transcript_matches;
use crate::WrappedLines;
use ratatui::style::Style;

/// Match navigation over the existing transcript wrapping projection.
#[derive(Default)]
pub struct TranscriptSearch {
    locations: Vec<usize>,
}

impl TranscriptSearch {
    /// Update highlights without creating or reflowing a second projection.
    pub fn update(&mut self, projection: &mut WrappedLines, query: &str, index: i64, style: Style) {
        let matches = transcript_matches(projection.text(), query);
        self.locations.clear();
        self.locations
            .extend(matches.iter().map(|range| projection.row_at(range.start)));
        let selected = self
            .position(index)
            .checked_sub(1)
            .and_then(|index| matches.get(index));
        projection.highlight(&matches, selected, style);
    }

    /// Number of matches in transcript order.
    pub fn count(&self) -> usize {
        self.locations.len()
    }

    /// One-based selected occurrence, or zero for an empty result.
    pub fn position(&self, index: i64) -> usize {
        if self.locations.is_empty() {
            0
        } else {
            index.rem_euclid(self.locations.len() as i64) as usize + 1
        }
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
