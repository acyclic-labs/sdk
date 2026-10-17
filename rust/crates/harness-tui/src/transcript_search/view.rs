use crate::TranscriptRows;
use caseless::Caseless;
use ratatui::{buffer::Buffer, layout::Rect, style::Style, widgets::Widget};
use std::ops::Range;

#[cfg(test)]
thread_local! { pub(super) static WORK: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0,0)) }; }
pub(crate) fn charge(bytes: usize, background: bool) {
    #[cfg(test)]
    WORK.with(|work| {
        let (front, back) = work.get();
        work.set(if background {
            (front, back + bytes)
        } else {
            (front + bytes, back)
        });
    });
    #[cfg(not(test))]
    let _ = (bytes, background);
}

/// Cancellable query fold over the transcript's maintained folded-text index.
#[derive(Default)]
pub struct TranscriptSearch {
    needle: String,
    previous_needle: String,
    prefix: Vec<usize>,
    revision: Option<(u64, Option<u64>)>,
    positions: Vec<usize>,
    previous: Vec<usize>,
    matches: Vec<Range<usize>>,
    local: Vec<Range<usize>>,
    cursor: usize,
    refine: bool,
    complete: bool,
    generation: u64,
    viewport: Option<(u64, (u64, Option<u64>), usize, usize)>,
    nearest: Option<Range<usize>>,
    probe: Option<Range<usize>>,
    index: i64,
    selected: Option<Range<usize>>,
    selected_rank: Option<usize>,
    steps: i64,
    navigation: Option<navigation::Probe>,
    style: Style,
}

impl TranscriptSearch {
    /// Observe a transcript cut only while Find is open. Closing releases its query storage.
    pub fn project<'a>(&mut self, projection: impl Into<TranscriptRows<'a>>, open: bool) {
        let projection = projection.into();
        if !open {
            *self = Self::default();
            self.complete = true;
            return;
        }
        if self.revision == Some(projection.index_revision()) {
            return;
        }
        self.revision = Some(projection.index_revision());
        self.restart(false);
    }

    /// A key only changes the needle and cancels the previous background fold.
    pub fn update<'a>(
        &mut self,
        projection: impl Into<TranscriptRows<'a>>,
        query: &str,
        index: i64,
        style: Style,
    ) {
        let projection = projection.into();
        self.project(projection, true);
        self.style = style;
        charge(query.len(), false);
        if query.chars().default_case_fold().eq(self.needle.chars()) {
            if index != self.index {
                self.steps = self.steps.saturating_add(index.saturating_sub(self.index));
                self.navigation = None;
                self.navigate(projection, 256);
                self.index = index;
            }
            return;
        }
        self.index = index;
        std::mem::swap(&mut self.needle, &mut self.previous_needle);
        self.needle.clear();
        self.needle.extend(query.chars().default_case_fold());
        let refine = self.complete
            && !self.previous_needle.is_empty()
            && self.needle.starts_with(&self.previous_needle);
        self.restart(refine);
        self.prefix.clear();
        self.prefix.resize(self.needle.len(), 0);
        for end in 1..self.needle.len() {
            let mut length = self.prefix[end - 1];
            while length != 0 && self.needle.as_bytes()[length] != self.needle.as_bytes()[end] {
                charge(1, false);
                length = self.prefix[length - 1];
            }
            charge(1, false);
            self.prefix[end] =
                length + usize::from(self.needle.as_bytes()[length] == self.needle.as_bytes()[end]);
        }
    }

    fn restart(&mut self, refine: bool) {
        if refine {
            std::mem::swap(&mut self.positions, &mut self.previous);
            self.positions.clear();
        } else {
            self.positions = Vec::new();
            self.previous = Vec::new();
        }
        self.matches = Vec::new();
        self.local.clear();
        self.cursor = 0;
        self.selected = None;
        self.selected_rank = None;
        self.steps = 0;
        self.navigation = None;
        self.refine = refine;
        self.complete = self.needle.is_empty();
        self.generation = self.generation.wrapping_add(1);
        self.viewport = None;
        self.nearest = None;
        self.probe = None;
    }

    /// Search only the viewport (plus needle overlap) before the next frame.
    pub fn prepare<'a>(
        &mut self,
        projection: impl Into<TranscriptRows<'a>>,
        first: usize,
        height: usize,
    ) {
        let projection = projection.into();
        let cut = (self.generation, projection.revision(), first, height);
        if self.viewport == Some(cut) {
            return;
        }
        self.viewport = Some(cut);
        self.local.clear();
        if self.needle.is_empty() {
            return;
        }
        let Some(start) = projection.folded_row(first) else {
            return;
        };
        let end = projection
            .folded_row(first.saturating_add(height).saturating_sub(1))
            .or_else(|| {
                projection
                    .len()
                    .checked_sub(1)
                    .and_then(|row| projection.folded_row(row))
            })
            .map_or(start.end, |row| row.end);
        let range = start.start..end;
        scan(
            projection,
            &self.needle,
            &self.prefix,
            range.clone(),
            false,
            |at| {
                push_range(&mut self.local, projection.source(at, self.needle.len()));
            },
        );
        if self.probe.is_none() && self.nearest.is_none() {
            self.probe = Some(if self.index < 0 {
                0..range.start
            } else {
                range.end..projection.folded_len()
            });
        }
        self.select_initial();
        // A short transcript already fits the frame budget: its count is final.
        if range.start == 0 && range.end == projection.folded_len() {
            self.advance(projection, projection.folded_len().max(1));
        }
    }

    /// One cooperative background slice. Call between frames, after input wins.
    pub fn advance<'a>(
        &mut self,
        projection: impl Into<TranscriptRows<'a>>,
        budget: usize,
    ) -> bool {
        let projection = projection.into();
        if self.complete {
            self.navigate(projection, budget);
            return false;
        }
        self.navigate(projection, budget);
        let budget = budget.max(self.needle.len()).max(1);
        if self.local.is_empty() && self.nearest.is_none() {
            if let Some(probe) = &mut self.probe {
                let range = if self.index < 0 {
                    probe.end.saturating_sub(budget).max(probe.start)..probe.end
                } else {
                    probe.start..probe.end.min(probe.start.saturating_add(budget))
                };
                scan(
                    projection,
                    &self.needle,
                    &self.prefix,
                    range.clone(),
                    true,
                    |at| {
                        if self.nearest.is_none() || self.index < 0 {
                            self.nearest = Some(projection.source(at, self.needle.len()));
                        }
                    },
                );
                if self.index < 0 {
                    probe.end = range.start;
                } else {
                    probe.start = range.end;
                }
            }
        }
        if self.refine {
            let extension = &self.needle.as_bytes()[self.previous_needle.len()..];
            let end = self.previous.len().min(
                self.cursor
                    .saturating_add((budget / extension.len()).max(1)),
            );
            for &at in &self.previous[self.cursor..end] {
                charge(extension.len(), true);
                if at + self.needle.len() <= projection.folded_len()
                    && projection.matches(
                        at + self.previous_needle.len()..at + self.needle.len(),
                        extension,
                    )
                {
                    self.positions.push(at);
                    push_range(&mut self.matches, projection.source(at, self.needle.len()));
                }
            }
            self.cursor = end;
            self.complete = end == self.previous.len();
        } else {
            let end = projection
                .folded_len()
                .min(self.cursor.saturating_add(budget));
            scan(
                projection,
                &self.needle,
                &self.prefix,
                self.cursor..end,
                true,
                |at| {
                    self.positions.push(at);
                    push_range(&mut self.matches, projection.source(at, self.needle.len()));
                },
            );
            self.cursor = end;
            self.complete = end == projection.folded_len();
        }
        if self.complete {
            self.selected_rank = self.selected.as_ref().and_then(|selected| {
                self.matches
                    .binary_search_by(|range| {
                        charge(1, true);
                        range
                            .start
                            .cmp(&selected.start)
                            .then(range.end.cmp(&selected.end))
                    })
                    .ok()
            });
            if self.selected_rank.is_none() && !self.matches.is_empty() {
                self.selected_rank =
                    Some(self.index.rem_euclid(self.matches.len() as i64) as usize);
                // Without an anchor, index already includes all queued navigation.
                self.steps = 0;
            }
            if let Some(rank) = self.selected_rank {
                self.selected_rank = Some(
                    (rank as i64 + self.steps.rem_euclid(self.matches.len() as i64))
                        .rem_euclid(self.matches.len() as i64) as usize,
                );
                self.steps = 0;
                self.navigation = None;
            }
            self.selected = self
                .selected_rank
                .and_then(|rank| self.matches.get(rank))
                .cloned();
        } else {
            self.select_initial();
        }
        self.pending()
    }

    /// True while a cancellable full-count fold remains.
    pub fn pending(&self) -> bool {
        !self.complete
    }

    fn active(&self) -> &[Range<usize>] {
        if self.complete {
            &self.matches
        } else if !self.local.is_empty() {
            &self.local
        } else {
            self.nearest.as_ref().map_or(&[], std::slice::from_ref)
        }
    }

    /// Borrow cached display text and overlay ranges directly in the buffer.
    pub fn row<'a>(
        &'a self,
        projection: impl Into<TranscriptRows<'a>>,
        row: usize,
    ) -> impl Widget + 'a {
        let projection = projection.into();
        let selected = self.selected.as_ref();
        let range = projection.range(row);
        let matches = &self.local;
        let first = matches.partition_point(|matched| matched.end <= range.start);
        let end = first + matches[first..].partition_point(|matched| matched.start < range.end);
        SearchRow {
            projection,
            row,
            matches: &matches[first..end],
            selected,
            style: self.style,
        }
    }

    /// Known match count; pending() distinguishes a partial viewport count.
    pub fn count(&self) -> usize {
        self.active().len()
    }

    fn select_initial(&mut self) {
        if self.selected.is_none() && !self.active().is_empty() {
            let rank = self.index.rem_euclid(self.active().len() as i64) as usize;
            self.selected = self.active().get(rank).cloned();
            // The initial rank already includes every move queued without an anchor.
            self.steps = 0;
            self.navigation = None;
        }
    }

    /// One-based selected occurrence, or zero while no match is known.
    pub fn position(&self, index: i64) -> usize {
        let count = self.count();
        if count == 0 {
            return 0;
        }
        let rank = if self.complete {
            self.selected_rank
        } else {
            self.selected.as_ref().and_then(|selected| {
                self.active()
                    .binary_search_by(|range| {
                        charge(1, false);
                        range
                            .start
                            .cmp(&selected.start)
                            .then(range.end.cmp(&selected.end))
                    })
                    .ok()
            })
        };
        let offset = if !self.complete {
            0
        } else if self.selected.is_some() {
            index.saturating_sub(self.index)
        } else {
            index
        };
        (rank.unwrap_or(0) as i64 + offset.rem_euclid(count as i64)).rem_euclid(count as i64)
            as usize
            + 1
    }

    /// Project the stable source-byte match identity to its current row.
    pub fn selected_row<'a>(&self, projection: impl Into<TranscriptRows<'a>>, index: i64) -> usize {
        let projection = projection.into();
        let selected = if index == self.index {
            self.selected.as_ref()
        } else {
            self.position(index)
                .checked_sub(1)
                .and_then(|rank| self.active().get(rank))
        };
        selected
            .and_then(|range| projection.source_row(range.start))
            .unwrap_or(0)
    }
}

fn push_range(ranges: &mut Vec<Range<usize>>, range: Range<usize>) {
    if ranges.last() != Some(&range) {
        ranges.push(range);
    }
}

fn scan(
    projection: TranscriptRows<'_>,
    needle: &str,
    prefix: &[usize],
    starts: Range<usize>,
    background: bool,
    mut found: impl FnMut(usize),
) {
    if needle.is_empty() || starts.is_empty() {
        return;
    }
    // Include enough context for a match crossing either edge of a wrapped row.
    let from = if background {
        starts.start
    } else {
        starts.start.saturating_sub(needle.len() - 1)
    };
    let end = projection
        .folded_len()
        .min(starts.end.saturating_add(needle.len() - 1));
    charge(end - from, background);
    if let Some(bytes) = projection
        .slice(from..end)
        .filter(|_| prefix.last() == Some(&0))
    {
        // Unbordered needles cannot overlap. memmem uses SIMD where available.
        for at in memchr::memmem::find_iter(bytes, needle.as_bytes()) {
            let at = from + at;
            if at < starts.end && at + needle.len() > starts.start {
                found(at);
            }
        }
    } else {
        // Borderful needles need overlapping matches; prefix links bound work.
        let mut length = 0;
        for (offset, byte) in projection.bytes(from..end).enumerate() {
            while length != 0 && needle.as_bytes()[length] != byte {
                charge(1, background);
                length = prefix[length - 1];
            }
            length += usize::from(needle.as_bytes()[length] == byte);
            if length == needle.len() {
                let at = from + offset + 1 - needle.len();
                if at < starts.end && at + needle.len() > starts.start {
                    found(at);
                }
                length = prefix[length - 1];
            }
        }
    }
}

struct SearchRow<'a> {
    projection: TranscriptRows<'a>,
    row: usize,
    matches: &'a [Range<usize>],
    selected: Option<&'a Range<usize>>,
    style: Style,
}
impl Widget for SearchRow<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        self.projection.render(
            self.row,
            self.matches,
            self.selected,
            self.style,
            area,
            buffer,
        );
    }
}

#[cfg(test)]
#[path = "work_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "memory_tests.rs"]
mod memory_tests;

mod navigation;
