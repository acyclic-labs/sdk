use crate::WrappedLines;
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
    revision: Option<u64>,
    positions: Vec<usize>,
    previous: Vec<usize>,
    matches: Vec<Range<usize>>,
    local: Vec<Range<usize>>,
    cursor: usize,
    refine: bool,
    complete: bool,
    generation: u64,
    viewport: Option<(u64, usize, usize)>,
    nearest: Option<Range<usize>>,
    probe: Option<Range<usize>>,
    index: i64,
    base: i64,
    style: Style,
}

impl TranscriptSearch {
    /// Reserve reusable query storage with transcript updates, never per key.
    pub fn project(&mut self, projection: &WrappedLines) {
        if self.revision == Some(projection.revision()) {
            return;
        }
        self.revision = Some(projection.revision());
        let size = projection.index.text.len();
        for positions in [&mut self.positions, &mut self.previous] {
            positions.reserve(size.saturating_sub(positions.len()));
        }
        for ranges in [&mut self.matches, &mut self.local] {
            ranges.reserve(size.saturating_sub(ranges.len()));
        }
        self.prefix.reserve(size.saturating_sub(self.prefix.len()));
        self.needle.reserve(size.saturating_sub(self.needle.len()));
        self.previous_needle
            .reserve(size.saturating_sub(self.previous_needle.len()));
        self.restart(false);
    }

    /// A key only changes the needle and cancels the previous background fold.
    pub fn update(&mut self, projection: &WrappedLines, query: &str, index: i64, style: Style) {
        self.project(projection);
        self.index = index;
        self.style = style;
        charge(query.len(), false);
        if query.chars().default_case_fold().eq(self.needle.chars()) {
            return;
        }
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
        }
        self.positions.clear();
        self.matches.clear();
        self.local.clear();
        self.cursor = 0;
        self.base = 0;
        self.refine = refine;
        self.complete = self.needle.is_empty();
        self.generation = self.generation.wrapping_add(1);
        self.viewport = None;
        self.nearest = None;
        self.probe = None;
    }

    /// Search only the viewport (plus needle overlap) before the next frame.
    pub fn prepare(&mut self, projection: &WrappedLines, first: usize, height: usize) {
        let cut = (self.generation, first, height);
        if self.viewport == Some(cut) {
            return;
        }
        self.viewport = Some(cut);
        self.local.clear();
        if self.needle.is_empty() {
            return;
        }
        let rows = &projection.folded_rows;
        let Some(start) = rows.get(first) else {
            return;
        };
        let end = rows
            .get(first.saturating_add(height).saturating_sub(1))
            .or_else(|| rows.last())
            .map_or(start.end, |row| row.end);
        let range = start.start..end;
        scan(
            projection,
            &self.needle,
            &self.prefix,
            range.clone(),
            false,
            |at| {
                push_range(
                    &mut self.local,
                    projection.index.source(at, self.needle.len()),
                );
            },
        );
        if self.probe.is_none() && self.nearest.is_none() {
            self.probe = Some(if self.index < 0 {
                0..range.start
            } else {
                range.end..projection.index.text.len()
            });
        }
        // A short transcript already fits the frame budget: its count is final.
        if range.start == 0 && range.end == projection.index.text.len() {
            self.advance(projection, projection.index.text.len().max(1));
        }
    }

    /// One cooperative background slice. Call between frames, after input wins.
    pub fn advance(&mut self, projection: &WrappedLines, budget: usize) -> bool {
        if self.complete {
            return false;
        }
        let selected = self
            .position(self.index)
            .checked_sub(1)
            .and_then(|index| self.active().get(index))
            .cloned();
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
                            self.nearest = Some(projection.index.source(at, self.needle.len()));
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
            let end = self.previous.len().min(
                self.cursor
                    .saturating_add((budget / self.needle.len()).max(1)),
            );
            for &at in &self.previous[self.cursor..end] {
                charge(self.needle.len(), true);
                if projection
                    .index
                    .text
                    .as_bytes()
                    .get(at..at + self.needle.len())
                    == Some(self.needle.as_bytes())
                {
                    self.positions.push(at);
                    push_range(
                        &mut self.matches,
                        projection.index.source(at, self.needle.len()),
                    );
                }
            }
            self.cursor = end;
            self.complete = end == self.previous.len();
        } else {
            let end = projection
                .index
                .text
                .len()
                .min(self.cursor.saturating_add(budget));
            scan(
                projection,
                &self.needle,
                &self.prefix,
                self.cursor..end,
                true,
                |at| {
                    self.positions.push(at);
                    push_range(
                        &mut self.matches,
                        projection.index.source(at, self.needle.len()),
                    );
                },
            );
            self.cursor = end;
            self.complete = end == projection.index.text.len();
        }
        if self.complete {
            if let Some(selected) = selected {
                charge(self.matches.len(), true);
                if let Some(position) = self.matches.iter().position(|range| *range == selected) {
                    self.base = position as i64 - self.index.rem_euclid(self.matches.len() as i64);
                }
            }
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
    pub fn row<'a>(&'a self, projection: &'a WrappedLines, row: usize) -> impl Widget + 'a {
        let matches = self.active();
        let selected = self
            .position(self.index)
            .checked_sub(1)
            .and_then(|index| matches.get(index));
        let range = &projection.ranges()[row];
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

    /// One-based selected occurrence, or zero while no match is known.
    pub fn position(&self, index: i64) -> usize {
        let count = self.count();
        if count == 0 {
            0
        } else {
            (index.rem_euclid(count as i64) + self.base).rem_euclid(count as i64) as usize + 1
        }
    }

    /// Indexed source-row lookup for the selected occurrence.
    pub fn selected_row(&self, projection: &WrappedLines, index: i64) -> usize {
        self.position(index).checked_sub(1).map_or(0, |index| {
            projection
                .source_rows
                .get(self.active()[index].start)
                .copied()
                .unwrap_or(0)
        })
    }
}

fn push_range(ranges: &mut Vec<Range<usize>>, range: Range<usize>) {
    if ranges.last() != Some(&range) {
        ranges.push(range);
    }
}

fn scan(
    projection: &WrappedLines,
    needle: &str,
    prefix: &[usize],
    starts: Range<usize>,
    background: bool,
    mut found: impl FnMut(usize),
) {
    if needle.is_empty() || starts.is_empty() {
        return;
    }
    let bytes = projection.index.text.as_bytes();
    // Include enough context for a match crossing either edge of a wrapped row.
    let from = if background {
        starts.start
    } else {
        starts.start.saturating_sub(needle.len() - 1)
    };
    let end = bytes.len().min(starts.end.saturating_add(needle.len() - 1));
    charge(end - from, background);
    let bytes = &bytes[from..end];
    if prefix.last() == Some(&0) {
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
        for (offset, &byte) in bytes.iter().enumerate() {
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
    projection: &'a WrappedLines,
    row: usize,
    matches: &'a [Range<usize>],
    selected: Option<&'a Range<usize>>,
    style: Style,
}
impl Widget for SearchRow<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        self.projection.render_ranges(
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
