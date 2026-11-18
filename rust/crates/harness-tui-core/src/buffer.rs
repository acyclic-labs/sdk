use std::{ops::Range, rc::Rc, slice::SliceIndex};
use unicode_segmentation::UnicodeSegmentation;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Buffer {
    pub text: String,
    pub cursor: usize,
    pub anchor: usize,
    pub folds: Vec<Range<usize>>,
    points: Vec<usize>,
    pub revision: Rc<()>,
    pub change: Option<(Rc<()>, Range<usize>, usize)>,
}

impl Default for Buffer {
    fn default() -> Self {
        Self::new(String::new())
    }
}

impl Buffer {
    pub fn new(text: String) -> Self {
        let end = text.len();
        let mut buffer = Self {
            text,
            cursor: end,
            anchor: end,
            folds: vec![],
            points: vec![],
            revision: Rc::new(()),
            change: None,
        };
        buffer.rebuild_boundaries();
        buffer
    }
    pub fn selection(&self) -> Range<usize> {
        self.anchor.min(self.cursor)..self.anchor.max(self.cursor)
    }

    pub fn rebuild_boundaries(&mut self) {
        let raw: Vec<_> = self
            .text
            .grapheme_indices(true)
            .map(|(i, _)| i)
            .chain([self.text.len()])
            .collect();
        self.folds
            .retain(|r| raw.binary_search(&r.start).is_ok() && raw.binary_search(&r.end).is_ok());
        let mut folds = self.folds.iter().peekable();
        self.points = raw
            .into_iter()
            .filter(|i| {
                while folds.peek().is_some_and(|r| r.end <= *i) {
                    folds.next();
                }
                !folds.peek().is_some_and(|r| r.start < *i && *i < r.end)
            })
            .collect();
    }

    /// The boundary at `index`; the text end is always the last boundary.
    fn point(&self, index: usize) -> usize {
        self.points.get(index).copied().unwrap_or(self.text.len())
    }

    pub fn floor(&self, offset: usize) -> usize {
        self.point(
            self.points
                .partition_point(|i| *i <= offset)
                .saturating_sub(1),
        )
    }

    pub fn ceil(&self, offset: usize) -> usize {
        self.points
            .get(self.points.partition_point(|i| *i < offset))
            .copied()
            .unwrap_or(self.text.len())
    }

    pub fn previous(&self, offset: usize) -> usize {
        self.point(
            self.points
                .partition_point(|i| *i < offset)
                .saturating_sub(1),
        )
    }

    pub fn next(&self, offset: usize) -> usize {
        self.points
            .get(self.points.partition_point(|i| *i <= offset))
            .copied()
            .unwrap_or(self.text.len())
    }

    pub fn changed(&mut self, range: Range<usize>, inserted: usize) {
        self.change = Some((Rc::clone(&self.revision), range, inserted));
        self.revision = Rc::new(());
    }

    pub fn replace(&mut self, range: Range<usize>, text: &str, fold: bool) {
        self.changed(range.clone(), text.len());
        self.folds.retain_mut(|r| {
            if r.end <= range.start {
                return true;
            }
            if r.start >= range.end {
                r.start = r.start - range.len() + text.len();
                r.end = r.end - range.len() + text.len();
                return true;
            }
            false
        });
        self.text.replace_range(range.clone(), text);
        let end = range.start + text.len();
        self.rebuild_boundaries();
        // Inserting a combining mark, ZWJ, or regional indicator may join its
        // neighbours. Keep both cursor and fold ends on the new segmentation.
        if fold && !text.is_empty() {
            let start = self.floor(range.start);
            let end = self.ceil(end);
            self.folds.retain(|r| r.end <= start || r.start >= end);
            self.folds.push(start..end);
            self.folds.sort_by_key(|r| r.start);
            self.rebuild_boundaries();
        }
        self.cursor = self.ceil(end);
        self.anchor = self.cursor;
    }

    pub fn word_left(&self, offset: usize) -> usize {
        self.word_edge(offset, true, &mut || {})
    }

    pub fn word_right(&self, offset: usize) -> usize {
        self.word_edge(offset, false, &mut || {})
    }

    fn word_edge(&self, offset: usize, backwards: bool, visit: &mut impl FnMut()) -> usize {
        let index = self.points.partition_point(|i| *i < offset);
        if backwards {
            word_edge(
                &self.text,
                spans(self.points.get(..=index).unwrap_or_default()).rev(),
                offset,
                true,
                visit,
            )
        } else {
            word_edge(
                &self.text,
                spans(self.points.get(index..).unwrap_or_default()),
                offset,
                false,
                visit,
            )
        }
    }

    pub fn word_range(&self, offset: usize) -> Range<usize> {
        if self.text.is_empty() {
            return 0..0;
        }
        let mut left = self
            .points
            .partition_point(|i| *i <= offset)
            .saturating_sub(1)
            .min(self.points.len() - 2);
        let blank = |index: usize| blank(&self.text, self.point(index)..self.point(index + 1));
        let mut right = left + 1;
        let space = blank(left);
        while left > 0 && blank(left - 1) == space {
            left -= 1;
        }
        while right + 1 < self.points.len() && blank(right) == space {
            right += 1;
        }
        self.point(left)..self.point(right)
    }
}

/// The text in `range`, or nothing when the range is off a char boundary.
pub(crate) fn slice<R: SliceIndex<str, Output = str>>(text: &str, range: R) -> &str {
    text.get(range).unwrap_or_default()
}

/// Each adjacent pair of boundaries as the segment between them.
fn spans(points: &[usize]) -> impl DoubleEndedIterator<Item = Range<usize>> + '_ {
    points
        .iter()
        .zip(points.iter().skip(1))
        .map(|(start, end)| *start..*end)
}

/// Whether a segment is whitespace; a range off a char boundary is not.
fn blank(text: &str, range: Range<usize>) -> bool {
    text.get(range)
        .is_some_and(|segment| segment.chars().all(char::is_whitespace))
}

fn word_edge(
    text: &str,
    ranges: impl Iterator<Item = Range<usize>>,
    offset: usize,
    backwards: bool,
    visit: &mut impl FnMut(),
) -> usize {
    let mut result = offset;
    let mut word = false;
    for range in ranges {
        visit();
        let space = blank(text, range.clone());
        if word && space {
            break;
        }
        word |= !space;
        result = if backwards { range.start } else { range.end };
    }
    result
}

pub(crate) fn normalize(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

#[cfg(test)]
mod tests {
    use super::Buffer;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig { cases: 32, ..ProptestConfig::default() })]
        #[test]
        fn word_work_is_bounded_by_distance(prefix in 0usize..4096, distance in 1usize..128) {
            let text = format!("{} {}", "z".repeat(prefix), "x".repeat(distance));
            let buffer = Buffer::new(text.clone());
            let mut work = 0;
            let left = buffer.word_edge(text.len(), true, &mut || work += 1);
            prop_assert_eq!(left, prefix + 1);
            prop_assert!(work <= distance + 1);
            work = 0;
            prop_assert_eq!(buffer.word_edge(prefix + 1, false, &mut || work += 1), text.len());
            prop_assert!(work <= distance);
        }
    }
}
