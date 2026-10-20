use crate::Line;
use std::{
    fmt,
    ops::{Bound, Index, IndexMut, Range, RangeBounds},
};

const FIRST: usize = 2;

// Pages grow geometrically; existing rows never move when another page is
// allocated. Only the logarithmic directory of page handles can relocate.
#[derive(Default)]
pub(crate) struct Buffer {
    pages: Vec<Vec<Line>>,
    length: usize,
    capacity: usize,
}

impl Buffer {
    fn location(index: usize) -> (usize, usize) {
        let page = (index / FIRST + 1).ilog2() as usize;
        (page, index - FIRST * ((1usize << page) - 1))
    }
    pub(crate) fn len(&self) -> usize {
        self.length
    }
    #[cfg(test)]
    pub(crate) fn last(&self) -> Option<&Line> {
        self.length.checked_sub(1).and_then(|index| self.get(index))
    }
    #[cfg(test)]
    pub(crate) fn pop(&mut self) -> Option<Line> {
        let index = self.length.checked_sub(1)?;
        let (page, _) = Self::location(index);
        self.length -= 1;
        self.pages.get_mut(page)?.pop()
    }
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }
    pub(crate) fn reserve(&mut self, additional: usize) {
        assert!(
            additional <= usize::MAX - self.length,
            "line capacity overflow"
        );
        let end = self.length + additional;
        while self.capacity() < end {
            #[cfg(test)]
            if self.pages.len() == self.pages.capacity() {
                crate::work::copy(self.pages.len() * std::mem::size_of::<Vec<Line>>());
            }
            let capacity = FIRST << self.pages.len();
            self.pages.push(Vec::with_capacity(capacity));
            self.capacity += capacity;
        }
    }
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        let mut value = Self::default();
        value.reserve(capacity);
        value
    }
    pub(crate) fn push(&mut self, line: Line) {
        #[cfg(test)]
        if COPY_HISTORY.get() {
            crate::work::copy(self.length * std::mem::size_of::<Line>());
            let copies: Vec<_> = self.iter().map(|line| line.spans.to_vec()).collect();
            std::hint::black_box(copies);
        }
        self.reserve(1);
        let (page, offset) = Self::location(self.length);
        let Some(page) = self.pages.get_mut(page) else {
            return;
        };
        debug_assert_eq!(offset, page.len());
        page.push(line);
        self.length += 1;
    }
    pub(crate) fn get(&self, index: usize) -> Option<&Line> {
        if index >= self.length {
            return None;
        }
        let (page, offset) = Self::location(index);
        self.pages.get(page)?.get(offset)
    }
    pub(crate) fn get_mut(&mut self, index: usize) -> Option<&mut Line> {
        if index >= self.length {
            return None;
        }
        let (page, offset) = Self::location(index);
        self.pages.get_mut(page)?.get_mut(offset)
    }
    pub(crate) fn view(&self, range: impl RangeBounds<usize>) -> Option<Lines<'_>> {
        self.all().slice(range)
    }
    pub(crate) fn all(&self) -> Lines<'_> {
        Lines {
            buffer: self,
            start: 0,
            end: self.length,
        }
    }
    pub(crate) fn iter(&self) -> LinesIter<'_> {
        self.all().iter()
    }
    pub(crate) fn truncate(&mut self, length: usize) {
        while self.length > length {
            let (page, _) = Self::location(self.length - 1);
            if let Some(page) = self.pages.get_mut(page) {
                page.pop();
            }
            self.length -= 1;
        }
    }
    pub(crate) fn append(&mut self, lines: &mut Vec<Line>) {
        self.extend(lines.drain(..));
    }
    pub(crate) fn split_off(&mut self, first: usize) -> Self {
        let mut suffix = Self::with_capacity(self.length - first);
        for index in first..self.length {
            if let Some(line) = self.get_mut(index) {
                suffix.push(std::mem::take(line));
            }
        }
        self.truncate(first);
        suffix
    }
    pub(crate) fn retain_suffix(&mut self, first: usize, mut keep: impl FnMut(&mut Line) -> bool) {
        let mut write = first;
        for read in first..self.length {
            let Some(line) = self.get_mut(read) else {
                break;
            };
            if keep(line) {
                if write != read {
                    let moved = std::mem::take(line);
                    if let Some(target) = self.get_mut(write) {
                        *target = moved;
                    }
                }
                write += 1;
            } else {
                *line = Line::default();
            }
        }
        self.truncate(write);
    }
    #[cfg(test)]
    pub(crate) fn retained(&self) -> usize {
        self.capacity() * std::mem::size_of::<Line>()
            + self.pages.capacity() * std::mem::size_of::<Vec<Line>>()
    }
}
impl Index<usize> for Buffer {
    type Output = Line;
    fn index(&self, index: usize) -> &Line {
        let (page, offset) = Self::location(index);
        self.pages.index(page).index(offset)
    }
}
impl IndexMut<usize> for Buffer {
    fn index_mut(&mut self, index: usize) -> &mut Line {
        let (page, offset) = Self::location(index);
        self.pages.index_mut(page).index_mut(offset)
    }
}
impl Extend<Line> for Buffer {
    fn extend<I: IntoIterator<Item = Line>>(&mut self, lines: I) {
        for line in lines {
            self.push(line);
        }
    }
}
impl IntoIterator for Buffer {
    type Item = Line;
    type IntoIter = std::iter::Flatten<std::vec::IntoIter<Vec<Line>>>;
    fn into_iter(self) -> Self::IntoIter {
        self.pages.into_iter().flatten()
    }
}
impl<'a> IntoIterator for &'a Buffer {
    type Item = &'a Line;
    type IntoIter = LinesIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Borrowed unwrapped rows. Growth preserves every existing row's address;
/// slicing and iteration borrow the immutable contents without flattening them.
#[derive(Clone, Copy)]
pub struct Lines<'a> {
    buffer: &'a Buffer,
    start: usize,
    end: usize,
}
impl<'a> Lines<'a> {
    /// Number of displayed rows.
    pub fn len(self) -> usize {
        self.end - self.start
    }
    /// Whether this view contains no rows.
    pub fn is_empty(self) -> bool {
        self.start == self.end
    }
    /// Borrow one row relative to this view.
    pub fn get(self, index: usize) -> Option<&'a Line> {
        (index < self.len()).then(|| &self.buffer[self.start + index])
    }
    /// Borrow a subrange without copying rows.
    pub fn slice(self, range: impl RangeBounds<usize>) -> Option<Self> {
        let start = match range.start_bound() {
            Bound::Included(n) => *n,
            Bound::Excluded(n) => n.checked_add(1)?,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(n) => n.checked_add(1)?,
            Bound::Excluded(n) => *n,
            Bound::Unbounded => self.len(),
        };
        (start <= end && end <= self.len()).then(|| Self {
            buffer: self.buffer,
            start: self.start + start,
            end: self.start + end,
        })
    }
    /// First row, if present.
    pub fn first(self) -> Option<&'a Line> {
        self.get(0)
    }
    /// Final row, if present.
    pub fn last(self) -> Option<&'a Line> {
        self.len().checked_sub(1).and_then(|index| self.get(index))
    }
    /// Iterate borrowed rows in display order.
    pub fn iter(self) -> LinesIter<'a> {
        LinesIter {
            buffer: self.buffer,
            range: self.start..self.end,
        }
    }
    /// Explicitly materialize owned row handles for a consumer snapshot.
    pub fn to_vec(self) -> Vec<Line> {
        self.iter().cloned().collect()
    }
}
impl Default for Lines<'_> {
    fn default() -> Self {
        static EMPTY: Buffer = Buffer {
            pages: Vec::new(),
            length: 0,
            capacity: 0,
        };
        Self {
            buffer: &EMPTY,
            start: 0,
            end: 0,
        }
    }
}
impl Index<usize> for Lines<'_> {
    type Output = Line;
    fn index(&self, index: usize) -> &Line {
        assert!(index < self.len(), "line index");
        &self.buffer[self.start + index]
    }
}
impl fmt::Debug for Lines<'_> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_list().entries(self.iter()).finish()
    }
}
impl PartialEq for Lines<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}
impl Eq for Lines<'_> {}
impl PartialEq<Vec<Line>> for Lines<'_> {
    fn eq(&self, other: &Vec<Line>) -> bool {
        self.iter().eq(other.iter())
    }
}
impl PartialEq<Lines<'_>> for Vec<Line> {
    fn eq(&self, other: &Lines<'_>) -> bool {
        self.iter().eq(other.iter())
    }
}
impl PartialEq<&Vec<Line>> for Lines<'_> {
    fn eq(&self, other: &&Vec<Line>) -> bool {
        self.iter().eq(other.iter())
    }
}
impl PartialEq<&[Line]> for Lines<'_> {
    fn eq(&self, other: &&[Line]) -> bool {
        self.iter().eq(other.iter())
    }
}
impl<'a> IntoIterator for Lines<'a> {
    type Item = &'a Line;
    type IntoIter = LinesIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
/// Exact-size iterator over borrowed immutable row contents.
#[derive(Clone)]
pub struct LinesIter<'a> {
    buffer: &'a Buffer,
    range: Range<usize>,
}
impl<'a> Iterator for LinesIter<'a> {
    type Item = &'a Line;
    fn next(&mut self) -> Option<Self::Item> {
        self.range.next().map(|index| &self.buffer[index])
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.range.nth(n).map(|index| &self.buffer[index])
    }
}
impl DoubleEndedIterator for LinesIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.range.next_back().map(|index| &self.buffer[index])
    }
    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        self.range.nth_back(n).map(|index| &self.buffer[index])
    }
}
impl ExactSizeIterator for LinesIter<'_> {}

impl FromIterator<Line> for Buffer {
    fn from_iter<I: IntoIterator<Item = Line>>(lines: I) -> Self {
        let mut buffer = Self::default();
        buffer.extend(lines);
        buffer
    }
}
impl PartialEq<Vec<Line>> for Buffer {
    fn eq(&self, other: &Vec<Line>) -> bool {
        self.iter().eq(other.iter())
    }
}
impl fmt::Debug for Buffer {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.all().fmt(out)
    }
}

#[cfg(test)]
thread_local! { pub(crate) static COPY_HISTORY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn nested_slices_match_slice_bounds(first in any::<usize>(), end in any::<usize>()) {
            let rows = vec![Line::default(); 9];
            let buffer: Buffer = rows.clone().into_iter().collect();
            let view = buffer.view(3..8).unwrap();
            let expected = rows.get(3..8).unwrap();
            prop_assert_eq!(view.slice(first..end).map(Lines::to_vec), expected.get(first..end).map(<[Line]>::to_vec));
            prop_assert_eq!(view.slice(first..).map(Lines::to_vec), expected.get(first..).map(<[Line]>::to_vec));
            prop_assert_eq!(view.slice(..=end).map(Lines::to_vec), expected.get(..=end).map(<[Line]>::to_vec));
        }
    }

    #[test]
    fn invalid_nested_slice_never_constructs_absolute_offsets() {
        let buffer: Buffer = vec![Line::default(); 2].into_iter().collect();
        let view = buffer.view(1..).unwrap();
        assert!(view.slice(usize::MAX..).is_none());
        let eager = std::panic::catch_unwind(|| {
            std::hint::black_box(
                false.then_some(view.start.checked_add(usize::MAX).expect("eager overflow")),
            );
        });
        assert!(eager.is_err());
    }

    #[test]
    fn appended_rows_keep_every_existing_address_and_snapshot() {
        let mut buffer = Buffer::default();
        let mut addresses = Vec::new();
        for index in 0..4096 {
            buffer.push(Line::plain(&index.to_string(), crate::Style::default()));
            addresses.push(std::ptr::from_ref(buffer.get(index).expect("existing row")));
        }
        for (index, address) in addresses.into_iter().enumerate() {
            assert_eq!(
                std::ptr::from_ref(buffer.get(index).expect("existing row")),
                address
            );
            assert_eq!(
                buffer
                    .get(index)
                    .expect("existing row")
                    .spans
                    .first()
                    .unwrap()
                    .text
                    .as_str(),
                index.to_string()
            );
        }
        let prefix = buffer.view(..64).unwrap().to_vec();
        buffer
            .get_mut(4095)
            .expect("last row")
            .spans
            .last_mut()
            .unwrap()
            .text
            .push_str(" changed");
        assert_eq!(buffer.view(..64).unwrap(), prefix);
    }
}
