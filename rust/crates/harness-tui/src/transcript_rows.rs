use crate::{RowWindow, TranscriptAnchor, TranscriptPart, WrappedLines};
use ratatui::{buffer::Buffer, layout::Rect, style::Style};
use std::ops::Range;

/// Borrow the rendered answer and tool rows, preserving their independent caches.
#[derive(Clone, Copy)]
pub struct TranscriptRows<'a> {
    answer: &'a WrappedLines,
    tools: Option<&'a WrappedLines>,
}

#[cfg(test)]
mod tests;

impl<'a> From<&'a WrappedLines> for TranscriptRows<'a> {
    fn from(answer: &'a WrappedLines) -> Self {
        Self {
            answer,
            tools: None,
        }
    }
}

impl<'a> TranscriptRows<'a> {
    /// The same row order used for painting and match navigation.
    pub fn new(answer: &'a WrappedLines, tools: &'a WrappedLines) -> Self {
        Self {
            answer,
            tools: Some(tools),
        }
    }

    /// Number of displayed rows, without copying either cache.
    pub fn len(self) -> usize {
        self.answer.shown().len() + self.tools.map_or(0, |tools| tools.shown().len())
    }

    /// Whether there are any displayed rows.
    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    pub(crate) fn revision(self) -> (u64, Option<u64>) {
        (
            self.answer.revision(),
            self.tools.map(WrappedLines::revision),
        )
    }

    pub(crate) fn index_revision(self) -> (u64, Option<u64>) {
        (
            self.answer.source_revision(),
            self.tools.map(|tools| tools.source_revision()),
        )
    }

    fn separator(self) -> bool {
        self.answer.source_len() != 0 && self.tools.is_some_and(|tools| tools.source_len() != 0)
    }

    fn folded_offset(self) -> usize {
        self.answer.search_index().text.len() + usize::from(self.separator())
    }

    fn source_offset(self) -> usize {
        self.answer.text().len() + usize::from(self.separator())
    }

    pub(crate) fn folded_len(self) -> usize {
        self.folded_offset()
            + self
                .tools
                .map_or(0, |tools| tools.search_index().text.len())
    }

    pub(crate) fn windows(self) -> (RowWindow, Option<RowWindow>) {
        (self.answer.window(), self.tools.map(WrappedLines::window))
    }

    fn part(self, row: usize) -> Option<(&'a WrappedLines, usize, usize)> {
        if row < self.answer.shown().len() {
            Some((self.answer, self.answer.window().start + row, 0))
        } else {
            let tools = self.tools?;
            let row = row - self.answer.shown().len();
            (row < tools.shown().len())
                .then(|| (tools, tools.window().start + row, self.source_offset()))
        }
    }

    pub(crate) fn folded_row(self, row: usize) -> Option<Range<usize>> {
        let (source, row, offset) = self.part(row)?;
        let range = source
            .search_index()
            .folded_range(source.ranges().get(row)?);
        let offset = if offset == 0 { 0 } else { self.folded_offset() };
        Some(offset + range.start..offset + range.end)
    }

    pub(crate) fn source_anchor(self, at: usize) -> Option<TranscriptAnchor> {
        let (source, part, at) = if self.answer.source_len() != 0 && at <= self.answer.text().len()
        {
            (self.answer, TranscriptPart::Answer, at)
        } else {
            (
                self.tools?,
                TranscriptPart::Tools,
                at.checked_sub(self.source_offset())?,
            )
        };
        Some(TranscriptAnchor {
            origin: None,
            part,
            position: source.position_of(at),
        })
    }

    pub(crate) fn folded_at_source(self, at: usize) -> Option<Range<usize>> {
        let (source, at, offset) =
            if self.answer.source_len() != 0 && at <= self.answer.text().len() {
                (self.answer, at, 0)
            } else {
                (
                    self.tools?,
                    at.checked_sub(self.source_offset())?,
                    self.folded_offset(),
                )
            };
        let range = source.search_index().folded_range(&source.row_range_at(at));
        Some(offset + range.start..offset + range.end)
    }

    fn origin(self, at: usize) -> Range<usize> {
        let end = self.answer.search_index().text.len();
        if at < end {
            self.answer.search_index().source(at, 1)
        } else if self.separator() && at == end {
            self.answer.text().len()..self.source_offset()
        } else {
            assert!(self.tools.is_some(), "tool source");
            let range = self.tools.map_or(0..0, |tools| {
                tools.search_index().source(at - self.folded_offset(), 1)
            });
            self.source_offset() + range.start..self.source_offset() + range.end
        }
    }

    pub(crate) fn source(self, at: usize, bytes: usize) -> Range<usize> {
        self.origin(at).start..self.origin(at + bytes - 1).end
    }

    pub(crate) fn slice(self, range: Range<usize>) -> Option<&'a [u8]> {
        if range.end <= self.answer.search_index().text.len() {
            self.answer.search_index().text.as_bytes().get(range)
        } else if range.start >= self.folded_offset() {
            self.tools?
                .search_index()
                .text
                .as_bytes()
                .get(range.start - self.folded_offset()..range.end - self.folded_offset())
        } else if self.separator()
            && range == (self.answer.search_index().text.len()..self.folded_offset())
        {
            Some(b"\n")
        } else {
            None
        }
    }

    pub(crate) fn bytes(self, range: Range<usize>) -> impl Iterator<Item = u8> + 'a {
        self.answer
            .search_index()
            .text
            .bytes()
            .chain(self.separator().then_some(b'\n'))
            .chain(
                self.tools
                    .map_or("", |tools| tools.search_index().text.as_str())
                    .bytes(),
            )
            .skip(range.start)
            .take(range.end - range.start)
    }

    pub(crate) fn matches(self, range: Range<usize>, bytes: &[u8]) -> bool {
        self.slice(range.clone()).map_or_else(
            || self.bytes(range).eq(bytes.iter().copied()),
            |slice| slice == bytes,
        )
    }

    pub(crate) fn range(self, row: usize) -> Range<usize> {
        let Some((source, row, offset)) = self.part(row) else {
            return 0..0;
        };
        let Some(range) = source.ranges().get(row) else {
            return 0..0;
        };
        offset + range.start..offset + range.end
    }

    pub(crate) fn render(
        self,
        row: usize,
        matches: &[Range<usize>],
        selected: Option<&Range<usize>>,
        style: Style,
        area: Rect,
        buffer: &mut Buffer,
    ) {
        let Some((source, row, offset)) = self.part(row) else {
            return;
        };
        source.render_ranges((row, offset), matches, selected, style, area, buffer);
    }
}
