use crate::WrappedLines;
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
        self.answer.rows().len() + self.tools.map_or(0, |tools| tools.rows().len())
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
            self.answer.index.revision,
            self.tools.map(|tools| tools.index.revision),
        )
    }

    fn separator(self) -> bool {
        !self.answer.rows().is_empty() && self.tools.is_some_and(|tools| !tools.rows().is_empty())
    }

    fn folded_offset(self) -> usize {
        self.answer.index.text.len() + usize::from(self.separator())
    }

    fn source_offset(self) -> usize {
        self.answer.text().len() + usize::from(self.separator())
    }

    pub(crate) fn folded_len(self) -> usize {
        self.folded_offset() + self.tools.map_or(0, |tools| tools.index.text.len())
    }

    pub(crate) fn folded_row(self, row: usize) -> Option<Range<usize>> {
        if row < self.answer.rows().len() {
            self.answer.folded_rows.get(row).cloned()
        } else {
            let range = self
                .tools?
                .folded_rows
                .get(row - self.answer.rows().len())?;
            Some(self.folded_offset() + range.start..self.folded_offset() + range.end)
        }
    }

    pub(crate) fn source_row(self, at: usize) -> Option<usize> {
        if !self.answer.rows().is_empty() && at <= self.answer.text().len() {
            self.answer.source_rows.get(at).copied()
        } else {
            self.tools?
                .source_rows
                .get(at.checked_sub(self.source_offset())?)
                .map(|row| self.answer.rows().len() + row)
        }
    }

    fn origin(self, at: usize) -> Range<usize> {
        let end = self.answer.index.text.len();
        if at < end {
            self.answer.index.source(at, 1)
        } else if self.separator() && at == end {
            self.answer.text().len()..self.source_offset()
        } else {
            assert!(self.tools.is_some(), "tool source");
            let range = self.tools.map_or(0..0, |tools| {
                tools.index.source(at - self.folded_offset(), 1)
            });
            self.source_offset() + range.start..self.source_offset() + range.end
        }
    }

    pub(crate) fn source(self, at: usize, bytes: usize) -> Range<usize> {
        self.origin(at).start..self.origin(at + bytes - 1).end
    }

    pub(crate) fn slice(self, range: Range<usize>) -> Option<&'a [u8]> {
        if range.end <= self.answer.index.text.len() {
            self.answer.index.text.as_bytes().get(range)
        } else if range.start >= self.folded_offset() {
            self.tools?
                .index
                .text
                .as_bytes()
                .get(range.start - self.folded_offset()..range.end - self.folded_offset())
        } else if self.separator() && range == (self.answer.index.text.len()..self.folded_offset())
        {
            Some(b"\n")
        } else {
            None
        }
    }

    pub(crate) fn bytes(self, range: Range<usize>) -> impl Iterator<Item = u8> + 'a {
        self.answer
            .index
            .text
            .bytes()
            .chain(self.separator().then_some(b'\n'))
            .chain(
                self.tools
                    .map_or("", |tools| tools.index.text.as_str())
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
        if row < self.answer.rows().len() {
            self.answer.ranges().get(row).cloned().unwrap_or(0..0)
        } else {
            self.tools
                .and_then(|tools| tools.ranges().get(row - self.answer.rows().len()))
                .map_or(0..0, |range| {
                    self.source_offset() + range.start..self.source_offset() + range.end
                })
        }
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
        let (source, row, offset) = if row < self.answer.rows().len() {
            (self.answer, row, 0)
        } else {
            let Some(tools) = self.tools else {
                return;
            };
            (tools, row - self.answer.rows().len(), self.source_offset())
        };
        source.render_ranges((row, offset), matches, selected, style, area, buffer);
    }
}
