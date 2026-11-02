//! Bounded transcript storage and incremental display-width wrapping.

use crate::Layout;
use ratatui::{
    style::{Modifier, Style},
    text::{Line, Span},
};
use std::{borrow::Cow, collections::VecDeque};
use unicode_segmentation::UnicodeSegmentation;

const MAX_BLOCKS: usize = 128;
const MAX_BYTES: usize = 1024 * 1024;
const MAX_BLOCK_BYTES: usize = 64 * 1024;
const MAX_ROWS: usize = 8192;
const OMITTED: &str = "… earlier text omitted …";

struct Block {
    prompt: bool,
    text: String,
    rows: usize,
}

/// One bounded transcript shared by terminal applications. Appending wraps only
/// the new block; resizing rewraps the retained window. Older text is evicted.
pub struct TranscriptRows {
    blocks: VecDeque<Block>,
    lines: Vec<Line<'static>>,
    bytes: usize,
    width: usize,
    pending_rows: usize,
    truncated: bool,
}

impl Default for TranscriptRows {
    fn default() -> Self {
        Self {
            blocks: VecDeque::new(),
            lines: Vec::new(),
            bytes: 0,
            width: 80,
            pending_rows: 0,
            truncated: false,
        }
    }
}

impl TranscriptRows {
    /// Append a settled prompt or answer, retaining a bounded suffix of a large
    /// block. Truncation is shown explicitly in the transcript.
    pub fn push(&mut self, prompt: bool, text: &str) {
        self.clear_pending();
        self.truncated |= text.len() > MAX_BLOCK_BYTES;
        self.push_owned(prompt, bounded_text(text).into_owned());
    }

    fn push_owned(&mut self, prompt: bool, text: String) {
        self.bytes += text.len();
        let rows = wrap(prompt, &text, self.width.max(1));
        self.blocks.push_back(Block {
            prompt,
            text,
            rows: rows.len(),
        });
        self.lines.extend(rows);
        self.evict();
    }

    /// Wrapped rows for `width` terminal columns, including the prompt marker.
    /// Call `pending` again after a width change to restore transient prompts.
    pub fn rows(&mut self, width: u16) -> &[Line<'static>] {
        let width = usize::from(width).max(1);
        if width != self.width {
            self.width = width;
            self.pending_rows = 0;
            self.lines.clear();
            self.bytes = 0;
            for block in std::mem::take(&mut self.blocks) {
                self.push_owned(block.prompt, block.text);
            }
        }
        &self.lines
    }

    /// Replace the transient suffix, each block a prompt (`true`) or an
    /// answer, without rewrapping settled text. Display budgets do not change
    /// the caller's pending operation identities.
    pub fn pending<'a>(&mut self, blocks: impl DoubleEndedIterator<Item = (bool, &'a str)>) {
        self.clear_pending();
        let mut rows = VecDeque::new();
        for (prompt, text) in blocks.rev().take(MAX_BLOCKS) {
            let wrapped = wrap(prompt, &bounded_text(text), self.width.max(1));
            for row in wrapped.into_iter().rev() {
                if rows.len() == MAX_ROWS {
                    break;
                }
                rows.push_front(row);
            }
            if rows.len() == MAX_ROWS {
                break;
            }
        }
        self.pending_rows = rows.len();
        self.lines.extend(rows);
    }

    /// Search the retained display before an exclusive row index. Call again
    /// with the previous hit to search backward; an empty query has no hit.
    #[must_use]
    pub fn find(&self, query: &str, before: usize) -> Option<usize> {
        if query.is_empty() {
            return None;
        }
        self.lines[..before.min(self.lines.len())]
            .iter()
            .rposition(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
                    .contains(query)
            })
    }

    /// Retained display rows, including transient prompts.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lines.len()
    }

    /// Whether the display is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    /// Whether settled source text was omitted from this retained window.
    #[must_use]
    pub fn truncated(&self) -> bool {
        self.truncated
    }

    fn clear_pending(&mut self) {
        self.lines
            .truncate(self.lines.len().saturating_sub(self.pending_rows));
        self.pending_rows = 0;
    }

    fn evict(&mut self) {
        let mut removed = 0;
        while self.blocks.len() > MAX_BLOCKS
            || self.bytes > MAX_BYTES
            || self.lines.len().saturating_sub(removed) > MAX_ROWS
        {
            let Some(block) = self.blocks.pop_front() else {
                break;
            };
            self.bytes -= block.text.len();
            removed += block.rows;
        }
        if removed > 0 {
            self.truncated = true;
            self.lines.drain(..removed);
        }
    }
}

fn bounded_text(text: &str) -> Cow<'_, str> {
    let text = text.trim_end_matches('\n');
    if text.len() <= MAX_BLOCK_BYTES {
        return Cow::Borrowed(text);
    }
    let minimum = text.len() - (MAX_BLOCK_BYTES - OMITTED.len() - 1);
    let start = text
        .grapheme_indices(true)
        .find(|(at, _)| *at >= minimum)
        .map_or(text.len(), |(at, _)| at);
    Cow::Owned(format!("{OMITTED}\n{}", &text[start..]))
}

/// A prompt is shown bold as typed; an answer is rendered markdown.
fn wrap(prompt: bool, text: &str, width: usize) -> Vec<Line<'static>> {
    let marked = width > 2;
    let columns = if marked { width - 2 } else { width };
    let (text, styles) = if prompt {
        (
            Cow::Borrowed(text),
            vec![(0..text.len(), Style::default().add_modifier(Modifier::BOLD))],
        )
    } else {
        let (text, styles) = crate::markdown::render(text, columns);
        (Cow::Owned(text), styles)
    };
    let mut style_at = crate::styled_cells::ordered_styles(styles);
    let layout = Layout::from_text(&text, columns);
    let first = layout.rows.len().saturating_sub(MAX_ROWS - 2);
    let mut rows = Vec::new();
    if first > 0 {
        // The marker must itself fit even in a one-column text area.
        rows.push(Line::raw("…"));
    }
    let mut previous_end = first
        .checked_sub(1)
        .map(|previous| layout.rows[previous].end);
    for (index, row) in layout.rows.into_iter().enumerate().skip(first) {
        let joins_previous = previous_end.replace(row.end) == Some(row.start);
        if row.cells.is_empty() && joins_previous {
            continue;
        }
        let marker = if !marked {
            ""
        } else if prompt && index == 0 {
            "› "
        } else {
            "  "
        };
        let mut spans: Vec<Span<'static>> = Vec::new();
        if !marker.is_empty() {
            spans.push(Span::styled(marker, style_at(row.start)));
        }
        // The marker span stays its own, borrowed; text cells of one style join.
        crate::styled_cells::append(
            &mut spans,
            row.cells
                .into_iter()
                .map(|cell| (cell.source.start, cell.symbol)),
            &mut style_at,
        );
        rows.push(Line::from(spans));
    }
    rows.push(Line::default());
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_keeps_existing_rows_and_status_work_needs_no_wrap() {
        let mut transcript = TranscriptRows::default();
        transcript.rows(20);
        transcript.push(true, "retained prompt");
        let allocation = transcript.lines[0].spans[0].content.as_ptr();
        transcript.push(false, "answer");
        assert_eq!(transcript.rows(20)[0].spans[0].content.as_ptr(), allocation);
        assert_eq!(transcript.find("prompt", transcript.len()), Some(0));
        transcript.pending([(true, "pending")].into_iter());
        transcript.pending([(false, "**replacement**")].into_iter());
        assert_eq!(transcript.find("**", transcript.len()), None);
        assert_eq!(transcript.find("pending", transcript.len()), None);
        assert_eq!(transcript.rows(20)[0].spans[0].content.as_ptr(), allocation);
    }

    #[test]
    fn retained_source_and_rows_stay_bounded_under_append_and_resize() {
        let mut transcript = TranscriptRows::default();
        for _ in 0..MAX_BLOCKS + 10 {
            transcript.push(false, &"界".repeat(MAX_BLOCK_BYTES));
            assert!(transcript.bytes <= MAX_BYTES);
            assert!(transcript.blocks.len() <= MAX_BLOCKS);
            assert!(transcript.lines.len() <= MAX_ROWS);
        }
        for width in [1, 2, 3, 12, 80] {
            let rows = transcript.rows(width);
            assert!(rows.len() <= MAX_ROWS);
            assert!(rows.iter().all(|row| row.width() <= usize::from(width)));
        }
        assert!(transcript.truncated());
    }

    #[test]
    fn resize_moves_retained_sources_and_preserves_the_bounded_projection() {
        let mut transcript = TranscriptRows::default();
        for index in 0..MAX_BLOCKS + 2 {
            transcript.push(
                index % 2 == 0,
                &format!("source {index:03}: 界e\u{301}👩‍💻\nlast"),
            );
        }
        let sources = transcript
            .blocks
            .iter()
            .map(|block| block.text.as_ptr())
            .collect::<Vec<_>>();
        let bytes = transcript.bytes;
        let original = transcript.lines.clone();
        // Negative control: transient text must be rebuilt at the new width,
        // rather than carried over with the immutable settled source.
        transcript.pending([(true, "transient text")].into_iter());
        assert!(transcript.pending_rows > 0);
        for width in [47, 15, 3, 0, 80] {
            let rows = transcript.rows(width);
            assert!(rows.len() <= MAX_ROWS);
            assert!(
                rows.iter()
                    .all(|row| row.width() <= usize::from(width.max(1)))
            );
            assert_eq!(transcript.pending_rows, 0);
            assert_eq!(transcript.bytes, bytes);
            assert_eq!(
                transcript
                    .blocks
                    .iter()
                    .map(|block| block.text.as_ptr())
                    .collect::<Vec<_>>(),
                sources,
                "resize copied settled source text"
            );
        }
        assert_eq!(transcript.lines, original);
        let allocation = transcript.lines[0].spans.as_ptr();
        transcript.rows(80);
        assert_eq!(transcript.lines[0].spans.as_ptr(), allocation);

        // A narrow projection may evict older display blocks, but it must
        // retain the original newest source without exceeding row credit.
        transcript.push(false, &"界".repeat(MAX_ROWS - 4));
        let newest = transcript.blocks.back().unwrap().text.as_ptr();
        assert!(transcript.rows(1).len() <= MAX_ROWS);
        assert_eq!(transcript.blocks.len(), 1);
        assert_eq!(transcript.blocks.back().unwrap().text.as_ptr(), newest);
        assert!(transcript.bytes <= MAX_BYTES);
        assert!(transcript.truncated());
    }

    #[test]
    fn display_width_and_graphemes_survive_wrapping() {
        let mut transcript = TranscriptRows::default();
        transcript.push(true, "界e\u{301}👩‍💻\ttext\nlast");
        for width in 1..12 {
            assert!(
                transcript
                    .rows(width)
                    .iter()
                    .all(|line| line.width() <= usize::from(width))
            );
        }
        let source = format!("{}e\u{301}end", "界".repeat(MAX_BLOCK_BYTES));
        let text = bounded_text(&source);
        assert!(text.len() <= MAX_BLOCK_BYTES);
        assert!(text.ends_with("e\u{301}end"));
        assert!(text.starts_with(OMITTED));
    }
}
