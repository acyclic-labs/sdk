//! Bounded transcript storage and incremental display-width wrapping.

use crate::{Editor, Layout};
use ratatui::{
    style::{Modifier, Style},
    text::{Line, Span},
};
use std::collections::VecDeque;
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
        let text = bounded_text(text);
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
                self.push(block.prompt, &block.text);
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

fn bounded_text(text: &str) -> String {
    let text = text.trim_end_matches('\n');
    if text.len() <= MAX_BLOCK_BYTES {
        return text.to_owned();
    }
    let minimum = text.len() - (MAX_BLOCK_BYTES - OMITTED.len() - 1);
    let start = text
        .grapheme_indices(true)
        .find(|(at, _)| *at >= minimum)
        .map_or(text.len(), |(at, _)| at);
    format!("{OMITTED}\n{}", &text[start..])
}

/// A prompt is shown bold as typed; an answer is rendered markdown.
fn wrap(prompt: bool, text: &str, width: usize) -> Vec<Line<'static>> {
    let marked = width > 2;
    let (text, styles) = if prompt {
        (
            text.to_owned(),
            vec![(0..text.len(), Style::default().add_modifier(Modifier::BOLD))],
        )
    } else {
        crate::markdown::render(text)
    };
    let style_at = |offset: usize| {
        let at = styles.partition_point(|(range, _)| range.end <= offset);
        styles
            .get(at)
            .filter(|(range, _)| range.start <= offset)
            .map_or_else(Style::default, |(_, style)| *style)
    };
    let layout = Layout::new(&Editor::new(&text), if marked { width - 2 } else { width });
    let first = layout.rows.len().saturating_sub(MAX_ROWS - 2);
    let mut rows = Vec::new();
    if first > 0 {
        // The marker must itself fit even in a one-column text area.
        rows.push(Line::raw("…"));
    }
    for (index, row) in layout.rows.iter().enumerate().skip(first) {
        if row.cells.is_empty()
            && index
                .checked_sub(1)
                .and_then(|previous| layout.rows.get(previous))
                .is_some_and(|previous| previous.end == row.start)
        {
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
        let text = spans.len();
        for cell in &row.cells {
            let style = style_at(cell.source.start);
            let joinable = spans.len() > text;
            match spans.last_mut() {
                Some(span) if joinable && span.style == style => {
                    span.content.to_mut().push_str(&cell.symbol);
                }
                _ => spans.push(Span::styled(cell.symbol.clone(), style)),
            }
        }
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
        let text = bounded_text(&format!("{}e\u{301}end", "界".repeat(MAX_BLOCK_BYTES)));
        assert!(text.len() <= MAX_BLOCK_BYTES);
        assert!(text.ends_with("e\u{301}end"));
        assert!(text.starts_with(OMITTED));
    }
}
