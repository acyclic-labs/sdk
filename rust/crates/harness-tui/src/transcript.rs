//! Transcript rows and incremental display-width wrapping.
//!
//! A transcript holds the turns its view paged in from the conversation
//! stream, oldest first, and nothing else: no text is evicted or omitted.
//! Older turns page in before the first row, settled turns append, and the
//! streaming tail rewraps alone, so a delta costs the tail block's rows.

use crate::{
    Layout,
    config::{ThemeStyles, Token},
};
use ratatui::{
    style::{Modifier, Style},
    text::{Line, Span},
};
use std::{borrow::Cow, collections::VecDeque};

struct Block {
    prompt: bool,
    text: String,
}

/// The transcript shared by terminal applications: settled blocks, then the
/// transient pending prompts, then the streaming tail. Appending or
/// prepending wraps only the new blocks; resizing rewraps the settled ones.
pub struct TranscriptRows {
    blocks: VecDeque<Block>,
    lines: Vec<Line<'static>>,
    width: usize,
    theme: ThemeStyles,
    /// Rows of the pending prompts, after the settled rows.
    pending_rows: usize,
    /// Rows of the streaming tail, after the pending prompts.
    tail_rows: usize,
}

impl Default for TranscriptRows {
    fn default() -> Self {
        Self {
            blocks: VecDeque::new(),
            lines: Vec::new(),
            width: 80,
            theme: ThemeStyles::default(),
            pending_rows: 0,
            tail_rows: 0,
        }
    }
}

impl TranscriptRows {
    /// Append a settled prompt or answer after every settled row. The
    /// pending prompts and the tail are cleared; the caller restores them.
    pub fn push(&mut self, prompt: bool, text: &str) {
        self.clear_pending();
        self.push_owned(prompt, settled(text).to_owned());
    }

    fn push_owned(&mut self, prompt: bool, text: String) {
        self.lines
            .extend(wrap(prompt, &text, self.width.max(1), &self.theme));
        self.blocks.push_back(Block { prompt, text });
    }

    /// Insert older settled blocks, oldest first, before every row, and
    /// return how many rows they add. Rows counted from the end, as a
    /// scroll offset from the live tail is, keep their place.
    pub fn prepend(&mut self, blocks: impl IntoIterator<Item = (bool, String)>) -> usize {
        let width = self.width.max(1);
        let mut rows = Vec::new();
        let mut added = Vec::new();
        for (prompt, text) in blocks {
            let text = settled(&text).to_owned();
            rows.extend(wrap(prompt, &text, width, &self.theme));
            added.push(Block { prompt, text });
        }
        for block in added.into_iter().rev() {
            self.blocks.push_front(block);
        }
        let count = rows.len();
        self.lines.splice(0..0, rows);
        count
    }

    /// Wrapped rows for `width` terminal columns, including the prompt marker.
    /// After a width change, call `pending` and `tail` again to restore the
    /// transient rows.
    pub fn rows(&mut self, width: u16) -> &[Line<'static>] {
        let width = usize::from(width).max(1);
        if width != self.width {
            self.width = width;
            self.rewrap();
        }
        &self.lines
    }

    /// Show every block in `theme`'s colours. A changed theme rewraps the
    /// settled blocks; call `pending` and `tail` again to restore the
    /// transient rows.
    pub fn set_theme(&mut self, theme: ThemeStyles) {
        if theme != self.theme {
            self.theme = theme;
            self.rewrap();
        }
    }

    /// Remove every block, keeping the width and theme.
    pub fn clear(&mut self) {
        self.blocks.clear();
        self.lines.clear();
        self.pending_rows = 0;
        self.tail_rows = 0;
    }

    fn rewrap(&mut self) {
        self.pending_rows = 0;
        self.tail_rows = 0;
        self.lines.clear();
        for block in std::mem::take(&mut self.blocks) {
            self.push_owned(block.prompt, block.text);
        }
    }

    /// Replace the pending prompts, each a prompt (`true`) or an answer,
    /// without rewrapping settled text. The tail is cleared with them, since
    /// it follows them; restore it with [`Self::tail`].
    pub fn pending<'a>(&mut self, blocks: impl Iterator<Item = (bool, &'a str)>) {
        self.clear_pending();
        let width = self.width.max(1);
        for (prompt, text) in blocks {
            let rows = wrap(prompt, settled(text), width, &self.theme);
            self.pending_rows += rows.len();
            self.lines.extend(rows);
        }
    }

    /// Replace the streaming answer after the pending prompts, wrapping only
    /// it: a delta costs the tail's rows, whatever precedes it.
    pub fn tail(&mut self, text: Option<&str>) {
        self.lines
            .truncate(self.lines.len().saturating_sub(self.tail_rows));
        self.tail_rows = 0;
        if let Some(text) = text.filter(|text| !text.is_empty()) {
            let rows = wrap(false, settled(text), self.width.max(1), &self.theme);
            self.tail_rows = rows.len();
            self.lines.extend(rows);
        }
    }

    /// Search the display before an exclusive row index. Call again with the
    /// previous hit to search backward; an empty query has no hit.
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

    /// Display rows, including the transient prompts and tail.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lines.len()
    }

    /// Whether the display is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    fn clear_pending(&mut self) {
        self.lines.truncate(
            self.lines
                .len()
                .saturating_sub(self.pending_rows + self.tail_rows),
        );
        self.pending_rows = 0;
        self.tail_rows = 0;
    }
}

/// A block's shown text: its trailing newlines end it rather than add rows.
fn settled(text: &str) -> &str {
    text.trim_end_matches('\n')
}

/// A prompt is shown bold as typed after an accent marker; an answer is
/// rendered markdown.
fn wrap(prompt: bool, text: &str, width: usize, theme: &ThemeStyles) -> Vec<Line<'static>> {
    let marked = width > 2;
    let columns = if marked { width - 2 } else { width };
    let (text, styles) = if prompt {
        (
            Cow::Borrowed(text),
            vec![(0..text.len(), Style::default().add_modifier(Modifier::BOLD))],
        )
    } else {
        let (text, styles) = crate::markdown::render(text, columns, theme);
        (Cow::Owned(text), styles)
    };
    let mut style_at = crate::styled_cells::ordered_styles(styles);
    let layout = Layout::from_text(&text, columns);
    let mut rows = Vec::new();
    let mut previous_end = None;
    for (index, row) in layout.rows.into_iter().enumerate() {
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
            let mut style = style_at(row.start);
            if prompt && index == 0 {
                style = style.patch(theme.token(Token::Accent));
            }
            spans.push(Span::styled(marker, style));
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

    fn text(rows: &[Line<'static>]) -> Vec<String> {
        rows.iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect()
    }

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
    fn a_delta_rewraps_only_the_tail_and_older_pages_keep_the_tail_offset() {
        let mut transcript = TranscriptRows::default();
        transcript.rows(20);
        transcript.push(true, "settled prompt");
        transcript.pending([(true, "pending prompt")].into_iter());
        let settled = transcript.lines[0].spans.as_ptr();
        let pending = transcript.lines[transcript.len() - 2].spans.as_ptr();
        // Rows wrap by character; the tail fits the 18 text columns whole.
        for streamed in ["str", "str **answer**", "str **answer** grows"] {
            transcript.tail(Some(streamed));
            // The settled and pending rows are the same allocations.
            assert_eq!(transcript.lines[0].spans.as_ptr(), settled);
            assert_eq!(transcript.lines[2].spans.as_ptr(), pending);
        }
        let rows = text(transcript.rows(20));
        assert!(rows.iter().any(|row| row.contains("grows")));
        assert!(!rows.iter().any(|row| row.contains("**")));
        // Negative control: a cleared tail leaves only its predecessors.
        let before = transcript.len();
        transcript.tail(None);
        assert!(transcript.len() < before);
        assert!(
            !text(transcript.rows(20))
                .iter()
                .any(|row| row.contains("grows"))
        );

        // An older page goes first; the rows counted from the end stay put.
        transcript.tail(Some("tail"));
        let tail = text(transcript.rows(20));
        let added = transcript.prepend([(true, "older prompt".into()), (false, "older".into())]);
        let rows = text(transcript.rows(20));
        assert_eq!(rows.len(), tail.len() + added);
        assert_eq!(&rows[added..], &tail[..]);
        assert!(rows[0].starts_with("› older prompt"), "{rows:?}");
        // A resize rewraps settled blocks in order: the page, then the push.
        transcript.rows(40);
        let rows = text(transcript.rows(40));
        let older = rows.iter().position(|row| row.contains("older prompt"));
        let newer = rows.iter().position(|row| row.contains("settled prompt"));
        assert!(older < newer, "{rows:?}");
    }

    #[test]
    fn resize_moves_settled_sources_without_copying_them() {
        let mut transcript = TranscriptRows::default();
        for index in 0..130 {
            transcript.push(
                index % 2 == 0,
                &format!("source {index:03}: 界e\u{301}👩‍💻\nlast"),
            );
        }
        // Nothing is evicted, however many blocks or rows there are.
        assert_eq!(transcript.blocks.len(), 130);
        assert!(text(transcript.rows(80))[0].contains("source 000"));
        let sources = transcript
            .blocks
            .iter()
            .map(|block| block.text.as_ptr())
            .collect::<Vec<_>>();
        let original = transcript.lines.clone();
        // Negative control: transient text must be rebuilt at the new width,
        // rather than carried over with the immutable settled source.
        transcript.pending([(true, "transient text")].into_iter());
        transcript.tail(Some("streaming"));
        assert!(transcript.pending_rows > 0 && transcript.tail_rows > 0);
        for width in [47, 15, 3, 0, 80] {
            let rows = transcript.rows(width);
            assert!(
                rows.iter()
                    .all(|row| row.width() <= usize::from(width.max(1)))
            );
            assert_eq!((transcript.pending_rows, transcript.tail_rows), (0, 0));
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
        // A block of any size is shown whole: its last characters included.
        let source = format!("{}e\u{301}end", "界".repeat(64 * 1024));
        transcript.push(false, &source);
        let rows = text(transcript.rows(11));
        let shown = rows.iter().map(|row| row.trim_start()).collect::<String>();
        assert_eq!(shown.matches('界').count(), 64 * 1024 + 1);
        assert!(
            shown.ends_with("e\u{301}end"),
            "{}",
            &shown[shown.len() - 32..]
        );
        assert!(!shown.contains('…'));
    }
}
