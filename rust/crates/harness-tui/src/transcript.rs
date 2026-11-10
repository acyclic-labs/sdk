//! Transcript rows and incremental display-width wrapping.
//!
//! A transcript holds the turns its view paged in from the conversation
//! stream, oldest first, and nothing else: no text is evicted or omitted.
//! Older turns page in before the first row, settled turns append, and the
//! streaming tail rewraps alone, so a delta costs the tail block's rows.
//! A turn's tool calls show as collapsed cards, one muted row each.

use crate::{
    Layout,
    config::{ThemeStyles, Token},
};
use ratatui::{
    style::{Modifier, Style},
    text::{Line, Span},
};
use std::{borrow::Cow, collections::VecDeque};
use unicode_width::UnicodeWidthStr;

#[cfg(test)]
thread_local! { pub(crate) static WRAP_BYTES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

/// What a transcript block is, and so how it shows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockKind {
    /// A prompt: bold, as typed, after the accent marker.
    Prompt,
    /// An answer or notice: rendered markdown.
    Answer,
    /// A collapsed tool card: one muted row, the call's first line clipped
    /// to the width, then its outcome.
    Tool {
        /// The call failed or was refused.
        failed: bool,
    },
}

struct Block {
    kind: BlockKind,
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
    /// Append a settled block after every settled row. The pending prompts
    /// and the tail are cleared; the caller restores them.
    pub fn push(&mut self, kind: BlockKind, text: &str) {
        self.clear_pending();
        self.push_owned(kind, settled(text).to_owned());
    }

    fn push_owned(&mut self, kind: BlockKind, text: String) {
        self.lines
            .extend(wrap(kind, &text, self.width.max(1), &self.theme));
        self.blocks.push_back(Block { kind, text });
    }

    /// Insert older settled blocks, oldest first, before every row, and
    /// return how many rows they add. Rows counted from the end, as a
    /// scroll offset from the live tail is, keep their place.
    pub fn prepend(&mut self, blocks: impl IntoIterator<Item = (BlockKind, String)>) -> usize {
        let width = self.width.max(1);
        let mut rows = Vec::new();
        let mut added = Vec::new();
        for (kind, text) in blocks {
            let text = settled(&text).to_owned();
            rows.extend(wrap(kind, &text, width, &self.theme));
            added.push(Block { kind, text });
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
            self.push_owned(block.kind, block.text);
        }
    }

    /// Replace the pending prompts without rewrapping settled text. The tail
    /// is cleared with them, since it follows them; restore it with
    /// [`Self::tail`].
    pub fn pending<'a>(&mut self, prompts: impl Iterator<Item = &'a str>) {
        self.clear_pending();
        let width = self.width.max(1);
        for text in prompts {
            let rows = wrap(BlockKind::Prompt, settled(text), width, &self.theme);
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
            let rows = wrap(
                BlockKind::Answer,
                settled(text),
                self.width.max(1),
                &self.theme,
            );
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
/// rendered markdown; a tool card is one row.
fn wrap(kind: BlockKind, text: &str, width: usize, theme: &ThemeStyles) -> Vec<Line<'static>> {
    #[cfg(test)]
    WRAP_BYTES.with(|work| work.set(work.get() + text.len()));
    let prompt = match kind {
        BlockKind::Prompt => true,
        BlockKind::Answer => false,
        BlockKind::Tool { failed } => return vec![card(text, failed, width, theme)],
    };
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

/// A collapsed tool card's one row: a muted marker and the call's first
/// line, clipped with an ellipsis so its outcome glyph always shows.
fn card(text: &str, failed: bool, width: usize, theme: &ThemeStyles) -> Line<'static> {
    const MARKER: &str = "\u{23fa} ";
    let (glyph, outcome) = if failed {
        (" \u{2717}", Token::Error)
    } else {
        (" \u{2713}", Token::Success)
    };
    let muted = theme.token(Token::Muted);
    let line = text.lines().next().unwrap_or_default();
    let fixed = MARKER.width() + glyph.width();
    // Too narrow for the marker and glyph: the clipped text alone.
    let framed = width > fixed;
    let columns = if framed { width - fixed } else { width.max(1) };
    let first = |columns: usize| {
        Layout::from_text(line, columns)
            .rows
            .into_iter()
            .next()
            .unwrap_or_default()
            .cells
    };
    let clipped = Layout::from_text(line, columns).rows.len() > 1;
    let mut shown = first(if clipped { columns - 1 } else { columns }.max(1))
        .into_iter()
        .map(|cell| cell.symbol)
        .collect::<String>();
    if clipped && columns > 1 {
        shown.push('\u{2026}');
    }
    let mut spans = Vec::with_capacity(3);
    if framed {
        spans.push(Span::styled(MARKER, muted));
    }
    spans.push(Span::styled(shown, muted));
    if framed {
        spans.push(Span::styled(glyph, theme.token(outcome)));
    }
    Line::from(spans)
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
        transcript.push(BlockKind::Prompt, "retained prompt");
        let allocation = transcript.lines[0].spans[0].content.as_ptr();
        transcript.push(BlockKind::Answer, "answer");
        assert_eq!(transcript.rows(20)[0].spans[0].content.as_ptr(), allocation);
        assert_eq!(transcript.find("prompt", transcript.len()), Some(0));
        transcript.pending(["pending"].into_iter());
        transcript.pending(["replacement"].into_iter());
        assert!(transcript.find("replacement", transcript.len()).is_some());
        assert_eq!(transcript.find("pending", transcript.len()), None);
        assert_eq!(transcript.rows(20)[0].spans[0].content.as_ptr(), allocation);
    }

    #[test]
    fn a_delta_rewraps_only_the_tail_and_older_pages_keep_the_tail_offset() {
        let mut transcript = TranscriptRows::default();
        transcript.rows(20);
        transcript.push(BlockKind::Prompt, "settled prompt");
        transcript.pending(["pending prompt"].into_iter());
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
        let added = transcript.prepend([
            (BlockKind::Prompt, "older prompt".into()),
            (BlockKind::Answer, "older".into()),
        ]);
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
            let kind = match index % 3 {
                0 => BlockKind::Prompt,
                1 => BlockKind::Tool { failed: false },
                _ => BlockKind::Answer,
            };
            transcript.push(kind, &format!("source {index:03}: 界e\u{301}👩‍💻\nlast"));
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
        transcript.pending(["transient text"].into_iter());
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
    fn a_tool_card_is_one_row_whose_outcome_survives_clipping() {
        let mut transcript = TranscriptRows::default();
        let call = "shell  cargo test -p foo --all-features\nsecond line";
        transcript.push(BlockKind::Tool { failed: false }, call);
        transcript.push(BlockKind::Tool { failed: true }, "read  missing.txt");
        for width in [80, 24, 9] {
            let rows = text(transcript.rows(width));
            assert_eq!(rows.len(), 2, "{rows:?}");
            assert!(rows.iter().all(|row| row.width() <= usize::from(width)));
            assert!(rows[0].starts_with('\u{23fa}') && rows[0].ends_with('\u{2713}'));
            assert!(rows[1].ends_with('\u{2717}'), "{rows:?}");
            assert!(!rows[0].contains("second line"));
        }
        let wide = text(transcript.rows(80));
        assert!(wide[0].contains("--all-features") && !wide[0].contains('\u{2026}'));
        // Negative control: a narrow card is clipped, never wrapped.
        let narrow = text(transcript.rows(24));
        assert!(narrow[0].contains('\u{2026}') && !narrow[0].contains("--all-features"));
    }

    #[test]
    fn display_width_and_graphemes_survive_wrapping() {
        let mut transcript = TranscriptRows::default();
        transcript.push(BlockKind::Prompt, "界e\u{301}👩‍💻\ttext\nlast");
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
        transcript.push(BlockKind::Answer, &source);
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
