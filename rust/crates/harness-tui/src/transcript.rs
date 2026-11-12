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
use std::collections::VecDeque;
use unicode_width::UnicodeWidthStr;

#[cfg(test)]
thread_local! { pub(crate) static WRAP_BYTES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

/// What a transcript block is, and so how it shows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockKind {
    /// A prompt: bold, as typed, after the accent marker.
    Prompt,
    /// Attributed input from another agent, displayed literally with a peer marker.
    Peer,
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
    parsed: Option<(usize, acyclic_markdown::Markdown)>,
}

impl Block {
    fn wrap(
        &mut self,
        width: usize,
        theme: &ThemeStyles,
        cache: &acyclic_markdown::CodeCache,
        tool_details: bool,
    ) -> Wrapped {
        if matches!(self.kind, BlockKind::Tool { .. }) {
            let mut header = wrap(self.kind, &self.text, width, theme);
            if tool_details && let Some((_, source)) = self.text.split_once('\n') {
                let mut body =
                    wrap_rendered(self.kind, literal(source, Style::default()), width, theme);
                header.lines.append(&mut body.lines);
                header.links.append(&mut body.links);
                header.code.append(&mut body.code);
            }
            return header;
        }
        let source = if self.kind == BlockKind::Answer {
            Some(self.text.as_str())
        } else {
            None
        };
        if let Some(source) = source {
            if self.parsed.is_none() {
                let mut parser = acyclic_markdown::Markdown::with_deferred_code_in(
                    u16::try_from(width.saturating_sub(1).max(1)).unwrap_or(u16::MAX),
                    cache.clone(),
                );
                parser.push(source);
                self.parsed = Some((width, parser));
            }
            if let Some((columns, parser)) = &mut self.parsed {
                if *columns != width {
                    parser.set_width(
                        u16::try_from(width.saturating_sub(1).max(1)).unwrap_or(u16::MAX),
                    );
                    *columns = width;
                }
            }
            let parsed = self
                .parsed
                .as_ref()
                .map(|(_, parser)| parser.lines())
                .unwrap_or_default();
            return wrap_rendered(
                self.kind,
                crate::markdown::render_lines(parsed, theme),
                width,
                theme,
            );
        }
        wrap(self.kind, &self.text, width, theme)
    }
}

/// The transcript shared by terminal applications: settled blocks, then the
/// transient pending prompts, then the streaming tail. Appending or
/// prepending wraps only the new blocks; resizing rewraps the settled ones.
pub struct TranscriptRows {
    blocks: VecDeque<Block>,
    lines: Vec<Line<'static>>,
    links: Vec<Vec<crate::RowLink>>,
    code: Vec<Option<crate::CodeRow>>,
    width: usize,
    theme: ThemeStyles,
    /// Rows of the pending prompts, after the settled rows.
    pending_rows: usize,
    /// Rows of the streaming tail, after the pending prompts.
    tail_rows: usize,
    tail_parser: Option<(usize, acyclic_markdown::Markdown)>,
    code_cache: acyclic_markdown::CodeCache,
    tool_details: bool,
}

impl Default for TranscriptRows {
    fn default() -> Self {
        Self {
            blocks: VecDeque::new(),
            lines: Vec::new(),
            links: Vec::new(),
            code: Vec::new(),
            width: 80,
            theme: ThemeStyles::default(),
            pending_rows: 0,
            tail_rows: 0,
            tail_parser: None,
            code_cache: acyclic_markdown::CodeCache::default(),
            tool_details: false,
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
        let mut block = Block {
            kind,
            text,
            parsed: None,
        };
        let wrapped = block.wrap(
            self.width.max(1),
            &self.theme,
            &self.code_cache,
            self.tool_details,
        );
        self.lines.extend(wrapped.lines);
        self.links.extend(wrapped.links);
        self.code.extend(wrapped.code);
        self.blocks.push_back(block);
    }

    /// Insert older settled blocks, oldest first, before every row, and
    /// return how many rows they add. Rows counted from the end, as a
    /// scroll offset from the live tail is, keep their place.
    pub fn prepend(&mut self, blocks: impl IntoIterator<Item = (BlockKind, String)>) -> usize {
        let width = self.width.max(1);
        let mut rows = Vec::new();
        let mut links = Vec::new();
        let mut code = Vec::new();
        let mut added = Vec::new();
        for (kind, text) in blocks {
            let text = settled(&text).to_owned();
            let mut block = Block {
                kind,
                text,
                parsed: None,
            };
            let wrapped = block.wrap(width, &self.theme, &self.code_cache, self.tool_details);
            rows.extend(wrapped.lines);
            links.extend(wrapped.links);
            code.extend(wrapped.code);
            added.push(block);
        }
        for block in added.into_iter().rev() {
            self.blocks.push_front(block);
        }
        let count = rows.len();
        self.lines.splice(0..0, rows);
        self.links.splice(0..0, links);
        self.code.splice(0..0, code);
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

    /// Fold or expand the original tool results. Only changing the setting
    /// reflows settled rows; ordinary frames and token deltas keep their cache.
    pub fn set_tool_details(&mut self, expanded: bool) {
        if self.tool_details != expanded {
            self.tool_details = expanded;
            self.rewrap();
        }
    }

    /// The already laid out rows and their link metadata, borrowed together.
    /// Call `rows` first when changing width; no rendering or I/O happens here.
    pub fn presentation(
        &self,
    ) -> (
        &[Line<'static>],
        &[Vec<crate::RowLink>],
        &[Option<crate::CodeRow>],
    ) {
        (&self.lines, &self.links, &self.code)
    }

    /// Remove every block, keeping the width and theme.
    pub fn clear(&mut self) {
        self.blocks.clear();
        self.lines.clear();
        self.links.clear();
        self.code.clear();
        self.pending_rows = 0;
        self.tail_rows = 0;
        self.tail_parser = None;
        self.code_cache = acyclic_markdown::CodeCache::default();
    }

    fn rewrap(&mut self) {
        self.pending_rows = 0;
        self.tail_rows = 0;
        self.lines.clear();
        self.links.clear();
        self.code.clear();
        for block in &mut self.blocks {
            let wrapped = block.wrap(
                self.width.max(1),
                &self.theme,
                &self.code_cache,
                self.tool_details,
            );
            self.lines.extend(wrapped.lines);
            self.links.extend(wrapped.links);
            self.code.extend(wrapped.code);
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
            self.pending_rows += rows.lines.len();
            self.lines.extend(rows.lines);
            self.links.extend(rows.links);
            self.code.extend(rows.code);
        }
    }

    /// Replace the streaming answer after the pending prompts, wrapping only
    /// it: a delta costs the tail's rows, whatever precedes it.
    pub fn tail(&mut self, text: Option<&str>) {
        self.lines
            .truncate(self.lines.len().saturating_sub(self.tail_rows));
        self.links.truncate(self.lines.len());
        self.code.truncate(self.lines.len());
        self.tail_rows = 0;
        if let Some(text) = text.filter(|text| !text.is_empty()) {
            let width = self.width.max(1);
            let text = settled(text);
            if let Some((columns, parser)) = &mut self.tail_parser {
                if *columns != width {
                    parser.set_width(
                        u16::try_from(width.saturating_sub(1).max(1)).unwrap_or(u16::MAX),
                    );
                    *columns = width;
                }
            }
            if self.tail_parser.as_ref().is_none_or(|(columns, parser)| {
                *columns != width || !text.starts_with(parser.source())
            }) {
                self.tail_parser = Some((
                    width,
                    acyclic_markdown::Markdown::with_deferred_code_in(
                        u16::try_from(width.saturating_sub(1).max(1)).unwrap_or(u16::MAX),
                        self.code_cache.clone(),
                    ),
                ));
            }
            let rows = match &mut self.tail_parser {
                Some((_, parser)) => {
                    let end = parser.source().len();
                    parser.push(&text[end..]);
                    wrap_rendered(
                        BlockKind::Answer,
                        crate::markdown::render_lines(parser.lines(), &self.theme),
                        width,
                        &self.theme,
                    )
                }
                None => wrap(BlockKind::Answer, text, width, &self.theme),
            };
            self.tail_rows = rows.lines.len();
            self.lines.extend(rows.lines);
            self.links.extend(rows.links);
            self.code.extend(rows.code);
        } else {
            self.tail_parser = None;
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
        self.links.truncate(self.lines.len());
        self.code.truncate(self.lines.len());
    }
}

/// A block's shown text: its trailing newlines end it rather than add rows.
fn settled(text: &str) -> &str {
    text.trim_end_matches('\n')
}

/// A prompt is shown bold as typed after an accent marker; an answer is
/// rendered markdown; a tool card is one row.
struct Wrapped {
    lines: Vec<Line<'static>>,
    links: Vec<Vec<crate::RowLink>>,
    code: Vec<Option<crate::CodeRow>>,
}

fn wrap(kind: BlockKind, text: &str, width: usize, theme: &ThemeStyles) -> Wrapped {
    let prompt = match kind {
        BlockKind::Prompt | BlockKind::Peer => true,
        BlockKind::Answer => false,
        BlockKind::Tool { failed } => {
            return Wrapped {
                lines: vec![card(text, failed, width, theme)],
                links: vec![Vec::new()],
                code: vec![None],
            };
        }
    };
    let marked = width > 1;
    let columns = if marked { width - 1 } else { width };
    let rendered = if prompt {
        literal(text, Style::default().add_modifier(Modifier::BOLD))
    } else {
        crate::markdown::render(text, columns, theme)
    };
    wrap_rendered(kind, rendered, width, theme)
}

fn literal(text: &str, style: Style) -> crate::markdown::Rendered {
    crate::markdown::Rendered {
        text: text.to_owned(),
        styles: vec![(0..text.len(), style)],
        links: Vec::new(),
        bands: Vec::new(),
        code: Vec::new(),
    }
}

fn wrap_rendered(
    kind: BlockKind,
    rendered: crate::markdown::Rendered,
    width: usize,
    theme: &ThemeStyles,
) -> Wrapped {
    #[cfg(test)]
    WRAP_BYTES.with(|work| work.set(work.get() + rendered.text.len()));
    let prompt = kind == BlockKind::Prompt;
    let marked = width > 1;
    let columns = if marked { width - 1 } else { width };
    let mut style_at = crate::styled_cells::ordered_styles(rendered.styles);
    let layout = Layout::from_text(&rendered.text, columns);
    let mut rows = Vec::new();
    let mut links = Vec::new();
    let mut code = Vec::new();
    let mut syntax = rendered.code.iter().peekable();
    let mut destinations = rendered.links.iter().peekable();
    let mut bands = rendered.bands.iter().peekable();
    let mut previous_end = None;
    for (index, row) in layout.rows.into_iter().enumerate() {
        let joins_previous = previous_end.replace(row.end) == Some(row.start);
        if row.cells.is_empty() && joins_previous {
            continue;
        }
        let marker = if !marked {
            ""
        } else if prompt && index == 0 {
            "›"
        } else if matches!(kind, BlockKind::Tool { .. }) {
            " "
        } else if kind == BlockKind::Peer && index == 0 {
            "↔"
        } else if index == 0 {
            "●"
        } else {
            " "
        };
        let mut spans: Vec<Span<'static>> = Vec::new();
        if !marker.is_empty() {
            let style = theme.token(if index == 0 {
                Token::Accent
            } else {
                Token::Muted
            });
            spans.push(Span::styled(marker, style));
        }
        while bands.peek().is_some_and(|band| band.end < row.start) {
            bands.next();
        }
        let band = bands
            .peek()
            .is_some_and(|band| band.start <= row.start && row.start <= band.end);
        let mut row_links: Vec<crate::RowLink> = Vec::new();
        for cell in &row.cells {
            while destinations
                .peek()
                .is_some_and(|(range, _)| range.end <= cell.source.start)
            {
                destinations.next();
            }
            if let Some((_, target)) = destinations
                .peek()
                .filter(|(range, _)| range.contains(&cell.source.start))
            {
                let start = cell.column + usize::from(marked);
                if let Some(previous) = row_links
                    .last_mut()
                    .filter(|link| link.columns.end == start && link.target == *target)
                {
                    previous.columns.end += cell.width;
                } else {
                    row_links.push(crate::RowLink {
                        columns: start..start + cell.width,
                        target: std::sync::Arc::clone(target),
                    });
                }
            }
        }
        while syntax
            .peek()
            .is_some_and(|(range, _)| range.end <= row.start)
        {
            syntax.next();
        }
        let decoration = syntax
            .peek()
            .filter(|(range, _)| {
                row.cells
                    .iter()
                    .any(|cell| range.contains(&cell.source.start))
            })
            .map(|(range, source)| {
                let mut at = marker.len();
                let mut cells = Vec::new();
                for cell in &row.cells {
                    if range.contains(&cell.source.start) {
                        cells.push((
                            at..at + cell.symbol.len(),
                            cell.source.start - range.start..cell.source.end - range.start,
                        ));
                    }
                    at += cell.symbol.len();
                }
                crate::CodeRow {
                    source: source.clone(),
                    cells,
                }
            });
        code.push(decoration);
        // The marker span stays its own, borrowed; text cells of one style join.
        crate::styled_cells::append(
            &mut spans,
            row.cells
                .into_iter()
                .map(|cell| (cell.source.start, cell.symbol)),
            &mut style_at,
        );
        let line = Line::from(spans).style(if band {
            theme.token(Token::Fg).patch(theme.code())
        } else {
            theme.token(Token::Fg)
        });
        rows.push(line);
        links.push(row_links);
    }
    rows.push(Line::default());
    links.push(Vec::new());
    code.push(None);
    Wrapped {
        lines: rows,
        links,
        code,
    }
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
        assert!(rows[0].starts_with("›older prompt"), "{rows:?}");
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
        let call = "shell  cargo test -p foo --all-features\nExit code: 0\nfirst\n**second**\n`code`\n> literal";
        transcript.push(BlockKind::Tool { failed: false }, call);
        transcript.push(BlockKind::Tool { failed: true }, "read  missing.txt");
        for width in [80, 24, 9] {
            let rows = text(transcript.rows(width));
            assert_eq!(rows.len(), 2, "{rows:?}");
            assert!(rows.iter().all(|row| row.width() <= usize::from(width)));
            assert!(rows[0].starts_with('\u{23fa}') && rows[0].ends_with('\u{2713}'));
            assert!(rows[1].ends_with('\u{2717}'), "{rows:?}");
            assert!(!rows[0].contains("**second**"));
        }
        let wide = text(transcript.rows(80));
        assert!(wide[0].contains("--all-features") && !wide[0].contains('\u{2026}'));
        // Negative control: a narrow card is clipped, never wrapped.
        let narrow = text(transcript.rows(24));
        assert!(narrow[0].contains('\u{2026}') && !narrow[0].contains("--all-features"));
        transcript.set_tool_details(true);
        let expanded = text(transcript.rows(80));
        assert_eq!(
            &expanded[1..6],
            [
                " Exit code: 0",
                " first",
                " **second**",
                " `code`",
                " > literal"
            ]
        );
        transcript.set_tool_details(false);
        assert_eq!(text(transcript.rows(80)).len(), 2);
        transcript.set_tool_details(true);
        assert_eq!(text(transcript.rows(80)), expanded);
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
