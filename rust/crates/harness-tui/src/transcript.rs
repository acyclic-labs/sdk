//! Complete transcript storage with a disk-backed row index and viewport cache.

use crate::Layout;
use ratatui::{
    style::Modifier,
    text::{Line, Span},
};
use ratatui_core::style::Style;
use std::{
    fs::File,
    io::{self, BufWriter, Read, Seek, SeekFrom, Write},
    ops::Range,
};

// Search continues through successive windows; this never limits the source.
const SEARCH_ROWS: usize = 256;
const INDEX_BYTES: usize = 16;

struct Block {
    prompt: bool,
    source: Range<u64>,
    row_end: usize,
    rendered_end: u64,
}

struct Rendered {
    text: BufWriter<File>,
    index: BufWriter<File>,
    bytes: u64,
    rows: usize,
}

impl Rendered {
    fn new() -> io::Result<Self> {
        Ok(Self {
            text: BufWriter::new(tempfile::tempfile()?),
            index: BufWriter::new(tempfile::tempfile()?),
            bytes: 0,
            rows: 0,
        })
    }

    fn push(&mut self, spans: Vec<Span<'static>>) -> io::Result<()> {
        let spans: Vec<_> = spans
            .into_iter()
            .map(|span| (span.content.into_owned(), span.style))
            .collect();
        let text = serde_json::to_vec(&spans).map_err(invalid)?;
        let end = self
            .bytes
            .checked_add(text.len() as u64)
            .ok_or_else(overflow)?;
        let rows = self.rows.checked_add(1).ok_or_else(overflow)?;
        self.index.write_all(&self.bytes.to_le_bytes())?;
        self.index.write_all(&(text.len() as u64).to_le_bytes())?;
        self.text.write_all(&text)?;
        self.bytes = end;
        self.rows = rows;
        Ok(())
    }

    fn truncate(&mut self, rows: usize, bytes: u64) -> io::Result<()> {
        self.text.flush()?;
        self.index.flush()?;
        self.text.get_mut().set_len(bytes)?;
        self.text.seek(SeekFrom::Start(bytes))?;
        let index_bytes = (rows as u64)
            .checked_mul(INDEX_BYTES as u64)
            .ok_or_else(overflow)?;
        self.index.get_mut().set_len(index_bytes)?;
        self.index.seek(SeekFrom::Start(index_bytes))?;
        self.bytes = bytes;
        self.rows = rows;
        Ok(())
    }

    fn block(&mut self, prompt: bool, source: &str, width: usize) -> io::Result<()> {
        let marked = width > 2;
        let columns = if marked { width - 2 } else { width };
        let (text, styles) = if prompt {
            (
                source.to_owned(),
                vec![(
                    0..source.len(),
                    Style::default().add_modifier(Modifier::BOLD),
                )],
            )
        } else {
            crate::markdown::render(source, columns)
        };
        let style_at = |offset| {
            let at = styles.partition_point(|(range, _)| range.end <= offset);
            styles
                .get(at)
                .filter(|(range, _)| range.start <= offset)
                .map_or_else(Style::default, |(_, style)| *style)
        };
        let mut previous_end = None;
        let mut first = true;
        Layout::visit_text_rows(&text, columns, |row| {
            let skip = row.cells.is_empty() && previous_end == Some(row.start);
            previous_end = Some(row.end);
            if skip {
                return Ok(());
            }
            let marker = if !marked {
                ""
            } else if prompt && first {
                "› "
            } else {
                "  "
            };
            first = false;
            let mut spans: Vec<Span<'static>> = Vec::new();
            if !marker.is_empty() {
                spans.push(Span::styled(marker, style_at(row.start)));
            }
            let start = spans.len();
            for cell in &row.cells {
                let style = style_at(cell.source.start);
                let join = spans.len() > start;
                match spans.last_mut() {
                    Some(span) if join && span.style == style => {
                        span.content.to_mut().push_str(&cell.symbol)
                    }
                    _ => spans.push(Span::styled(cell.symbol.clone(), style)),
                }
            }
            self.push(spans)
        })?;
        self.push(Vec::new())
    }
}

struct Spool {
    source: File,
    rendered: Rendered,
}

/// Complete settled and pending text. Source and wrapped rows live in temporary
/// files; only block addresses and the requested viewport are retained in memory.
/// Appending wraps the new block once; a width change rebuilds the disk index.
/// The committed conversation remains the durable source of truth.
pub struct TranscriptRows {
    spool: Option<Spool>,
    blocks: Vec<Block>,
    settled: usize,
    rows: usize,
    width: usize,
    window: Option<Range<usize>>,
    lines: Vec<Line<'static>>,
}

impl Default for TranscriptRows {
    fn default() -> Self {
        Self {
            spool: None,
            blocks: Vec::new(),
            settled: 0,
            rows: 0,
            width: 80,
            window: None,
            lines: Vec::new(),
        }
    }
}

impl TranscriptRows {
    /// Append a complete settled prompt or answer.
    ///
    /// # Errors
    /// Temporary storage or address representation failed.
    pub fn push(&mut self, prompt: bool, text: &str) -> io::Result<()> {
        self.clear_pending()?;
        self.append(prompt, text)?;
        self.settled = self.blocks.len();
        Ok(())
    }

    fn append(&mut self, prompt: bool, text: &str) -> io::Result<()> {
        if self.spool.is_none() {
            self.spool = Some(Spool {
                source: tempfile::tempfile()?,
                rendered: Rendered::new()?,
            });
        }
        let spool = self.spool.as_mut().expect("created above");
        let (source, rendered) = self
            .blocks
            .last()
            .map_or((0, 0), |block| (block.source.end, block.rendered_end));
        let end = source.checked_add(text.len() as u64).ok_or_else(overflow)?;
        // An earlier failed append owns no visible block. Retire its partial
        // writes before retrying, rather than publishing a partial row index.
        spool.source.set_len(source)?;
        spool.source.seek(SeekFrom::Start(source))?;
        spool.rendered.truncate(self.rows, rendered)?;
        spool.source.write_all(text.as_bytes())?;
        spool.rendered.block(prompt, text, self.width)?;
        self.rows = spool.rendered.rows;
        self.blocks.push(Block {
            prompt,
            source: source..end,
            row_end: self.rows,
            rendered_end: spool.rendered.bytes,
        });
        Ok(())
    }

    /// Rebuild wrapping when terminal columns change, without retaining all rows.
    /// Existing source and index remain valid if rebuilding fails.
    ///
    /// # Errors
    /// Temporary storage or address representation failed.
    pub fn set_width(&mut self, width: u16) -> io::Result<()> {
        let width = usize::from(width).max(1);
        if width == self.width {
            return Ok(());
        }
        if let Some(spool) = &mut self.spool {
            let mut rendered = Rendered::new()?;
            let mut ends = Vec::with_capacity(self.blocks.len());
            for block in &self.blocks {
                spool.source.seek(SeekFrom::Start(block.source.start))?;
                let bytes = read_bytes(&mut spool.source, block.source.end - block.source.start)?;
                let text = std::str::from_utf8(&bytes).map_err(invalid)?;
                rendered.block(block.prompt, text, width)?;
                ends.push((rendered.rows, rendered.bytes));
            }
            rendered.text.flush()?;
            rendered.index.flush()?;
            for (block, (row_end, rendered_end)) in self.blocks.iter_mut().zip(ends) {
                block.row_end = row_end;
                block.rendered_end = rendered_end;
            }
            self.rows = rendered.rows;
            spool.rendered = rendered;
        }
        self.width = width;
        self.window = None;
        self.lines.clear();
        Ok(())
    }

    /// Replace transient prompts, retaining every pending operation's text.
    ///
    /// # Errors
    /// Temporary storage or address representation failed.
    pub fn pending<'a>(
        &mut self,
        prompts: impl Iterator<Item = (bool, &'a str)>,
    ) -> io::Result<()> {
        self.clear_pending()?;
        for (prompt, text) in prompts {
            self.append(prompt, text)?;
        }
        Ok(())
    }

    fn clear_pending(&mut self) -> io::Result<()> {
        if self.settled == self.blocks.len() {
            return Ok(());
        }
        let (source, rows, bytes) = self.settled.checked_sub(1).map_or((0, 0, 0), |index| {
            let block = &self.blocks[index];
            (block.source.end, block.row_end, block.rendered_end)
        });
        let spool = self.spool.as_mut().expect("pending blocks have storage");
        spool.source.set_len(source)?;
        spool.rendered.truncate(rows, bytes)?;
        self.blocks.truncate(self.settled);
        self.rows = rows;
        if self.window.as_ref().is_some_and(|window| window.end > rows) {
            self.window = None;
            self.lines.clear();
        }
        Ok(())
    }

    /// Materialize just the requested visual rows. Repeating the same viewport
    /// after a status update or append reuses its allocations.
    ///
    /// # Errors
    /// Temporary storage, UTF-8 validation or address representation failed.
    pub fn window(&mut self, range: Range<usize>) -> io::Result<&[Line<'static>]> {
        let range =
            range.start.min(self.rows)..range.end.min(self.rows).max(range.start.min(self.rows));
        if self.window.as_ref() == Some(&range) {
            return Ok(&self.lines);
        }
        let mut lines = Vec::with_capacity(range.len());
        if !range.is_empty() {
            let spool = self.spool.as_mut().expect("rows have storage");
            let rendered = &mut spool.rendered;
            rendered.index.flush()?;
            rendered.text.flush()?;
            let start = (range.start as u64)
                .checked_mul(INDEX_BYTES as u64)
                .ok_or_else(overflow)?;
            rendered.index.get_mut().seek(SeekFrom::Start(start))?;
            let count = range.len().checked_mul(INDEX_BYTES).ok_or_else(overflow)?;
            let index = read_bytes(rendered.index.get_mut(), count as u64)?;
            let mut entries = Vec::with_capacity(range.len());
            for entry in index.chunks_exact(INDEX_BYTES) {
                let offset = u64::from_le_bytes(entry[..8].try_into().map_err(invalid)?);
                let length = u64::from_le_bytes(entry[8..].try_into().map_err(invalid)?);
                if offset
                    .checked_add(length)
                    .is_none_or(|end| end > rendered.bytes)
                {
                    return Err(invalid("row index outside storage"));
                }
                entries.push((offset, length));
            }
            let first = entries[0].0;
            let (last, length) = entries[entries.len() - 1];
            let end = last.checked_add(length).ok_or_else(overflow)?;
            let length = end.checked_sub(first).ok_or_else(overflow)?;
            rendered.text.get_mut().seek(SeekFrom::Start(first))?;
            let bytes = read_bytes(rendered.text.get_mut(), length)?;
            for (offset, length) in entries {
                let offset = usize::try_from(offset.checked_sub(first).ok_or_else(overflow)?)
                    .map_err(invalid)?;
                let length = usize::try_from(length).map_err(invalid)?;
                let end = offset.checked_add(length).ok_or_else(overflow)?;
                let spans: Vec<(String, Style)> =
                    serde_json::from_slice(bytes.get(offset..end).ok_or_else(overflow)?)
                        .map_err(invalid)?;
                lines.push(Line::from(
                    spans
                        .into_iter()
                        .map(|(text, style)| Span::styled(text, style))
                        .collect::<Vec<_>>(),
                ));
            }
        }
        self.lines = lines;
        self.window = Some(range);
        Ok(&self.lines)
    }

    /// Search backward before an exclusive row index, through successive disk
    /// windows. An empty query has no hit.
    ///
    /// # Errors
    /// Temporary storage or address representation failed.
    pub fn find(&mut self, query: &str, before: usize) -> io::Result<Option<usize>> {
        if query.is_empty() {
            return Ok(None);
        }
        let mut end = before.min(self.rows);
        while end > 0 {
            let start = end.saturating_sub(SEARCH_ROWS);
            if let Some(row) = self.window(start..end)?.iter().rposition(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
                    .contains(query)
            }) {
                return Ok(Some(start + row));
            }
            end = start;
        }
        Ok(None)
    }

    /// Complete visual row count, including pending prompts.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows
    }

    /// Whether no visual rows exist.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }
}

fn read_bytes(input: &mut File, length: u64) -> io::Result<Vec<u8>> {
    let length = usize::try_from(length).map_err(invalid)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(io::Error::other)?;
    bytes.resize(length, 0);
    input.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn invalid(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
fn overflow() -> io::Error {
    invalid("transcript address cannot be represented")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_status_updates_reuse_the_existing_viewport() {
        let mut transcript = TranscriptRows::default();
        transcript.set_width(20).unwrap();
        transcript.push(true, "retained prompt").unwrap();
        let allocation = transcript.window(0..1).unwrap()[0].spans[0]
            .content
            .as_ptr();
        transcript.push(false, "answer").unwrap();
        transcript.pending([(true, "pending")].into_iter()).unwrap();
        transcript
            .pending([(false, "**replacement**")].into_iter())
            .unwrap();
        assert_eq!(
            transcript.window(0..1).unwrap()[0].spans[0]
                .content
                .as_ptr(),
            allocation
        );
        assert_eq!(
            transcript.find("prompt", transcript.len()).unwrap(),
            Some(0)
        );
        assert_eq!(transcript.find("pending", transcript.len()).unwrap(), None);
        assert_eq!(transcript.find("**", transcript.len()).unwrap(), None);
        assert!(
            transcript
                .find("replacement", transcript.len())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn complete_source_remains_searchable_while_only_visible_rows_are_materialized() {
        let mut transcript = TranscriptRows::default();
        transcript
            // Explicit Markdown hard breaks keep this a 22,000-row source
            // after answer styling; soft breaks intentionally render as spaces.
            .push(false, &format!("begin  \n{}\nend", "界  \n".repeat(22_000)))
            .unwrap();
        for index in 0..300 {
            transcript.push(true, &format!("prompt {index}")).unwrap();
        }
        assert!(transcript.len() > 22_000);
        let mut glyphs = 0;
        for start in (0..transcript.len()).step_by(SEARCH_ROWS) {
            for line in transcript.window(start..start + SEARCH_ROWS).unwrap() {
                glyphs += line
                    .spans
                    .iter()
                    .map(|span| span.content.matches('界').count())
                    .sum::<usize>();
            }
        }
        assert_eq!(glyphs, 22_000);
        assert_eq!(transcript.find("begin", transcript.len()).unwrap(), Some(0));
        let end = transcript.find("end", transcript.len()).unwrap().unwrap();
        assert!(end > 22_000);
        let row = transcript
            .find("prompt 299", transcript.len())
            .unwrap()
            .unwrap();
        assert!(row > end);
        assert_eq!(transcript.window(0..1).unwrap().len(), 1);
        assert_eq!(transcript.lines.len(), 1);
        transcript.set_width(1).unwrap();
        assert!(
            transcript
                .window(0..10)
                .unwrap()
                .iter()
                .all(|line| line.width() <= 1)
        );
    }

    #[test]
    fn display_width_graphemes_and_pending_text_survive_wrapping_and_resize() {
        let mut transcript = TranscriptRows::default();
        transcript.push(true, "界e\u{301}👩‍💻\ttext\nlast").unwrap();
        transcript
            .pending([(true, "still pending")].into_iter())
            .unwrap();
        for width in 1..12 {
            transcript.set_width(width).unwrap();
            let len = transcript.len();
            assert!(
                transcript
                    .window(0..len)
                    .unwrap()
                    .iter()
                    .all(|line| line.width() <= usize::from(width))
            );
        }
        transcript.set_width(80).unwrap();
        assert!(
            transcript
                .find("e\u{301}👩‍💻", transcript.len())
                .unwrap()
                .is_some()
        );
        assert!(
            transcript
                .find("still pending", transcript.len())
                .unwrap()
                .is_some()
        );
        transcript.push(false, "settled").unwrap();
        assert!(
            transcript
                .find("still pending", transcript.len())
                .unwrap()
                .is_none()
        );
    }
}
