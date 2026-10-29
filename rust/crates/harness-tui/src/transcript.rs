//! Complete transcript storage with a disk-backed row index and viewport cache.

mod index;

use crate::Layout;
use index::Index;
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
// Rebuild advances through successive pages, never a total-block allowance.
const BLOCK_PAGE: usize = 256;

struct Block {
    prompt: bool,
    source: Range<u64>,
    row_end: usize,
    rendered_end: u64,
}

impl Block {
    fn record(&self) -> io::Result<[u64; 5]> {
        Ok([
            u64::from(self.prompt),
            self.source.start,
            self.source.end,
            u64::try_from(self.row_end).map_err(invalid)?,
            self.rendered_end,
        ])
    }

    fn from_record(record: [u64; 5]) -> io::Result<Self> {
        let [prompt, start, end, rows, rendered_end] = record;
        if prompt > 1 || start > end {
            return Err(invalid("invalid transcript block address"));
        }
        Ok(Self {
            prompt: prompt != 0,
            source: start..end,
            row_end: usize::try_from(rows).map_err(invalid)?,
            rendered_end,
        })
    }
}

struct Rendered {
    text: BufWriter<File>,
    index: Index<2>,
    bytes: u64,
    reading: bool,
}

impl Rendered {
    fn new() -> io::Result<Self> {
        Ok(Self {
            text: BufWriter::new(tempfile::tempfile()?),
            index: Index::new()?,
            bytes: 0,
            reading: false,
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
        if self.reading {
            self.text.seek(SeekFrom::Start(self.bytes))?;
            self.reading = false;
        }
        self.index.push([self.bytes, text.len() as u64])?;
        self.text.write_all(&text)?;
        self.bytes = end;
        Ok(())
    }

    fn truncate(&mut self, rows: usize, bytes: u64) -> io::Result<()> {
        self.text.flush()?;
        self.index.truncate(rows)?;
        self.text.get_mut().set_len(bytes)?;
        self.text.seek(SeekFrom::Start(bytes))?;
        self.bytes = bytes;
        self.reading = false;
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
    source: BufWriter<File>,
    blocks: Index<5>,
    rendered: Rendered,
    reading: bool,
    failed: bool,
}

/// Complete settled and pending text. Source and wrapped rows live in temporary
/// files. Block addresses are indexed on disk; only the last block address,
/// a bounded index page and the requested viewport are retained in memory.
/// Appending wraps the new block once; a width change rebuilds the disk index.
/// The committed conversation remains the durable source of truth.
pub struct TranscriptRows {
    spool: Option<Spool>,
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
        self.settled = self
            .spool
            .as_ref()
            .expect("append created storage")
            .blocks
            .len();
        Ok(())
    }

    fn append(&mut self, prompt: bool, text: &str) -> io::Result<()> {
        if self.spool.is_none() {
            self.spool = Some(Spool {
                source: BufWriter::new(tempfile::tempfile()?),
                blocks: Index::new()?,
                rendered: Rendered::new()?,
                reading: false,
                failed: false,
            });
        }
        let spool = self.spool.as_mut().expect("created above");
        let (source, rendered) = spool.blocks.last().map_or(Ok((0, 0)), |record| {
            let block = Block::from_record(record)?;
            Ok::<_, io::Error>((block.source.end, block.rendered_end))
        })?;
        let end = source.checked_add(text.len() as u64).ok_or_else(overflow)?;
        // Ordinary appends keep all four writers buffered. Only an earlier
        // failed append can leave unpublished source or row bytes to retire.
        if spool.failed {
            spool.source.flush()?;
            spool.source.get_mut().set_len(source)?;
            spool.rendered.truncate(self.rows, rendered)?;
        }
        if spool.failed || spool.reading {
            spool.source.seek(SeekFrom::Start(source))?;
            spool.reading = false;
        }
        spool.failed = true;
        spool.source.write_all(text.as_bytes())?;
        spool.rendered.block(prompt, text, self.width)?;
        let rows = spool.rendered.index.len();
        spool.blocks.push(
            Block {
                prompt,
                source: source..end,
                row_end: rows,
                rendered_end: spool.rendered.bytes,
            }
            .record()?,
        )?;
        self.rows = rows;
        spool.failed = false;
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
            spool.source.flush()?;
            // Reads may fail partway through a block. The next append restores
            // the original published source end before writing in either case.
            spool.reading = true;
            let mut rendered = Rendered::new()?;
            let mut blocks = Index::new()?;
            for start in (0..spool.blocks.len()).step_by(BLOCK_PAGE) {
                let end = start.saturating_add(BLOCK_PAGE).min(spool.blocks.len());
                for record in spool.blocks.page(start..end)? {
                    let mut block = Block::from_record(record)?;
                    spool
                        .source
                        .get_mut()
                        .seek(SeekFrom::Start(block.source.start))?;
                    let bytes = read_bytes(
                        spool.source.get_mut(),
                        block.source.end - block.source.start,
                    )?;
                    let text = std::str::from_utf8(&bytes).map_err(invalid)?;
                    rendered.block(block.prompt, text, width)?;
                    block.row_end = rendered.index.len();
                    block.rendered_end = rendered.bytes;
                    blocks.push(block.record()?)?;
                }
            }
            rendered.text.flush()?;
            rendered.index.flush()?;
            blocks.flush()?;
            // Both replacement indexes are complete before either becomes visible.
            self.rows = rendered.index.len();
            spool.rendered = rendered;
            spool.blocks = blocks;
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
        let Some(spool) = &mut self.spool else {
            return Ok(());
        };
        if self.settled == spool.blocks.len() {
            return Ok(());
        }
        let (source, rows, bytes) = if self.settled == 0 {
            (0, 0, 0)
        } else {
            let record = spool
                .blocks
                .page(self.settled - 1..self.settled)?
                .pop()
                .ok_or_else(overflow)?;
            let block = Block::from_record(record)?;
            (block.source.end, block.row_end, block.rendered_end)
        };
        spool.source.flush()?;
        spool.source.get_mut().set_len(source)?;
        spool.source.seek(SeekFrom::Start(source))?;
        spool.reading = false;
        spool.rendered.truncate(rows, bytes)?;
        spool.blocks.truncate(self.settled)?;
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
            rendered.text.flush()?;
            let entries = rendered.index.page(range.clone())?;
            for &[offset, length] in &entries {
                if offset
                    .checked_add(length)
                    .is_none_or(|end| end > rendered.bytes)
                {
                    return Err(invalid("row index outside storage"));
                }
            }
            let first = entries[0][0];
            let [last, length] = entries[entries.len() - 1];
            let end = last.checked_add(length).ok_or_else(overflow)?;
            let length = end.checked_sub(first).ok_or_else(overflow)?;
            rendered.reading = true;
            rendered.text.get_mut().seek(SeekFrom::Start(first))?;
            let bytes = read_bytes(rendered.text.get_mut(), length)?;
            for [offset, length] in entries {
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
    fn block_addresses_stay_on_disk_and_every_page_survives_reflow() {
        for retain_addresses in [false, true] {
            let mut retained = Vec::new();
            for count in [257, 4_097] {
                let mut transcript = TranscriptRows::default();
                let mut resident = Vec::new();
                let allocations = acyclic_test_support::measure(|| {
                    for index in 0..count {
                        transcript
                            .push(true, &format!("prompt {index:08}"))
                            .unwrap();
                        if retain_addresses {
                            let record = transcript.spool.as_ref().unwrap().blocks.last().unwrap();
                            resident.push(Block::from_record(record).unwrap());
                        }
                    }
                });
                retained.push((allocations.bytes_current, allocations.count_current));
                let reflow = acyclic_test_support::measure(|| transcript.set_width(20).unwrap());
                let mut next = 0;
                for start in (0..transcript.len()).step_by(SEARCH_ROWS) {
                    for line in transcript.window(start..start + SEARCH_ROWS).unwrap() {
                        let text = line
                            .spans
                            .iter()
                            .map(|span| span.content.as_ref())
                            .collect::<String>();
                        if text.is_empty() {
                            continue;
                        }
                        assert_eq!(text, format!("› prompt {next:08}"));
                        next += 1;
                    }
                }
                assert_eq!(next, count);
                // A page read must preserve the append position of both indexes.
                transcript.push(true, "after page reads").unwrap();
                assert!(
                    transcript
                        .find("after page reads", transcript.len())
                        .unwrap()
                        .is_some()
                );
                // Cross a block-page boundary with transient entries, then retire
                // them and reflow the settled addresses without retaining a vector.
                transcript
                    .pending((0..257).map(|_| (true, "transient")))
                    .unwrap();
                transcript.pending(std::iter::empty()).unwrap();
                transcript.set_width(80).unwrap();
                assert_eq!(
                    transcript.find("transient", transcript.len()).unwrap(),
                    None
                );
                assert!(
                    transcript
                        .find("after page reads", transcript.len())
                        .unwrap()
                        .is_some()
                );
                eprintln!(
                    "resident_control={retain_addresses} blocks={count} retained={allocations:?} reflow={reflow:?}"
                );
            }
            let oracle = || assert_eq!(retained[0], retained[1]);
            if retain_addresses {
                // Retaining the real accepted block addresses restores the old
                // resident Vec<Block> while preserving identical output. The
                // very same physical allocation oracle must reject that growth.
                assert!(std::panic::catch_unwind(oracle).is_err());
            } else {
                oracle();
            }
        }
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
