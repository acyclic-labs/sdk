//! Bounded cell discovery over the original immutable range producer.
use super::*;
use crate::executor::json::{JsonBounds, JsonCodecCharge, JsonSpans, SpanEvent, SpanKind};

#[cfg(test)]
#[path = "notebook_read_checks.rs"]
mod checks;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Field {
    Other,
    Cells,
    Format,
}

struct Document {
    origin: u64,
    end: u64,
    count: u64,
}

impl Document {
    const BYTES: usize = 40;
    fn decode(bytes: &[u8], length: u64, work: crate::NativeWork<'_>) -> Result<Self> {
        let invalid = || executor::rejection(work, "notebook index differs from its source");
        if bytes.len() != Self::BYTES || &bytes[..8] != b"nbcells1" {
            return Err(invalid());
        }
        let word = |start| {
            let mut word = [0; 8];
            word.copy_from_slice(&bytes[start..start + 8]);
            u64::from_le_bytes(word)
        };
        let document = Self {
            origin: word(16),
            end: word(24),
            count: word(32),
        };
        if word(8) != length
            || document.origin == 0
            || document.origin > document.end
            || document.end >= length
            || document.count > document.end - document.origin
        {
            return Err(invalid());
        }
        Ok(document)
    }
    fn encode(&self, length: u64) -> [u8; Self::BYTES] {
        let mut bytes = [0; Self::BYTES];
        bytes[..8].copy_from_slice(b"nbcells1");
        for (slot, word) in
            bytes[8..]
                .chunks_exact_mut(8)
                .zip([length, self.origin, self.end, self.count])
        {
            slot.copy_from_slice(&word.to_le_bytes());
        }
        bytes
    }
}

struct Selection {
    offset: u64,
    limit: Option<u64>,
    maximum: u64,
    field: Field,
    seen_cells: bool,
    seen_format: bool,
    cells: bool,
    format: Option<u32>,
    count: u64,
    next_cell: u64,
    interval: Option<(u64, u64)>,
    empty: Option<u64>,
    array_end: Option<u64>,
    key: Option<(u64, u64)>,
    format_span: Option<(u64, u64)>,
    array_origin: Option<u64>,
    origin_pending: Option<u64>,
    boundary: Option<(u64, u64, bool)>,
    element_depth: u32,
    total: Option<u64>,
    warm: bool,
    done: bool,
}

impl Selection {
    fn new(offset: u64, limit: Option<u64>, maximum: u64) -> Self {
        Self {
            offset,
            limit,
            maximum,
            field: Field::Other,
            seen_cells: false,
            seen_format: false,
            cells: false,
            format: None,
            count: 0,
            next_cell: offset,
            interval: None,
            empty: None,
            array_end: None,
            key: None,
            format_span: None,
            array_origin: None,
            origin_pending: None,
            boundary: None,
            element_depth: 2,
            total: None,
            warm: false,
            done: false,
        }
    }

    fn supply(&mut self, bytes: &[u8], key: bool, work: crate::NativeWork<'_>) -> Result<()> {
        let text = std::str::from_utf8(bytes)
            .map_err(|_| executor::rejection(work, "invalid notebook metadata"))?;
        if key {
            let key = notebook::decode(
                std::marker::PhantomData::<crate::executor::json::BorrowedText<'_>>,
                text,
                work,
            )?;
            self.field = match key.0.as_ref() {
                "cells" => Field::Cells,
                "nbformat" => Field::Format,
                _ => Field::Other,
            };
        } else {
            self.format = Some(*notebook::decode(
                std::marker::PhantomData::<u32>,
                text,
                work,
            )?);
        }
        Ok(())
    }

    fn pending(&mut self) -> Option<(u64, u64, bool)> {
        if let Some((start, end)) = self.key.take() {
            self.field = Field::Other;
            // A longer spelling cannot equal either known ASCII key, even
            // when every character uses a six-byte Unicode escape.
            if end - start <= 2 + 6 * 8 {
                return Some((start, end, true));
            }
        }
        self.format_span
            .take()
            .map(|(start, end)| (start, end, false))
    }

    fn event(&mut self, event: SpanEvent, work: crate::NativeWork<'_>) -> Result<bool> {
        let invalid = || executor::rejection(work, "invalid nbformat v4 cell document");
        match event {
            SpanEvent::Key {
                depth: 1,
                start,
                end,
            } if !self.warm => {
                self.key = Some((start, end));
                return Ok(true);
            }
            SpanEvent::Start { depth: 0, kind, .. } if !self.warm && kind != SpanKind::Object => {
                return Err(invalid());
            }
            SpanEvent::Start {
                depth: 1,
                start,
                kind,
            } if !self.warm && self.field == Field::Cells => {
                if kind != SpanKind::Array || self.seen_cells {
                    return Err(invalid());
                }
                self.seen_cells = true;
                self.cells = true;
                self.array_origin = Some(start + 1);
                self.origin_pending = self.array_origin;
                return Ok(true);
            }
            SpanEvent::Start { depth, start, kind }
                if self.cells && depth == self.element_depth =>
            {
                if kind != SpanKind::Object {
                    return Err(invalid());
                }
                if self.count == self.offset {
                    self.empty = Some(start);
                }
                if self.count != 0 {
                    self.boundary = Some((self.count, start, false));
                }
                if self.warm && self.count >= self.offset && self.limit == Some(0) {
                    self.done = true;
                }
                return Ok(true);
            }
            SpanEvent::End {
                depth, start, end, ..
            } if self.cells && depth == self.element_depth => {
                let next = self.count.checked_add(1).ok_or_else(invalid)?;
                if self.count >= self.offset
                    && self.count - self.offset < self.limit.unwrap_or(u64::MAX)
                {
                    let first = self.interval.map_or(start, |(first, _)| first);
                    if self.interval.is_none() || end - first <= self.maximum {
                        self.interval = Some((first, end));
                        self.next_cell = next;
                    }
                }
                self.count = next;
                if self.warm
                    && self.interval.is_some()
                    && (self.next_cell < self.count
                        || self
                            .interval
                            .is_some_and(|(start, end)| end - start >= self.maximum)
                        || self
                            .limit
                            .is_some_and(|limit| self.next_cell - self.offset >= limit))
                {
                    self.done = true;
                    return Ok(true);
                }
            }
            SpanEvent::End { depth, end, .. } if self.cells && depth + 1 == self.element_depth => {
                self.cells = false;
                self.array_end = Some(end - 1);
                self.boundary = Some((self.count, end - 1, true));
                if self.warm {
                    if self.total != Some(self.count) {
                        return Err(invalid());
                    }
                    self.done = true;
                }
                return Ok(true);
            }
            SpanEvent::End {
                depth: 1,
                start,
                end,
                kind,
            } if !self.warm && self.field == Field::Format => {
                if self.seen_format || kind != SpanKind::Scalar || end - start > 10 {
                    return Err(invalid());
                }
                self.seen_format = true;
                self.format_span = Some((start, end));
                return Ok(true);
            }
            _ => {}
        }
        Ok(false)
    }
}

pub(super) async fn read(
    context: &ToolContext,
    input: &ReadFileInput,
    offset: u64,
    limit: Option<u64>,
    work: crate::NativeWork<'_>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(ToolResult, crate::NativeCustody)> {
    let maximum = context.effective_scope().limits().render_bytes;
    let quantum = context
        .effective_scope()
        .limits()
        .file_bytes
        .min(crate::tool::output::VIEW_BYTES);
    if quantum == 0 {
        return Err(executor::rejection(
            work,
            "cell projection requires source byte credit",
        ));
    }
    let destination = super::index::OrdinalIndex::destination(context, io)?;
    let mut window = super::window::Window::first(
        context,
        input,
        acyclic_fs::ByteRange {
            offset: 0,
            length: if destination.is_some() { 0 } else { quantum },
        },
        work,
        io,
    )
    .await?;
    let _version_hold;
    let (version, version_hold) = super::version::encode_source(&window.source, work)?;
    _version_hold = version_hold;
    let mut index = destination
        .filter(|destination| destination.volume() != window.source.volume())
        .map(|destination| {
            super::index::OrdinalIndex::new(
                context,
                destination,
                &version,
                window.source.byte_length(),
                super::index::Projection::Cells,
                work,
            )
        })
        .transpose()?;
    let document = if let Some(index) = &index {
        index
            .metadata(Document::BYTES as u64, io)
            .await?
            .map(|bytes| Document::decode(&bytes, window.source.byte_length(), work))
            .transpose()?
    } else {
        None
    };
    let budget = super::read::encoded_text_budget(&version, &input.path, maximum, work, io)?;
    let _scan_hold = edit::allocation(
        work,
        std::mem::size_of::<JsonSpans>() + std::mem::size_of::<Selection>() + Document::BYTES,
    )?;
    let mut cursor = JsonSpans::default();
    let mut selection = Selection::new(offset, limit, budget.min(quantum));
    let mut position = 0;
    if let Some(document) = &document {
        if offset > document.count {
            return Err(executor::rejection(
                work,
                "notebook offset exceeds cell count",
            ));
        }
        let index = index
            .as_mut()
            .ok_or_else(|| executor::rejection(work, "notebook index destination is missing"))?;
        index.origin(document.origin)?;
        index.end(document.end)?;
        let (ordinal, start) = index.seek(offset, work, io).await?;
        position = start;
        cursor = JsonSpans::array_at(position, document.origin - 1);
        selection.count = ordinal;
        selection.element_depth = 1;
        selection.warm = true;
        selection.cells = true;
        selection.seen_cells = true;
        selection.seen_format = true;
        selection.format = Some(4);
        selection.array_origin = Some(document.origin);
        selection.array_end = Some(document.end);
        selection.total = Some(document.count);
    }
    if window.bytes.is_empty() && window.source.byte_length() != 0 {
        window.bytes = window
            .next(
                context,
                &input.path,
                acyclic_fs::ByteRange {
                    offset: position,
                    length: quantum,
                },
                work,
                io,
            )
            .await?;
    }
    let bounds = JsonBounds {
        maximum_bytes: window.source.byte_length(),
        maximum_items: u64::MAX,
        maximum_depth: crate::executor::json::RECORD_MAXIMUM_DEPTH,
    };
    let code = Some(crate::wire::ErrorCode::Invalid);
    loop {
        crate::executor::with_history_io(io, |history| {
            if let Some(history) = history.as_deref_mut() {
                history.codec_charge(JsonCodecCharge::Read {
                    bytes: window.bytes.len() as u64,
                })?;
            }
            Ok(())
        })?;
        let mut consumed = 0;
        while consumed < window.bytes.len() {
            let count = crate::executor::with_history_io(io, |history| {
                cursor.feed(
                    &window.bytes[consumed..],
                    bounds,
                    code,
                    &mut |event| {
                        if let Some(history) = history.as_deref_mut() {
                            history.codec_charge(event)?;
                        }
                        Ok(())
                    },
                    &mut |event| selection.event(event, work),
                )
            })?;
            if count == 0 {
                return Err(executor::rejection(work, "JSON range made no progress"));
            }
            consumed += count;
            if let Some(origin) = selection.origin_pending.take() {
                if let Some(index) = &mut index {
                    index.origin(origin)?;
                }
            }
            if let Some((ordinal, position, terminal)) = selection.boundary.take() {
                if let Some(index) = &mut index {
                    if terminal {
                        index.finish(ordinal, position)?;
                    } else {
                        index.record(ordinal, position)?;
                    }
                    index.publish(io).await?;
                }
            }
            if let Some((start, end, key)) = selection.pending() {
                let extra;
                let bytes = match start.checked_sub(position).zip(end.checked_sub(position)) {
                    Some((start, end)) if end <= window.bytes.len() as u64 => {
                        &window.bytes[start as usize..end as usize]
                    }
                    _ => {
                        extra = window
                            .next(
                                context,
                                &input.path,
                                acyclic_fs::ByteRange {
                                    offset: start,
                                    length: end - start,
                                },
                                work,
                                io,
                            )
                            .await?;
                        &extra
                    }
                };
                selection.supply(bytes, key, work)?;
            }
            if selection.done {
                break;
            }
        }
        if selection.done || position + window.bytes.len() as u64 == window.source.byte_length() {
            break;
        }
        if window.bytes.is_empty() {
            return Err(executor::rejection(work, "source range made no progress"));
        }
        position += window.bytes.len() as u64;
        window.bytes = acyclic_stream::OwnedBytes::new();
        window.bytes = window
            .next(
                context,
                &input.path,
                acyclic_fs::ByteRange {
                    offset: position,
                    length: quantum,
                },
                work,
                io,
            )
            .await?;
    }
    if !selection.warm {
        cursor.finish(code, &mut |event| selection.event(event, work))?;
    }
    let total = selection.total.unwrap_or(selection.count);
    if selection.format != Some(4) || !selection.seen_cells || offset > total {
        return Err(executor::rejection(
            work,
            "cell operations require nbformat v4 and an existing cell offset",
        ));
    }
    if !selection.warm {
        if let Some(index) = &index {
            let document = Document {
                origin: selection
                    .array_origin
                    .ok_or_else(|| executor::rejection(work, "notebook array origin is missing"))?,
                end: selection
                    .array_end
                    .ok_or_else(|| executor::rejection(work, "notebook array end is missing"))?,
                count: total,
            };
            index
                .publish_metadata(&document.encode(window.source.byte_length()), io)
                .await?;
        }
    }
    let empty = selection
        .empty
        .or(selection.array_end)
        .ok_or_else(|| executor::rejection(work, "notebook cell array is missing"))?;
    let (start, selected_end) = selection.interval.unwrap_or((empty, empty));
    let length = (selected_end - start)
        .min(budget.saturating_add(3))
        .min(quantum);
    if let Some(relative) = start.checked_sub(position)
        && relative + length <= window.bytes.len() as u64
    {
        window.bytes = window
            .bytes
            .slice(relative as usize..(relative + length) as usize);
    } else {
        window.bytes = acyclic_stream::OwnedBytes::new();
        window.bytes = window
            .next(
                context,
                &input.path,
                acyclic_fs::ByteRange {
                    offset: start,
                    length,
                },
                work,
                io,
            )
            .await?;
    }
    let text = super::read::page_text(
        &window.bytes,
        start == selected_end,
        start + (window.bytes.len() as u64) < selected_end,
        work,
    )?;
    let shown = crate::tools::bounds::prefix(text, usize::try_from(budget).unwrap_or(usize::MAX));
    let end = start + shown.len() as u64;
    #[derive(Serialize)]
    struct Read<'a> {
        file: FileReadSource,
        text: &'a str,
        page: ReadPage,
    }
    let result = super::value(
        &Read {
            file: window.source,
            text: shown,
            page: ReadPage {
                start,
                end,
                next: (selection.next_cell < total || end < selected_end).then_some(end),
                offset,
            },
        },
        u64::MAX,
        work,
    )?;
    Ok((ToolResult { value: result.0 }, result.1))
}
