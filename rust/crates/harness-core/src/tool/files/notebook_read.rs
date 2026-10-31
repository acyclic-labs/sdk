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
            } => {
                self.key = Some((start, end));
                return Ok(true);
            }
            SpanEvent::Start { depth: 0, kind, .. } if kind != SpanKind::Object => {
                return Err(invalid());
            }
            SpanEvent::Start { depth: 1, kind, .. } if self.field == Field::Cells => {
                if kind != SpanKind::Array || self.seen_cells {
                    return Err(invalid());
                }
                self.seen_cells = true;
                self.cells = true;
            }
            SpanEvent::Start {
                depth: 2,
                start,
                kind,
            } if self.cells => {
                if kind != SpanKind::Object {
                    return Err(invalid());
                }
                if self.count == self.offset {
                    self.empty = Some(start);
                }
            }
            SpanEvent::End {
                depth: 2,
                start,
                end,
                ..
            } if self.cells => {
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
            }
            SpanEvent::End { depth: 1, end, .. } if self.field == Field::Cells => {
                self.cells = false;
                self.array_end = Some(end - 1);
            }
            SpanEvent::End {
                depth: 1,
                start,
                end,
                kind,
            } if self.field == Field::Format => {
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
    let mut window = super::window::Window::first(
        context,
        input,
        acyclic_fs::ByteRange {
            offset: 0,
            length: quantum,
        },
        work,
        io,
    )
    .await?;
    let _version_hold;
    let (version, version_hold) = super::version::encode_source(&window.source, work)?;
    _version_hold = version_hold;
    let budget = super::read::encoded_text_budget(&version, &input.path, maximum, work, io)?;
    let _scan_hold = edit::allocation(
        work,
        std::mem::size_of::<JsonSpans>() + std::mem::size_of::<Selection>(),
    )?;
    let mut cursor = JsonSpans::default();
    let mut selection = Selection::new(offset, limit, budget.min(quantum));
    let bounds = JsonBounds {
        maximum_bytes: window.source.byte_length(),
        maximum_items: u64::MAX,
        maximum_depth: crate::executor::json::RECORD_MAXIMUM_DEPTH,
    };
    let code = Some(crate::wire::ErrorCode::Invalid);
    let mut position = 0;
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
        }
        position += window.bytes.len() as u64;
        if position == window.source.byte_length() {
            break;
        }
        if window.bytes.is_empty() {
            return Err(executor::rejection(work, "source range made no progress"));
        }
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
    cursor.finish(code, &mut |event| selection.event(event, work))?;
    if selection.format != Some(4) || !selection.seen_cells || offset > selection.count {
        return Err(executor::rejection(
            work,
            "cell operations require nbformat v4 and an existing cell offset",
        ));
    }
    let empty = selection
        .empty
        .or(selection.array_end)
        .ok_or_else(|| executor::rejection(work, "notebook cell array is missing"))?;
    let (start, selected_end) = selection.interval.unwrap_or((empty, empty));
    window.bytes = acyclic_stream::OwnedBytes::new();
    window.bytes = window
        .next(
            context,
            &input.path,
            acyclic_fs::ByteRange {
                offset: start,
                length: (selected_end - start)
                    .min(budget.saturating_add(3))
                    .min(quantum),
            },
            work,
            io,
        )
        .await?;
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
                next: (selection.next_cell < selection.count || end < selected_end).then_some(end),
                offset,
            },
        },
        u64::MAX,
        work,
    )?;
    Ok((ToolResult { value: result.0 }, result.1))
}
