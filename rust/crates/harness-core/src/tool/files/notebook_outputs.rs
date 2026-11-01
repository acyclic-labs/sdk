//! Native notebook payload spans discovered by the shared JSON cursor.
use super::*;
use crate::executor::json::{BorrowedText, SpanEvent, SpanKind};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Media {
    Png,
    Jpeg,
    Gif,
    Webp,
    Pdf,
}

impl Media {
    pub fn mime(self) -> &'static str {
        match self {
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
            Self::Gif => "image/gif",
            Self::Webp => "image/webp",
            Self::Pdf => "application/pdf",
        }
    }
    fn parse(value: &str) -> Option<Self> {
        match value {
            "image/png" => Some(Self::Png),
            "image/jpeg" => Some(Self::Jpeg),
            "image/gif" => Some(Self::Gif),
            "image/webp" => Some(Self::Webp),
            "application/pdf" => Some(Self::Pdf),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct Payload {
    pub cell: u64,
    pub cell_start: u64,
    pub output: u64,
    pub start: u64,
    pub end: u64,
    pub media: Media,
}

#[derive(Default)]
pub(super) struct Spans {
    pub payloads: Vec<Payload>,
    pub hold: crate::NativeCustody,
    active: Option<u64>,
    cell_start: u64,
    checkpoint: usize,
    output: u64,
    outputs: bool,
    data: bool,
    cell_field: bool,
    output_field: bool,
    media: Option<Media>,
    seen_outputs: bool,
    seen_data: bool,
    seen_media: u8,
    key: Option<(u64, u64, u32)>,
}

impl Spans {
    pub fn pending(&mut self) -> Option<(u64, u64, u32)> {
        self.key.take()
    }

    pub fn supply(&mut self, bytes: &[u8], level: u32, work: crate::NativeWork<'_>) -> Result<()> {
        match level {
            1 => self.cell_field = false,
            3 => self.output_field = false,
            4 => self.media = None,
            _ => {
                return Err(executor::rejection(
                    work,
                    "invalid notebook payload key depth",
                ));
            }
        }
        // The longest supported name is application/pdf. Longer spellings
        // cannot match even when every character uses a Unicode escape.
        if bytes.len() > 2 + 6 * 15 {
            return Ok(());
        }
        let text = std::str::from_utf8(bytes)
            .map_err(|_| executor::rejection(work, "invalid notebook payload key"))?;
        let key = notebook::decode(std::marker::PhantomData::<BorrowedText<'_>>, text, work)?;
        match level {
            1 => self.cell_field = key.0 == "outputs",
            3 => self.output_field = key.0 == "data",
            4 => self.media = Media::parse(key.0.as_ref()),
            _ => unreachable!(),
        }
        Ok(())
    }

    pub fn retain_cell(&mut self, accepted: bool) {
        if !accepted {
            self.payloads.truncate(self.checkpoint);
        }
        self.active = None;
    }

    pub fn event(
        &mut self,
        event: SpanEvent,
        depth: u32,
        cell: u64,
        selected: bool,
        work: crate::NativeWork<'_>,
    ) -> Result<bool> {
        let invalid = || executor::rejection(work, "invalid notebook output bundle");
        if let SpanEvent::Start {
            depth: at, start, ..
        } = event
            && at == depth
        {
            self.active = selected.then_some(cell);
            self.cell_start = start;
            self.checkpoint = self.payloads.len();
            self.output = 0;
            self.outputs = false;
            self.data = false;
            self.cell_field = false;
            self.output_field = false;
            self.media = None;
            self.seen_outputs = false;
        }
        let Some(cell) = self.active else {
            return Ok(false);
        };
        match event {
            SpanEvent::Key {
                depth: at,
                start,
                end,
            } if at == depth + 1
                || (self.outputs && at == depth + 3)
                || (self.data && at == depth + 4) =>
            {
                match at - depth {
                    1 => self.cell_field = false,
                    3 => self.output_field = false,
                    4 => self.media = None,
                    _ => unreachable!(),
                }
                if end - start > 2 + 6 * 15 {
                    return Ok(false);
                }
                self.key = Some((start, end, at - depth));
                return Ok(true);
            }
            SpanEvent::Start {
                depth: at, kind, ..
            } if at == depth + 1 && self.cell_field => {
                if self.seen_outputs || kind != SpanKind::Array {
                    return Err(invalid());
                }
                self.seen_outputs = true;
                self.outputs = true;
            }
            SpanEvent::Start {
                depth: at, kind, ..
            } if self.outputs && at == depth + 2 => {
                if kind != SpanKind::Object {
                    return Err(invalid());
                }
                self.output_field = false;
                self.seen_data = false;
            }
            SpanEvent::Start {
                depth: at, kind, ..
            } if self.outputs && at == depth + 3 && self.output_field => {
                if self.seen_data || kind != SpanKind::Object {
                    return Err(invalid());
                }
                self.seen_data = true;
                self.data = true;
                self.media = None;
                self.seen_media = 0;
            }
            SpanEvent::Start {
                depth: at, kind, ..
            } if self.data && at == depth + 4 => {
                if let Some(media) = self.media {
                    let bit = 1 << media as u8;
                    if self.seen_media & bit != 0
                        || !matches!(kind, SpanKind::String | SpanKind::Array)
                    {
                        return Err(invalid());
                    }
                    self.seen_media |= bit;
                }
            }
            SpanEvent::End {
                depth: at,
                start,
                end,
                ..
            } if self.data && at == depth + 4 => {
                if let Some(media) = self.media.take() {
                    let length = self.payloads.len().checked_add(1).ok_or_else(invalid)?;
                    edit::reserve_vec(&mut self.hold, &mut self.payloads, length)?;
                    edit::charge(&self.hold, std::mem::size_of::<Payload>() as u64, 1)?;
                    self.payloads.push(Payload {
                        cell,
                        cell_start: self.cell_start,
                        output: self.output,
                        start,
                        end,
                        media,
                    });
                }
            }
            SpanEvent::End { depth: at, .. } if self.data && at == depth + 3 => {
                self.data = false;
            }
            SpanEvent::End { depth: at, .. } if self.outputs && at == depth + 2 => {
                self.output = self.output.checked_add(1).ok_or_else(invalid)?;
            }
            SpanEvent::End { depth: at, .. } if self.outputs && at == depth + 1 => {
                self.outputs = false;
            }
            _ => {}
        }
        Ok(false)
    }
}

pub(super) struct Retained {
    pub values: Vec<NotebookOutput>,
    pub remaining: u64,
    pub next: Option<(u64, u64)>,
    hold: crate::NativeCustody,
}

fn path(
    operation: crate::OperationId,
    work: crate::NativeWork<'_>,
) -> Result<(String, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let (path, hold) =
            work.display(&format_args!("projections/v1/notebook-output/{operation}"))?;
        return Ok((path, Some(hold)));
    }
    let _ = work;
    Ok((format!("projections/v1/notebook-output/{operation}"), None))
}

/// The source identity and exact MIME interval name the original publication
/// receipt. Mutable path heads are never consulted to reuse an output.
pub(super) async fn retain(
    context: &ToolContext,
    input: &ReadFileInput,
    window: &super::window::Window,
    source: (&str, u64),
    spans: &[Payload],
    budget: u64,
    work: crate::NativeWork<'_>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<Retained> {
    let (version, position) = source;
    let mut retained = Retained {
        values: Vec::new(),
        remaining: budget,
        next: None,
        hold: edit::allocation(work, 0)?,
    };
    if spans.is_empty() {
        return Ok(retained);
    }
    let destination = context
        .task()
        .projection_destination(io)?
        .filter(|destination| destination.volume() != window.source.volume())
        .ok_or_else(|| {
            executor::rejection(
                work,
                "native notebook outputs require a separate private writer",
            )
        })?;
    let domain = b"file-projection/notebook-output/v1\0";
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        work.charge(acyclic_fs::WorkCounters {
            bytes_hashed: (domain.len() + version.len()) as u64,
            ..Default::default()
        })?;
    }
    let mut identity = blake3::Hasher::new();
    identity.update(domain);
    identity.update(version.as_bytes());
    let mut cell = None;
    let mut checkpoint = 0;
    let mut cell_budget = budget;
    for payload in spans {
        if cell != Some(payload.cell) {
            cell = Some(payload.cell);
            checkpoint = retained.values.len();
            cell_budget = retained.remaining;
        }
        let length = payload
            .end
            .checked_sub(payload.start)
            .filter(|length| {
                *length > 0
                    && *length <= context.effective_scope().limits().file_bytes
                    && payload.end <= window.source.byte_length()
            })
            .ok_or_else(|| {
                executor::rejection(work, "notebook payload exceeds source byte credit")
            })?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.charge(acyclic_fs::WorkCounters {
                bytes_hashed: 16,
                ..Default::default()
            })?;
        }
        let mut digest = identity.clone();
        digest.update(&payload.start.to_le_bytes());
        digest.update(&payload.end.to_le_bytes());
        let mut key = [0; 16];
        key.copy_from_slice(&digest.finalize().as_bytes()[..16]);
        let operation = crate::OperationId::from_bytes(key);
        let _path_hold;
        let (path, path_hold) = path(operation, work)?;
        _path_hold = path_hold;
        let mut file_hold;
        let (cached, owned) = context
            .task()
            .observe_private_file_once(destination, operation, io)
            .await?;
        file_hold = super::window::custody(io, owned)?;
        let file = if let Some(file) = cached {
            file
        } else {
            let bytes = if let Some(relative) = payload.start.checked_sub(position)
                && relative + length <= window.bytes.len() as u64
            {
                window
                    .bytes
                    .slice(relative as usize..(relative + length) as usize)
            } else {
                window
                    .next(
                        context,
                        &input.path,
                        acyclic_fs::ByteRange {
                            offset: payload.start,
                            length,
                        },
                        work,
                        io,
                    )
                    .await?
            };
            let decoded = decode_payload(&bytes, work)?;
            let expected = match payload.media {
                Media::Pdf => decoded.bytes.starts_with(b"%PDF-"),
                media => crate::model::image_media_type(&decoded.bytes) == Some(media.mime()),
            };
            if !expected {
                return Err(executor::rejection(
                    work,
                    "notebook payload differs from its MIME type",
                ));
            }
            // No durability work is paid during decoding. The original writer's
            // one publication commit retains the complete immutable output.
            let (file, owned) = context
                .task()
                .stage_private_file_once(
                    destination,
                    operation,
                    &path,
                    &decoded.bytes,
                    (payload.media.mime(), "notebook-output"),
                    io,
                )
                .await?;
            file_hold = super::window::custody(io, owned)?;
            file
        };
        if file.volume() != destination.volume()
            || file.path() != path
            || file.descriptor().media_type() != payload.media.mime()
            || file.display_name() != "notebook-output"
            || file.descriptor().byte_length() == 0
            || file.descriptor().byte_length() > context.effective_scope().limits().file_bytes
        {
            return Err(executor::rejection(
                work,
                "notebook output receipt differs from its projection",
            ));
        }
        let rendered = super::projection::notebook_output_bytes(&file, io)?;
        if rendered > retained.remaining.saturating_sub(6) {
            retained.values.truncate(checkpoint);
            retained.remaining = cell_budget;
            retained.next = Some((payload.cell, payload.cell_start));
            break;
        }
        retained.remaining -= rendered;
        let length = retained
            .values
            .len()
            .checked_add(1)
            .ok_or_else(|| executor::rejection(work, "notebook output count overflow"))?;
        edit::reserve_vec(&mut retained.hold, &mut retained.values, length)?;
        edit::charge(
            &retained.hold,
            std::mem::size_of::<NotebookOutput>() as u64,
            1,
        )?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let (Some(total), Some(incoming)) = (&mut retained.hold, &mut file_hold) {
            total.absorb(incoming)?;
        }
        let _ = &file_hold;
        retained.values.push(NotebookOutput {
            file,
            cell_index: payload.cell,
            output_index: payload.output,
            start: payload.start,
            end: payload.end,
        });
    }
    Ok(retained)
}

struct Decoded {
    bytes: Vec<u8>,
    _hold: crate::NativeCustody,
}

fn decode_payload(source: &[u8], work: crate::NativeWork<'_>) -> Result<Decoded> {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let text = std::str::from_utf8(source)
        .map_err(|_| executor::rejection(work, "invalid notebook MIME value"))?;
    enum Text<'a> {
        One(notebook::Decoded<BorrowedText<'a>>),
        Many(notebook::Decoded<Vec<BorrowedText<'a>>>),
    }
    let value = match source.first() {
        Some(b'"') => Text::One(notebook::decode(
            std::marker::PhantomData::<BorrowedText<'_>>,
            text,
            work,
        )?),
        Some(b'[') => Text::Many(notebook::decode(
            std::marker::PhantomData::<Vec<BorrowedText<'_>>>,
            text,
            work,
        )?),
        _ => {
            return Err(executor::rejection(
                work,
                "notebook MIME value must be a string or string array",
            ));
        }
    };
    let parts: &[BorrowedText<'_>] = match &value {
        Text::One(value) => std::slice::from_ref(&**value),
        Text::Many(value) => value.as_slice(),
    };
    let mut normalized_hold = edit::allocation(work, 0)?;
    let input_length = parts
        .iter()
        .try_fold(0usize, |length, text| length.checked_add(text.0.len()))
        .ok_or_else(|| executor::rejection(work, "notebook MIME length overflow"))?;
    edit::charge(&normalized_hold, 0, input_length as u64)?;
    let encoded = if let [text] = parts
        && !text.0.as_bytes().iter().any(u8::is_ascii_whitespace)
    {
        std::borrow::Cow::Borrowed(text.0.as_bytes())
    } else {
        edit::charge(&normalized_hold, input_length as u64, 0)?;
        let mut bytes = Vec::new();
        edit::reserve_vec(&mut normalized_hold, &mut bytes, input_length)?;
        for text in parts {
            bytes.extend(text.0.bytes().filter(|byte| !byte.is_ascii_whitespace()));
        }
        std::borrow::Cow::Owned(bytes)
    };
    let maximum = base64::decoded_len_estimate(encoded.len());
    let mut hold = edit::allocation(work, 0)?;
    let mut bytes = Vec::new();
    edit::reserve_vec(&mut hold, &mut bytes, maximum)?;
    edit::charge(&hold, maximum as u64 * 2, encoded.len() as u64)?;
    bytes.resize(maximum, 0);
    let length = STANDARD
        .decode_slice(&encoded, &mut bytes)
        .map_err(|_| executor::rejection(work, "invalid base64 notebook output"))?;
    bytes.truncate(length);
    Ok(Decoded { bytes, _hold: hold })
}

#[cfg(test)]
mod checks {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn encoded_outputs_decode_exactly_across_string_and_line_framing(
            bytes in prop::collection::vec(any::<u8>(), 0..2000), chunk in 1usize..100,
        ) {
            use base64::{Engine as _, engine::general_purpose::STANDARD};
            let encoded = STANDARD.encode(&bytes);
            let lines: Vec<_> = encoded.as_bytes().chunks(chunk)
                .map(|bytes| format!("{}\n",std::str::from_utf8(bytes).unwrap())).collect();
            for value in [json!(encoded), json!(lines)] {
                let source = serde_json::to_vec(&value).unwrap();
                let decoded = decode_payload(&source,None).unwrap();
                prop_assert_eq!(decoded.bytes.as_slice(), bytes.as_slice());
            }
            let invalid = serde_json::to_vec(&format!("{encoded}!bad")).unwrap();
            prop_assert!(decode_payload(&invalid,None).is_err());
        }
    }
}
