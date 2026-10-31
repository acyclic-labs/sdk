//! The shared versioned read path used by every model tool profile.
use super::executor::{read_text, rejection};
use super::*;

impl FileTool {
    pub(super) async fn read(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(ToolResult, crate::NativeCustody)> {
        let invalid = |reason| rejection(work, reason);
        let input = decode::<ReadFileInput>(&invocation.arguments, work)?;
        if input.0.range.is_some() && input.0.selection.is_some() {
            return Err(invalid(
                "byte ranges and projections are mutually exclusive",
            ));
        }
        let authored_version = input
            .0
            .version
            .as_deref()
            .is_some_and(super::version::is_authored);
        let path_range = input.0.version.is_none()
            && input.0.selection.is_none()
            && (input.0.range.is_some() || !input.0.path.ends_with(".ipynb"));
        if (path_range || authored_version) && input.0.selection.is_none() {
            let original = input
                .0
                .version
                .as_deref()
                .map(|version| {
                    super::version::decode_tool_version(Some(context), &input.0.path, version, work)
                })
                .transpose()?;
            let selected = original.as_ref().and_then(|(source, _)| match source {
                FileReadSource::Authored(selected) => Some(selected),
                FileReadSource::Content(_) => None,
            });
            return self
                .read_path_range(context, &input.0, selected, work, io)
                .await;
        }
        let (file, resolved, _source_custody) = self
            .source(context, &input.0.path, input.0.version.as_deref(), work, io)
            .await?;
        let length = file.descriptor().byte_length();
        let selection = input.0.selection.clone().or_else(|| {
            if input.0.range.is_some() {
                return None;
            }
            match file.descriptor().media_type() {
                "image/png" | "image/jpeg" | "image/webp" | "image/gif" => {
                    Some(ReadSelection::Image {
                        detail: crate::model::ImageDetail::Auto,
                    })
                }
                "application/pdf" => Some(ReadSelection::Pdf),
                _ if input.0.path.ends_with(".ipynb") => Some(ReadSelection::Cells {
                    offset: 0,
                    limit: None,
                }),
                _ => None,
            }
        });
        if matches!(
            selection,
            Some(ReadSelection::Image { .. } | ReadSelection::Pdf)
        ) {
            return self.media_result(file, selection, work);
        }
        if let Some(selection) = &selection {
            let maximum = text_budget(
                &file,
                &input.0.path,
                context.effective_scope().limits().render_bytes,
                work,
                io,
            )?;
            let bytes = match resolved {
                Some(bytes) => bytes,
                None => context.read_file(&file, io).await?,
            };
            let source = std::str::from_utf8(&bytes)
                .map_err(|_| invalid("text/cell projection requires UTF-8"))?;
            let (text, page, _hold) = match selection {
                ReadSelection::Cells { offset, limit } => {
                    notebook::read(source, *offset, *limit, maximum, work)?
                }
                ReadSelection::Lines { offset, limit } => {
                    line_page(source, *offset, *limit, maximum, work)?
                }
                _ => return Err(invalid("unsupported read projection")),
            };
            let encoded = super::value(
                &ReadFileResult {
                    file: FileReadSource::Content(file),
                    text,
                    page: Some(page),
                    media: None,
                },
                u64::MAX,
                work,
            )?;
            return Ok((ToolResult { value: encoded.0 }, encoded.1));
        }
        let range = input.0.range.unwrap_or(text::TextRange {
            start: 0,
            end: length,
        });
        let count = range
            .end
            .checked_sub(range.start)
            .filter(|_| range.end <= length)
            .ok_or_else(|| invalid("read range exceeds source or is reversed"))?;
        let empty = count == 0;
        // At most three extra bytes are needed to finish a UTF-8 scalar at
        // the byte window boundary. A large requested interval never causes
        // an equally large pinned read allocation.
        let admitted = count.min(
            context
                .effective_scope()
                .limits()
                .render_bytes
                .min(crate::tool::output::VIEW_BYTES)
                .saturating_add(3),
        );
        let was_resolved = resolved.is_some();
        let bytes = if let Some(bytes) = resolved {
            bytes
        } else {
            context
                .read_file_range(
                    &file,
                    acyclic_fs::ByteRange {
                        offset: range.start,
                        length: if empty && range.start < length {
                            1
                        } else {
                            admitted
                        },
                    },
                    io,
                )
                .await?
        };
        let selected = if was_resolved {
            let end = if empty && range.start < length {
                range.start.checked_add(1)
            } else {
                range.start.checked_add(admitted)
            };
            usize::try_from(range.start)
                .ok()
                .zip(end.and_then(|n| usize::try_from(n).ok()))
                .and_then(|(a, b)| bytes.get(a..b))
                .ok_or_else(|| invalid("read range exceeds source"))?
        } else {
            bytes.as_ref()
        };
        let source = page_text(selected, empty, admitted < count, work)?;
        text_result(
            FileReadSource::Content(file),
            source,
            &input.0.path,
            (range.start, range.end),
            context.effective_scope().limits().render_bytes,
            work,
            io,
        )
    }

    async fn read_path_range(
        &self,
        context: &ToolContext,
        input: &ReadFileInput,
        original: Option<&crate::conversation::FilePathSelection>,
        work: crate::NativeWork<'_>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(ToolResult, crate::NativeCustody)> {
        let invalid = |reason| rejection(work, reason);
        let start = input.range.map_or(0, |range| range.start);
        let count = input
            .range
            .map(|range| range.end.checked_sub(range.start))
            .unwrap_or(Some(u64::MAX))
            .ok_or_else(|| invalid("read range is reversed"))?;
        let window = context
            .effective_scope()
            .limits()
            .render_bytes
            .min(crate::tool::output::VIEW_BYTES)
            .saturating_add(3)
            .min(context.effective_scope().limits().file_bytes);
        let (selected, bytes, retained) = context
            .read_tool_path_range(
                &input.path,
                original.map(|file| &file.generation),
                acyclic_fs::ByteRange {
                    offset: start,
                    length: count.min(window).max(1),
                },
                io,
            )
            .await?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let _source_custody = io.map(|io| io.take_native_custody(retained)).transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = retained;
        let end = input.range.map_or(selected.byte_length, |range| range.end);
        if end > selected.byte_length || original.is_some_and(|file| file != &selected) {
            return Err(invalid("read result belongs to another selection"));
        }
        if input.range.is_none() {
            let media = crate::model::image_media_type(&bytes)
                .map(|_| ReadSelection::Image {
                    detail: crate::model::ImageDetail::Auto,
                })
                .or_else(|| bytes.starts_with(b"%PDF-").then_some(ReadSelection::Pdf));
            if media.is_some() {
                // Native input needs complete bytes. Resolve only this already
                // selected generation; a concurrent head cannot substitute it.
                let (file, _complete, retained) = context
                    .read_tool_path(&input.path, Some(&selected.generation), io)
                    .await?;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let _media_custody = io.map(|io| io.take_native_custody(retained)).transpose()?;
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                let _ = retained;
                return self.media_result(file, media, work);
            }
        }
        let source = page_text(
            &bytes,
            end == start,
            start + (bytes.len() as u64) < end,
            work,
        )?;
        text_result(
            FileReadSource::Authored(selected),
            source,
            &input.path,
            (start, end),
            context.effective_scope().limits().render_bytes,
            work,
            io,
        )
    }

    fn media_result(
        &self,
        file: FileRef,
        selection: Option<ReadSelection>,
        work: crate::NativeWork<'_>,
    ) -> Result<(ToolResult, crate::NativeCustody)> {
        let valid = match &selection {
            Some(ReadSelection::Image { .. }) => matches!(
                file.descriptor().media_type(),
                "image/png" | "image/jpeg" | "image/gif" | "image/webp"
            ),
            Some(ReadSelection::Pdf) => file.descriptor().media_type() == "application/pdf",
            _ => false,
        };
        if !valid {
            return Err(rejection(
                work,
                "requested modality differs from source media type",
            ));
        }
        let encoded = super::value(
            &ReadFileResult {
                file: FileReadSource::Content(file),
                text: String::new(),
                page: None,
                media: selection,
            },
            u64::MAX,
            work,
        )?;
        Ok((ToolResult { value: encoded.0 }, encoded.1))
    }
}

fn text_result(
    file: FileReadSource,
    source: &str,
    path: &str,
    interval: (u64, u64),
    maximum: u64,
    work: crate::NativeWork<'_>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(ToolResult, crate::NativeCustody)> {
    let (start, end) = interval;
    let (version, _version_hold) = super::version::encode_source(&file, work)?;
    let complete = source.len() as u64 == end - start
        && crate::executor::with_history_io(io, |history| {
            crate::executor::json::json_byte_length(
                &FileView {
                    path,
                    version: &version,
                    text: Some(source),
                    page: None,
                },
                u64::MAX,
                history,
            )
        })? + schema::json_envelope_bytes()? as u64
            <= maximum.min(crate::tool::output::VIEW_BYTES);
    let shown = if complete {
        source
    } else {
        let budget = encoded_text_budget(&version, path, maximum, work, io)?;
        crate::tools::bounds::prefix(source, usize::try_from(budget).unwrap_or(usize::MAX))
    };
    let shown_end = start + shown.len() as u64;
    #[derive(Serialize)]
    struct Read<'a> {
        file: FileReadSource,
        text: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        page: Option<ReadPage>,
    }
    let encoded = super::value(
        &Read {
            file,
            text: shown,
            page: (shown_end < end).then_some(ReadPage {
                start,
                end: shown_end,
                next: Some(shown_end),
                offset: 0,
            }),
        },
        u64::MAX,
        work,
    )?;
    Ok((ToolResult { value: encoded.0 }, encoded.1))
}

fn text_budget(
    file: &FileRef,
    path: &str,
    maximum: u64,
    work: crate::NativeWork<'_>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<u64> {
    let (version, _hold) = super::version::encode_version(file, work)?;
    encoded_text_budget(&version, path, maximum, work, io)
}

fn encoded_text_budget(
    version: &str,
    path: &str,
    maximum: u64,
    work: crate::NativeWork<'_>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<u64> {
    let empty = FileView {
        path,
        version,
        text: Some(""),
        page: Some(&ReadPage {
            start: u64::MAX,
            end: u64::MAX,
            next: Some(u64::MAX),
            offset: u64::MAX,
        }),
    };
    let overhead = crate::executor::with_history_io(io, |history| {
        crate::executor::json::json_byte_length(&empty, u64::MAX, history)
    })? + schema::json_envelope_bytes()? as u64;
    maximum
        .min(crate::tool::output::VIEW_BYTES)
        .checked_sub(overhead)
        .filter(|budget| *budget >= 6)
        .ok_or_else(|| executor::rejection(work, "render credit cannot hold a source page"))
}

fn line_page(
    source: &str,
    offset: u64,
    limit: Option<u64>,
    maximum: u64,
    work: crate::NativeWork<'_>,
) -> Result<(String, ReadPage, crate::NativeCustody)> {
    let mut start = 0;
    let mut line = 0;
    for part in source.split_inclusive('\n') {
        if line == offset {
            break;
        }
        start += part.len();
        line += 1;
    }
    if line < offset {
        return Err(rejection(work, "line offset exceeds source"));
    }
    let remaining = source
        .get(start..)
        .ok_or_else(|| rejection(work, "invalid line boundary"))?;
    let mut selected = 0;
    for part in remaining
        .split_inclusive('\n')
        .take(usize::try_from(limit.unwrap_or(u64::MAX)).unwrap_or(usize::MAX))
    {
        selected += part.len();
        if selected as u64 >= maximum {
            break;
        }
    }
    let selected = remaining
        .get(..selected)
        .ok_or_else(|| rejection(work, "invalid line boundary"))?;
    let shown =
        crate::tools::bounds::prefix(selected, usize::try_from(maximum).unwrap_or(usize::MAX));
    let mut text = String::new();
    let mut hold = edit::allocation(work, std::mem::size_of::<String>())?;
    edit::reserve_string(&mut hold, &mut text, shown.len())?;
    edit::charge(&hold, shown.len() as u64, (start + selected.len()) as u64)?;
    text.push_str(shown);
    let end = start + shown.len();
    Ok((
        text,
        ReadPage {
            start: start as u64,
            end: end as u64,
            next: (end < source.len()).then_some(end as u64),
            offset,
        },
        hold,
    ))
}

fn page_text<'a>(
    bytes: &'a [u8],
    empty: bool,
    truncated: bool,
    work: crate::NativeWork<'_>,
) -> Result<&'a str> {
    if !empty && truncated {
        match std::str::from_utf8(bytes) {
            Ok(text) => Ok(text),
            Err(error) if error.error_len().is_none() => {
                std::str::from_utf8(&bytes[..error.valid_up_to()])
                    .map_err(|_| rejection(work, "file read requires UTF-8 content"))
            }
            Err(_) => Err(rejection(work, "file read requires UTF-8 content")),
        }
    } else {
        read_text(bytes, empty, work)
    }
}

#[cfg(test)]
mod checks {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn text_windows_resume_exact_bytes_and_admit_exact_complete_credit(source in ".{0,12000}", maximum in 4096u64..32000) {
            let file = crate::tool::tests::file(source.as_bytes()).unwrap();
            let mut cursor = 0;
            let mut assembled = String::new();
            loop {
                let (result, _hold) = text_result(FileReadSource::Content(file.clone()), &source[cursor..], file.path(),
                    (cursor as u64, source.len() as u64), maximum, None, None).unwrap();
                let invocation = ToolInvocation {
                    operation_id:crate::OperationId::new(),call_id:"page".into(),name:"read".into(),
                    arguments:serde_json::json!({"path":file.path(),"version":serde_json::to_string(&file).unwrap(),"range":{"start":cursor,"end":source.len()}}),
                };
                let tool = super::super::read().unwrap();
                let value = tool.projection.project(&invocation, &result, None).unwrap().0;
                prop_assert!(serde_json::to_vec(&value).unwrap().len() as u64 <= maximum);
                tool.definition.validate_projection(&value, crate::conversation::Limits::default(), None).unwrap();
                let shown = value["value"]["text"].as_str().unwrap();
                prop_assert!(source[cursor..].starts_with(shown));
                assembled.push_str(shown);
                if result.value.get("page").is_none() { break; }
                let next = result.value["page"]["next"].as_u64().unwrap() as usize;
                prop_assert!(next > cursor, "page failed to advance");
                prop_assert_eq!(next, cursor + shown.len());
                cursor = next;
            }
            prop_assert_eq!(assembled.as_str(), source.as_str());
            let tiny: String = source.chars().take(40).collect();
            let file = crate::tool::tests::file(tiny.as_bytes()).unwrap();
            let version = serde_json::to_string(&file).unwrap();
            let exact = serde_json::json!({"kind":"json","value":{"path":file.path(),"version":version,"text":tiny}});
            let credit = serde_json::to_vec(&exact).unwrap().len() as u64;
            let (result, _hold) = text_result(FileReadSource::Content(file.clone()), &tiny, file.path(), (0,tiny.len() as u64), credit, None, None).unwrap();
            prop_assert_eq!(result.value["text"].as_str().unwrap(), tiny.as_str());
            prop_assert!(result.value.get("page").is_none());
        }
    }
}
