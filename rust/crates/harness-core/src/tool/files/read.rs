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
        let maximum = text_budget(
            &file,
            &input.0.path,
            context.effective_scope().limits().render_bytes,
            work,
            io,
        )?;
        if let Some(selection) = &selection {
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
                    file,
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
        let admitted = count.min(maximum.saturating_add(3));
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
        let source = if admitted < count && !empty {
            match std::str::from_utf8(selected) {
                Ok(text) => text,
                Err(error) if error.error_len().is_none() => {
                    std::str::from_utf8(&selected[..error.valid_up_to()])
                        .map_err(|_| invalid("file read requires UTF-8 content"))?
                }
                Err(_) => return Err(invalid("file read requires UTF-8 content")),
            }
        } else {
            read_text(selected, empty, work)?
        };
        let shown =
            crate::tools::bounds::prefix(source, usize::try_from(maximum).unwrap_or(usize::MAX));
        let end = range.start + shown.len() as u64;
        #[derive(Serialize)]
        struct Read<'a> {
            file: &'a FileRef,
            text: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            page: Option<ReadPage>,
        }
        let encoded = super::value(
            &Read {
                file: &file,
                text: shown,
                page: (end < range.end).then_some(ReadPage {
                    start: range.start,
                    end,
                    next: Some(end),
                    offset: 0,
                }),
            },
            u64::MAX,
            work,
        )?;
        Ok((ToolResult { value: encoded.0 }, encoded.1))
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
                file,
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

fn text_budget(
    file: &FileRef,
    path: &str,
    maximum: u64,
    work: crate::NativeWork<'_>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<u64> {
    let (version, _hold) = super::version::encode_version(file, work)?;
    let empty = FileView {
        path,
        version: &version,
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
