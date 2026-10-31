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
        if let Some(ReadSelection::Lines { offset, limit }) = input.0.selection {
            return super::lines::read(context, &input.0, offset, limit, work, io).await;
        }
        if let Some(ReadSelection::Cells { offset, limit }) = input.0.selection {
            return super::notebook_read::read(context, &input.0, offset, limit, work, io).await;
        }
        if input.0.selection.is_none()
            && input.0.range.is_none()
            && input.0.path.ends_with(".ipynb")
        {
            return super::notebook_read::read(context, &input.0, 0, None, work, io).await;
        }
        if input.0.selection.is_none() {
            return self.read_path_range(context, &input.0, work, io).await;
        }
        let (file, _resolved, _source_custody) = self
            .source(context, &input.0.path, input.0.version.as_deref(), work, io)
            .await?;
        self.media_result(file, input.0.selection.clone(), work)
    }

    async fn read_path_range(
        &self,
        context: &ToolContext,
        input: &ReadFileInput,
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
        let window = super::window::Window::first(
            context,
            input,
            acyclic_fs::ByteRange {
                offset: start,
                length: count.min(window).max(1),
            },
            work,
            io,
        )
        .await?;
        let bytes = &window.bytes;
        let end = input
            .range
            .map_or(window.source.byte_length(), |range| range.end);
        if end > window.source.byte_length() {
            return Err(invalid("read result belongs to another selection"));
        }
        if input.range.is_none() {
            let media = crate::model::image_media_type(&bytes)
                .map(|_| ReadSelection::Image {
                    detail: crate::model::ImageDetail::Auto,
                })
                .or_else(|| bytes.starts_with(b"%PDF-").then_some(ReadSelection::Pdf));
            if media.is_some() {
                let selected = match window.source {
                    FileReadSource::Authored(selected) => selected,
                    FileReadSource::Content(file) => return self.media_result(file, media, work),
                };
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
            bytes,
            end == start,
            start + (bytes.len() as u64) < end,
            work,
        )?;
        text_result(
            window.source,
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
    let _version_hold;
    let (version, version_hold) = super::version::encode_source(&file, work)?;
    _version_hold = version_hold;
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

pub(super) fn encoded_text_budget(
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

pub(super) fn page_text<'a>(
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
