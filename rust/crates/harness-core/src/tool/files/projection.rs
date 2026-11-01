use super::super::text_files::SearchResult;
use super::*;
use schema::ProjectionMode;

/// Replaceable model projection for the canonical file result shapes.
#[derive(Clone, Copy)]
pub enum FileProjection {
    /// Exact UTF-8 read result.
    Read(ProjectionMode),
    /// Published immutable reference.
    Publication(ProjectionMode),
    /// Literal matches and omissions.
    Search(ProjectionMode),
}

impl FileProjection {
    /// Exact model envelope, including a bounded source without a body digest.
    pub fn schema(self) -> Result<Value> {
        match self {
            Self::Read(ProjectionMode::Reference) => {
                let mut generator = schemars::generate::SchemaSettings::draft2020_12()
                    .for_serialize()
                    .into_generator();
                let content = generator.subschema_for::<schema::ReferenceProjection<'_>>();
                let authored = generator.subschema_for::<schema::JsonProjection<FileView<'_>>>();
                Ok(json!({"oneOf":[content,authored],"$defs":generator.take_definitions(true)}))
            }
            Self::Read(ProjectionMode::Full) => schema::output::<ReadProjection<'_>>(),
            Self::Publication(mode) => Self::model_schema(mode),
            Self::Search(mode) => mode.schema::<SearchResult>(),
        }
    }

    pub(crate) fn model_schema(mode: ProjectionMode) -> Result<Value> {
        mode.schema::<FileView<'_>>()
    }

    pub(crate) fn read_schema(mode: ProjectionMode) -> Result<Value> {
        Self::Read(mode).schema()
    }
}

#[derive(Serialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ReadProjection<'a> {
    Json {
        value: FileView<'a>,
    },
    Parts {
        #[schemars(with = "Vec<MediaPart<'a>>")]
        parts: MediaParts<'a>,
    },
}

#[derive(Serialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum MediaPart<'a> {
    Text {
        #[schemars(with = "str")]
        text: MediaText<'a>,
    },
    File {
        file: &'a FileRef,
        #[schemars(with = "Value")]
        policy: NativePolicy,
    },
}

#[derive(Serialize)]
struct NativePolicy {
    native: crate::model::NativeMediaPolicy,
}

#[derive(Clone, Copy)]
enum MediaText<'a> {
    Version(&'a str),
    Notebook {
        path: &'a str,
        version: &'a str,
        page: &'a ReadPage,
        text: &'a str,
    },
}

impl Serialize for MediaText<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Version(version) => serializer.serialize_str(version),
            Self::Notebook { path, version, page, text } => serializer.collect_str(&format_args!(
                "{version}\nFile {path}; cells from {}; source bytes {}..{}; next byte {:?}\n{text}",
                page.offset, page.start, page.end, page.next,
            )),
        }
    }
}

struct MediaParts<'a> {
    text: MediaText<'a>,
    primary: Option<(&'a FileRef, &'a ReadSelection)>,
    outputs: &'a [NotebookOutput],
}

fn policy(file: &FileRef, intent: crate::model::NativeMediaIntent) -> NativePolicy {
    NativePolicy {
        native: crate::model::NativeMediaPolicy {
            intent,
            maximum_bytes: file.descriptor().byte_length().max(1),
            maximum_work: file.descriptor().byte_length().max(1),
            configuration: None,
        },
    }
}

fn output_part(file: &FileRef) -> MediaPart<'_> {
    let intent = if file.descriptor().media_type() == "application/pdf" {
        crate::model::NativeMediaIntent::Document
    } else {
        crate::model::NativeMediaIntent::Image {
            detail: crate::model::ImageDetail::Auto,
        }
    };
    MediaPart::File {
        file,
        policy: policy(file, intent),
    }
}

pub(super) fn notebook_text_budget(
    path: &str,
    version: &str,
    maximum: impl Into<Option<u64>>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<u64> {
    let maximum = maximum
        .into()
        .map_or(crate::tool::output::VIEW_BYTES, |maximum| {
            maximum.min(crate::tool::output::VIEW_BYTES)
        });
    let page = ReadPage {
        start: u64::MAX,
        end: u64::MAX,
        next: Some(u64::MAX),
        offset: u64::MAX,
    };
    let empty = ReadProjection::Parts {
        parts: MediaParts {
            text: MediaText::Notebook {
                path,
                version,
                page: &page,
                text: "",
            },
            primary: None,
            outputs: &[],
        },
    };
    let overhead = crate::executor::with_history_io(io, |history| {
        crate::executor::json::json_byte_length(&empty, None, history)
    })?;
    Ok(maximum
        .min(crate::tool::output::VIEW_BYTES)
        .saturating_sub(overhead))
}

pub(super) fn notebook_output_bytes(
    file: &FileRef,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<u64> {
    crate::executor::with_history_io(io, |history| {
        crate::executor::json::json_byte_length(&output_part(file), None, history)
    })
    .map(|bytes| bytes + 1) // The preceding sequence comma.
}

impl Serialize for MediaParts<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::{Error as _, SerializeSeq};
        let length = self
            .outputs
            .len()
            .checked_add(1 + usize::from(self.primary.is_some()))
            .ok_or_else(|| S::Error::custom("native read part count overflow"))?;
        let mut parts = serializer.serialize_seq(Some(length))?;
        parts.serialize_element(&MediaPart::Text { text: self.text })?;
        if let Some((file, selection)) = self.primary {
            let intent = match selection {
                ReadSelection::Image { detail } => {
                    crate::model::NativeMediaIntent::Image { detail: *detail }
                }
                ReadSelection::Pdf => crate::model::NativeMediaIntent::Document,
                _ => return Err(S::Error::custom("invalid native read modality")),
            };
            parts.serialize_element(&MediaPart::File {
                file,
                policy: policy(file, intent),
            })?;
        }
        for output in self.outputs {
            parts.serialize_element(&output_part(&output.file))?;
        }
        parts.end()
    }
}

impl ToolProjection for FileProjection {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_projection(&self, work: crate::NativeWork<'_>) -> Result<()> {
        if let Some(work) = work {
            work.remaining()?;
        }
        Ok(())
    }
    fn project(
        &self,
        invocation: &ToolInvocation,
        result: &ToolResult,
        work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        match *self {
            Self::Read(mode) => {
                let result = decode::<ReadFileResult>(&result.value, work)?;
                let file = &result.0.file;
                let text = &result.0.text;
                let input = decode::<ReadFileInput>(&invocation.arguments, work)?;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let owned_io = work
                    .map(crate::executor::PreparationIo::from_native_work)
                    .transpose()?;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let io = owned_io.as_ref();
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                let io: Option<&crate::executor::PreparationIo> = None;
                if let Some(requested) = &input.0.selection {
                    let native =
                        matches!(requested, ReadSelection::Image { .. } | ReadSelection::Pdf);
                    if native != result.0.media.is_some()
                        || (native && result.0.media.as_ref() != Some(requested))
                    {
                        return Err(super::executor::rejection(
                            work,
                            "read result differs from requested projection",
                        ));
                    }
                }
                if let Some(page) = &result.0.page {
                    if page.start > page.end || (result.0.media.is_some()) {
                        return Err(super::executor::rejection(
                            work,
                            "invalid read page provenance",
                        ));
                    }
                    if page.end.checked_sub(page.start) != Some(text.len() as u64)
                        || page.end > file.byte_length()
                        || page
                            .next
                            .is_some_and(|next| next != page.end || next > file.byte_length())
                        || input
                            .0
                            .range
                            .is_some_and(|range| page.start != range.start || page.end > range.end)
                        || match input.0.selection {
                            Some(
                                ReadSelection::Lines { offset, .. }
                                | ReadSelection::Cells { offset, .. },
                            ) => page.offset != offset,
                            Some(ReadSelection::Image { .. } | ReadSelection::Pdf) => true,
                            None if input.0.path.ends_with(".ipynb") && input.0.range.is_none() => {
                                page.offset != 0
                            }
                            None => {
                                page.offset != 0 || (input.0.range.is_none() && page.start != 0)
                            }
                        }
                    {
                        return Err(super::executor::rejection(
                            work,
                            "text page differs from source interval",
                        ));
                    }
                } else if result.0.media.is_some() {
                    if !text.is_empty() {
                        return Err(super::executor::rejection(
                            work,
                            "native read must preserve the source file",
                        ));
                    }
                } else if let Some(range) = input.0.range {
                    if range.end.checked_sub(range.start) != Some(text.len() as u64)
                        || range.end > file.byte_length()
                    {
                        return Err(crate::executor::with_history_io(io, |history| {
                            crate::store::guarded_boundary(
                                history,
                                crate::wire::ErrorCode::Invalid,
                                "read result does not match its requested interval",
                            )
                        }));
                    }
                } else {
                    crate::executor::with_history_io(io, |history| match file {
                        FileReadSource::Content(file) => {
                            file.descriptor().verify(text.as_bytes(), history)
                        }
                        FileReadSource::Authored(file) if file.byte_length == text.len() as u64 => {
                            Ok(())
                        }
                        FileReadSource::Authored(_) => Err(crate::store::guarded_boundary(
                            history,
                            crate::wire::ErrorCode::Invalid,
                            "read result length differs from its selection",
                        )),
                    })?;
                }
                if !result.0.outputs.is_empty() {
                    let cells = match input.0.selection {
                        Some(ReadSelection::Cells { offset, limit }) => Some((offset, limit)),
                        None if input.0.path.ends_with(".ipynb") && input.0.range.is_none() => {
                            Some((0, None))
                        }
                        _ => None,
                    };
                    let Some((offset, limit)) = cells else {
                        return Err(super::executor::rejection(
                            work,
                            "native outputs require a notebook cell selection",
                        ));
                    };
                    let Some(page) = &result.0.page else {
                        return Err(super::executor::rejection(
                            work,
                            "notebook outputs require source page provenance",
                        ));
                    };
                    let mut previous = None;
                    for output in &result.0.outputs {
                        let position = (output.cell_index, output.output_index, output.start);
                        if output.start < page.start
                            || output.start >= output.end
                            || output.end > file.byte_length()
                            || (page.next.is_none() && output.end > page.end)
                            || output.cell_index < offset
                            || limit.is_some_and(|limit| output.cell_index - offset >= limit)
                            || previous.is_some_and(|previous| previous >= position)
                            || !matches!(
                                output.file.descriptor().media_type(),
                                "image/png"
                                    | "image/jpeg"
                                    | "image/gif"
                                    | "image/webp"
                                    | "application/pdf"
                            )
                            || output.file.descriptor().byte_length() == 0
                        {
                            return Err(super::executor::rejection(
                                work,
                                "notebook output differs from source provenance",
                            ));
                        }
                        previous = Some(position);
                    }
                }
                // The original executor validates the exact mounted path and
                // volume before dispatch. A projector has no workspace and
                // cannot compare that view path to a volume-relative path.
                if let Some(version) = input.0.version.as_deref() {
                    let original = super::version::decode_version(file.path(), version, work)?;
                    if !original.0.same_source(file) {
                        return Err(super::executor::rejection(
                            work,
                            "read result belongs to another version",
                        ));
                    }
                }
                if mode == ProjectionMode::Full {
                    let version = super::version::encode_source(file, work)?;
                    if let Some(media) = &result.0.media {
                        let FileReadSource::Content(file) = file else {
                            return Err(super::executor::rejection(
                                work,
                                "native media requires a complete content descriptor",
                            ));
                        };
                        return super::value(
                            &ReadProjection::Parts {
                                parts: MediaParts {
                                    text: MediaText::Version(&version.0),
                                    primary: Some((file, media)),
                                    outputs: &[],
                                },
                            },
                            None,
                            work,
                        );
                    }
                    if !result.0.outputs.is_empty() {
                        let page = result.0.page.as_ref().ok_or_else(|| {
                            super::executor::rejection(
                                work,
                                "notebook outputs require source page provenance",
                            )
                        })?;
                        return super::value(
                            &ReadProjection::Parts {
                                parts: MediaParts {
                                    text: MediaText::Notebook {
                                        path: &input.0.path,
                                        version: &version.0,
                                        page,
                                        text,
                                    },
                                    primary: None,
                                    outputs: &result.0.outputs,
                                },
                            },
                            None,
                            work,
                        );
                    }
                    return super::value(
                        &ReadProjection::Json {
                            value: FileView {
                                path: &input.0.path,
                                version: &version.0,
                                text: Some(text),
                                page: result.0.page.as_ref(),
                            },
                        },
                        None,
                        work,
                    );
                }
                let FileReadSource::Content(file) = file else {
                    let version = super::version::encode_source(file, work)?;
                    return schema::project_json(
                        &FileView {
                            path: &input.0.path,
                            version: &version.0,
                            text: None,
                            page: result.0.page.as_ref(),
                        },
                        work,
                    );
                };
                if let Some(range) = input.0.range {
                    return schema::summary_reference(
                        file,
                        400,
                        format_args!(
                            "Read bytes {}..{}; {} source bytes omitted before and {} after. Selected text is retained in the canonical tool result and omitted from this model projection.",
                            range.start,
                            range.end,
                            range.start,
                            file.descriptor().byte_length() - range.end,
                        ),
                        work,
                    );
                }
                schema::summary_reference(
                    file,
                    160,
                    format_args!(
                        "Read {} UTF-8 bytes. The exact text is retained in the canonical tool result and omitted from this model projection.",
                        text.len()
                    ),
                    work,
                )
            }
            Self::Publication(mode) => {
                let value = decode::<FileResult>(&result.value, work)?;
                match mode {
                    schema::ProjectionMode::Full => {
                        let version = super::version::encode_version(&value.0.file, work)?;
                        schema::project_json(
                            &FileView {
                                path: invocation
                                    .arguments
                                    .get("path")
                                    .and_then(Value::as_str)
                                    .unwrap_or_else(|| value.0.file.path()),
                                version: &version.0,
                                text: None,
                                page: None,
                            },
                            work,
                        )
                    }
                    schema::ProjectionMode::Reference => schema::project_reference(
                        &value.0.file,
                        "File publication completed. The original output reference, workspace and content identity remain in the canonical tool result. Body bytes are omitted from this model projection.",
                        work,
                    ),
                }
            }
            Self::Search(mode) => {
                let value = decode::<SearchResult>(&result.value, work)?;
                if mode == ProjectionMode::Full {
                    return schema::project_json(&value.0, work);
                }
                schema::summary_reference(
                    &value.0.file,
                    480,
                    format_args!(
                        "Literal search completed: {} total matches, {} retained positions, {} positions omitted from the canonical result, {} byte comparisons. Query and retained positions are retained in the canonical tool result and omitted from this model projection.",
                        value.0.matches.total_matches,
                        value.0.matches.matches.len(),
                        value.0.matches.omitted_matches,
                        value.0.matches.work,
                    ),
                    work,
                )
            }
        }
    }
}
