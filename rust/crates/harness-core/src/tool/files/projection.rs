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
    pub(crate) fn model_schema(mode: ProjectionMode) -> Result<Value> {
        mode.schema::<FileView<'_>>()
    }

    pub(crate) fn read_schema(mode: ProjectionMode) -> Result<Value> {
        if mode == ProjectionMode::Reference {
            return Self::model_schema(mode);
        }
        schema::output::<ReadProjection<'_>>()
    }
}

#[derive(Serialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ReadProjection<'a> {
    Json { value: FileView<'a> },
    Parts { parts: [MediaPart<'a>; 2] },
}

#[derive(Serialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum MediaPart<'a> {
    Text { text: &'a str },
    File { file: &'a FileRef, policy: Value },
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
                if let Some(page) = &result.0.page {
                    if page.start > page.end || (result.0.media.is_some()) {
                        return Err(super::executor::rejection(
                            work,
                            "invalid read page provenance",
                        ));
                    }
                    if !matches!(input.0.selection, Some(ReadSelection::Cells { .. }))
                        && !input.0.path.ends_with(".ipynb")
                        && (page.end.checked_sub(page.start) != Some(text.len() as u64)
                            || page.end > file.descriptor().byte_length())
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
                        || range.end > file.descriptor().byte_length()
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
                    crate::executor::with_history_io(io, |history| {
                        file.descriptor().verify(text.as_bytes(), history)
                    })?;
                }
                // The original executor validates the exact mounted path and
                // volume before dispatch. A projector has no workspace and
                // cannot compare that view path to a volume-relative path.
                if let Some(version) = input.0.version.as_deref() {
                    let original = super::version::decode_version(file.path(), version, work)?;
                    if original.0 != *file {
                        return Err(super::executor::rejection(
                            work,
                            "read result belongs to another version",
                        ));
                    }
                }
                if mode == ProjectionMode::Full {
                    let version = super::version::encode_version(file, work)?;
                    if let Some(media) = &result.0.media {
                        let intent = match media {
                            ReadSelection::Image { detail } => {
                                crate::model::NativeMediaIntent::Image { detail: *detail }
                            }
                            ReadSelection::Pdf => crate::model::NativeMediaIntent::Document {
                                maximum_pages: u32::MAX - 1,
                            },
                            _ => {
                                return Err(super::executor::rejection(
                                    work,
                                    "invalid native read modality",
                                ));
                            }
                        };
                        let policy = crate::model::NativeMediaPolicy {
                            intent,
                            maximum_bytes: crate::conversation::MAX_EXACT_JS_INTEGER,
                            maximum_work: crate::conversation::MAX_EXACT_JS_INTEGER,
                            configuration: None,
                        };
                        // The source version doubles as the compact text provenance.
                        let encoded = super::value(&policy, u64::MAX, work)?;
                        #[derive(Serialize)]
                        struct Native<'a> {
                            native: &'a Value,
                        }
                        let encoded = super::value(&Native { native: &encoded.0 }, u64::MAX, work)?;
                        return super::value(
                            &ReadProjection::Parts {
                                parts: [
                                    MediaPart::Text { text: &version.0 },
                                    MediaPart::File {
                                        file,
                                        policy: encoded.0,
                                    },
                                ],
                            },
                            u64::MAX,
                            work,
                        );
                    }
                    return schema::project_json(
                        &FileView {
                            path: &input.0.path,
                            version: &version.0,
                            text: Some(text),
                            page: result.0.page.as_ref(),
                        },
                        work,
                    );
                }
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
