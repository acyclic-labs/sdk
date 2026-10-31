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
                if let Some(range) = input.0.range {
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
                if input.0.path != file.path() {
                    return Err(super::executor::rejection(
                        work,
                        "read result belongs to another path",
                    ));
                }
                if let Some(version) = input.0.version.as_deref() {
                    let original = super::version::decode_version(&input.0.path, version, work)?;
                    if original.0 != *file {
                        return Err(super::executor::rejection(
                            work,
                            "read result belongs to another version",
                        ));
                    }
                }
                if mode == ProjectionMode::Full {
                    let version = super::version::encode_version(file, work)?;
                    return schema::project_json(
                        &FileView {
                            path: file.path(),
                            version: &version.0,
                            text: Some(text),
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
                                path: value.0.file.path(),
                                version: &version.0,
                                text: None,
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
