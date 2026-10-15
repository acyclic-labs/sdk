use super::super::text_files::{ReadResult, SearchResult};
use super::*;
use schema::ProjectionMode;

/// Replaceable model projection for the four canonical file result shapes.
#[derive(Clone, Copy)]
pub enum FileProjection {
    /// Exact UTF-8 read result.
    Read(ProjectionMode),
    /// Published immutable reference.
    Publication(ProjectionMode),
    /// Explicit UTF-8 interval and omissions.
    Range(ProjectionMode),
    /// Literal matches and omissions.
    Search(ProjectionMode),
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
                let text = decode::<String>(&result.value, work)?;
                if mode == schema::ProjectionMode::Full {
                    return schema::project_json(&text.0, work);
                }
                let input = decode::<ReadFileInput>(&invocation.arguments, work)?;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let owned_io = work
                    .map(crate::executor::PreparationIo::from_native_work)
                    .transpose()?;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let io = owned_io.as_ref();
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                let io: Option<&crate::executor::PreparationIo> = None;
                crate::executor::with_history_io(io, |history| {
                    input.0.file.descriptor().verify(text.0.as_bytes(), history)
                })?;
                schema::summary_reference(
                    &input.0.file,
                    160,
                    format_args!(
                        "Read {} UTF-8 bytes. The exact text is retained in the canonical tool result and omitted from this model projection.",
                        text.0.len()
                    ),
                    work,
                )
            }
            Self::Publication(mode) => {
                let value = decode::<FileResult>(&result.value, work)?;
                match mode {
                    schema::ProjectionMode::Full => schema::project_json(&value.0, work),
                    schema::ProjectionMode::Reference => schema::project_reference(
                        &value.0.file,
                        "File publication completed. The original output reference, workspace and content identity remain in the canonical tool result. Body bytes are omitted from this model projection.",
                        work,
                    ),
                }
            }
            Self::Range(mode) => {
                let value = decode::<ReadResult>(&result.value, work)?;
                if mode == ProjectionMode::Full {
                    return schema::project_json(&value.0, work);
                }
                schema::summary_reference(
                    &value.0.file,
                    400,
                    format_args!(
                        "Read bytes {}..{}; {} source bytes omitted before and {} after. Selected text is retained in the canonical tool result and omitted from this model projection.",
                        value.0.selection.range.start,
                        value.0.selection.range.end,
                        value.0.selection.omitted_before,
                        value.0.selection.omitted_after,
                    ),
                    work,
                )
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
