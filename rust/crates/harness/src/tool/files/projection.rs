use super::super::text_files::SearchResult;
use super::*;
use schema::ProjectionMode;

/// Replaceable model projection for the canonical file result shapes.
#[derive(Clone, Copy)]
pub enum FileProjection {
    /// Line-numbered text of a read selection.
    Read(ProjectionMode),
    /// Published immutable reference.
    Publication(ProjectionMode),
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
        _invocation: &ToolInvocation,
        result: &ToolResult,
        work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        match *self {
            Self::Read(mode) => {
                let value = decode::<ReadFileResult>(&result.value, work)?;
                let read = &value.0;
                if read.text.len() as u64 > read.file.descriptor().byte_length() || read.offset == 0
                {
                    let allocation = edit::allocation(work, 0)?;
                    return Err(edit::invalid(
                        &allocation,
                        "read result does not fit its source file",
                    ));
                }
                if mode == ProjectionMode::Full {
                    let mut allocation = edit::allocation(work, std::mem::size_of::<String>())?;
                    let text = super::lines::numbered(&read.text, read.offset, &mut allocation)?;
                    return schema::project_text(&text, work);
                }
                let lines = read.text.lines().count() as u64;
                schema::summary_reference(
                    &read.file,
                    read.file.path().len() + 240,
                    format_args!(
                        "Read {} lines from line {} of {} ({} UTF-8 bytes). The text is retained in the canonical tool result and omitted from this model projection.",
                        lines,
                        read.offset,
                        read.file.path(),
                        read.text.len(),
                    ),
                    work,
                )
            }
            Self::Publication(mode) => {
                let value = decode::<FileResult>(&result.value, work)?;
                let file = &value.0.file;
                match mode {
                    schema::ProjectionMode::Full => {
                        let (text, _allocation) = schema::bounded_format(
                            file.path().len() + 64,
                            format_args!(
                                "Updated {} ({} bytes).",
                                file.path(),
                                file.descriptor().byte_length()
                            ),
                            work,
                        )?;
                        schema::project_text(&text, work)
                    }
                    schema::ProjectionMode::Reference => schema::project_reference(
                        file,
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
