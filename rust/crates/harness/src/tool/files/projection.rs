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
        invocation: &ToolInvocation,
        result: &ToolResult,
        work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        match *self {
            Self::Read(mode) => {
                let value = decode::<ReadFileResult>(&result.value, work)?;
                let read = &value.0;
                // Executors and projections are replaceable independently, so
                // the projection checks the result answers this exact request:
                // its window starts at the requested line and holds no more
                // than the requested lines. A request for the whole file must
                // return the file, verified against its descriptor on the
                // caller's work ledger whatever length was returned.
                let request = decode::<ReadFileInput>(&invocation.arguments, work)?;
                let whole = request.0.offset.unwrap_or(1) == 1 && request.0.limit.is_none();
                if read.text.len() as u64 > read.file.descriptor().byte_length()
                    || read.offset != request.0.offset.unwrap_or(1)
                    || request
                        .0
                        .limit
                        .is_some_and(|limit| read.text.split_inclusive('\n').count() as u64 > limit)
                    || (whole && !matches_file(&read.file, &read.text, work)?)
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

/// Whether `text` is exactly `file`'s content, hashing on the caller's ledger.
fn matches_file(file: &FileRef, text: &str, work: crate::NativeWork<'_>) -> Result<bool> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        return file
            .descriptor()
            .matches_bytes(text.as_bytes(), &mut Some(&mut io));
    }
    let _ = work;
    file.descriptor().matches_bytes(text.as_bytes(), &mut None)
}
