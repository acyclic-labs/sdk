use super::*;

/// Replaceable projector for the canonical published file result.
#[derive(Clone, Copy)]
pub struct FileResultProjection(pub super::schema::ProjectionMode);

impl FileResultProjection {
    /// Complete generated model envelope contract for this representation.
    pub fn schema(self) -> Result<Value> {
        match self.0 {
            super::schema::ProjectionMode::Full => super::schema::json_projection::<FileResult>(),
            super::schema::ProjectionMode::Reference => super::schema::reference_projection(),
        }
    }
}

impl ToolProjection for FileResultProjection {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_projection(&self, work: crate::NativeWork<'_>) -> Result<()> {
        if let Some(work) = work {
            work.remaining()?;
        }
        Ok(())
    }

    fn project(
        &self,
        _: &ToolInvocation,
        result: &ToolResult,
        _work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = _work {
            let value = super::decode::<FileResult>(&result.value, Some(work))?;
            return match self.0 {
                super::schema::ProjectionMode::Full => {
                    super::schema::project_json(&value.0, Some(work))
                }
                super::schema::ProjectionMode::Reference => super::schema::project_reference(
                    &value.0.file,
                    "File publication completed. The original output reference, workspace and content identity remain in the canonical tool result. Body bytes are omitted from this model projection.",
                    Some(work),
                ),
            };
        }
        let value: FileResult = decode(&result.value, None)?.0;
        match self.0 {
            super::schema::ProjectionMode::Full => super::schema::project_json(&value, None),
            super::schema::ProjectionMode::Reference => super::schema::project_reference(
                &value.file,
                "File publication completed. The original output reference, workspace and content identity remain in the canonical tool result. Body bytes are omitted from this model projection.",
                None,
            ),
        }
    }
}

/// Replaceable projector for the exact reader's unchanged canonical string.
#[derive(Clone, Copy)]
pub struct ReadFileProjection(pub super::schema::ProjectionMode);

impl ReadFileProjection {
    /// Complete generated model envelope contract for this representation.
    pub fn schema(self) -> Result<Value> {
        match self.0 {
            super::schema::ProjectionMode::Full => super::schema::json_projection::<String>(),
            super::schema::ProjectionMode::Reference => super::schema::reference_projection(),
        }
    }
}

impl ToolProjection for ReadFileProjection {
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
        _work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = _work {
            let text = super::decode::<String>(&result.value, Some(work))?;
            return match self.0 {
                super::schema::ProjectionMode::Full => {
                    super::schema::project_json(&text.0, Some(work))
                }
                super::schema::ProjectionMode::Reference => {
                    use sha2::Digest as _;
                    let input = super::decode::<ReadFileInput>(&invocation.arguments, Some(work))?;
                    work.charge(acyclic_fs::WorkCounters {
                        bytes_hashed: text.0.len() as u64,
                        ..Default::default()
                    })
                    .map_err(super::edit::native_error)?;
                    if input.0.file.descriptor().byte_length() != text.0.len() as u64
                        || input.0.file.descriptor().sha256()
                            != &<[u8; 32]>::from(sha2::Sha256::digest(text.0.as_bytes()))
                    {
                        return Err(native::invalid(
                            "file descriptor differs from exact text result",
                        ));
                    }
                    native::summary_reference(
                        &input.0.file,
                        160,
                        format_args!(
                            "Read {} UTF-8 bytes. The exact text is retained in the canonical tool result and omitted from this model projection.",
                            text.0.len()
                        ),
                        work,
                    )
                }
            };
        }
        let text: String = decode(&result.value, None)?.0;
        match self.0 {
            super::schema::ProjectionMode::Full => super::schema::project_json(&text, None),
            super::schema::ProjectionMode::Reference => {
                let input: ReadFileInput = decode(&invocation.arguments, None)?.0;
                input.file.descriptor().verify(text.as_bytes(), &mut None)?;
                super::schema::project_reference(
                    &input.file,
                    &format!(
                        "Read {} UTF-8 bytes. The exact text is retained in the canonical tool result and omitted from this model projection.",
                        text.len()
                    ),
                    None,
                )
            }
        }
    }
}

impl ToolProjection for FileTool {
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
        match self {
            Self::Read => ReadFileProjection(super::schema::ProjectionMode::Full)
                .project(invocation, result, work),
            _ => FileResultProjection(super::schema::ProjectionMode::Full)
                .project(invocation, result, work),
        }
    }
}
