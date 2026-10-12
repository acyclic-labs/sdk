use super::*;

/// Replaceable projector for the canonical published file result.
#[derive(Clone, Copy)]
pub struct FileResultProjection(pub super::schema::ProjectionMode);

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
        work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        let value = decode::<FileResult>(&result.value, work)?;
        match self.0 {
            super::schema::ProjectionMode::Full => super::schema::project_json(&value.0, work),
            super::schema::ProjectionMode::Reference => super::schema::project_reference(
                &value.0.file,
                "File publication completed. The original output reference, workspace and content identity remain in the canonical tool result. Body bytes are omitted from this model projection.",
                work,
            ),
        }
    }
}

/// Replaceable projector for the exact reader's unchanged canonical string.
#[derive(Clone, Copy)]
pub struct ReadFileProjection(pub super::schema::ProjectionMode);

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
        work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        let text = decode::<String>(&result.value, work)?;
        if self.0 == super::schema::ProjectionMode::Full {
            return super::schema::project_json(&text.0, work);
        }
        let input = decode::<ReadFileInput>(&invocation.arguments, work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            input.0.file.descriptor().verify(
                text.0.as_bytes(),
                &mut Some(&mut crate::store::HistoryWork::new(
                    crate::filesystem::PreparationIo::from_native_work(work)?,
                )),
            )?;
            return native::summary_reference(
                &input.0.file,
                160,
                format_args!(
                    "Read {} UTF-8 bytes. The exact text is retained in the canonical tool result and omitted from this model projection.",
                    text.0.len()
                ),
                work,
            );
        }
        input
            .0
            .file
            .descriptor()
            .verify(text.0.as_bytes(), &mut None)?;
        super::schema::project_reference(
            &input.0.file,
            &format!(
                "Read {} UTF-8 bytes. The exact text is retained in the canonical tool result and omitted from this model projection.",
                text.0.len()
            ),
            None,
        )
    }
}
