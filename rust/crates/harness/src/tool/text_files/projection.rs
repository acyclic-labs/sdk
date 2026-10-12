use super::*;

/// Independently selectable read projector and its complete generated contract.
#[derive(Clone, Copy)]
pub struct ReadProjection(pub ProjectionMode);

impl ToolProjection for ReadProjection {
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
        let value = decode::<ReadResult>(&result.value, work)?;
        if self.0 == ProjectionMode::Full {
            return super::schema::project_json(&value.0, work);
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            return super::files::native::summary_reference(
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
            );
        }
        super::schema::project_reference(
            &value.0.file,
            &format!(
                "Read bytes {}..{}; {} source bytes omitted before and {} after. Selected text is retained in the canonical tool result and omitted from this model projection.",
                value.0.selection.range.start,
                value.0.selection.range.end,
                value.0.selection.omitted_before,
                value.0.selection.omitted_after,
            ),
            None,
        )
    }
}

/// Independently selectable search projector and its complete generated contract.
#[derive(Clone, Copy)]
pub struct SearchProjection(pub ProjectionMode);

impl ToolProjection for SearchProjection {
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
        let value = decode::<SearchResult>(&result.value, work)?;
        if self.0 == ProjectionMode::Full {
            return super::schema::project_json(&value.0, work);
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            return super::files::native::summary_reference(
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
            );
        }
        super::schema::project_reference(
            &value.0.file,
            &format!(
                "Literal search completed: {} total matches, {} retained positions, {} positions omitted from the canonical result, {} byte comparisons. Query and retained positions are retained in the canonical tool result and omitted from this model projection.",
                value.0.matches.total_matches,
                value.0.matches.matches.len(),
                value.0.matches.omitted_matches,
                value.0.matches.work,
            ),
            None,
        )
    }
}
