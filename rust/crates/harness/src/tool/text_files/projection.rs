use super::*;

/// Independently selectable read projector and its complete generated contract.
#[derive(Clone, Copy)]
pub struct ReadProjection(pub ProjectionMode);

/// Independently selectable search projector and its complete generated contract.
#[derive(Clone, Copy)]
pub struct SearchProjection(pub ProjectionMode);

impl ReadProjection {
    /// Complete projection envelope schema, owning all nested definitions.
    pub fn schema(self) -> Result<Value> {
        match self.0 {
            ProjectionMode::Full => super::schema::json_projection::<ReadResult>(),
            ProjectionMode::Reference => super::schema::reference_projection(),
        }
    }
}

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
        _work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = _work {
            let value = super::decode::<ReadResult>(&result.value, Some(work))?;
            return match self.0 {
                ProjectionMode::Full => super::schema::project_json(&value.0, Some(work)),
                ProjectionMode::Reference => super::files::native::summary_reference(
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
                ),
            };
        }
        let value: ReadResult = decode(&result.value, None)?.0;
        match self.0 {
            ProjectionMode::Full => super::schema::project_json(&value, None),
            ProjectionMode::Reference => super::schema::project_reference(
                &value.file,
                &format!(
                    "Read bytes {}..{}; {} source bytes omitted before and {} after. Selected text is retained in the canonical tool result and omitted from this model projection.",
                    value.selection.range.start,
                    value.selection.range.end,
                    value.selection.omitted_before,
                    value.selection.omitted_after,
                ),
                None,
            ),
        }
    }
}

impl SearchProjection {
    /// Complete projection envelope schema, owning all nested definitions.
    pub fn schema(self) -> Result<Value> {
        match self.0 {
            ProjectionMode::Full => super::schema::json_projection::<SearchResult>(),
            ProjectionMode::Reference => super::schema::reference_projection(),
        }
    }
}

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
        _work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = _work {
            let value = super::decode::<SearchResult>(&result.value, Some(work))?;
            return match self.0 {
                ProjectionMode::Full => super::schema::project_json(&value.0, Some(work)),
                ProjectionMode::Reference => super::files::native::summary_reference(
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
                ),
            };
        }
        let value: SearchResult = decode(&result.value, None)?.0;
        match self.0 {
            ProjectionMode::Full => super::schema::project_json(&value, None),
            ProjectionMode::Reference => super::schema::project_reference(
                &value.file,
                &format!(
                    "Literal search completed: {} total matches, {} retained positions, {} positions omitted from the canonical result, {} byte comparisons. Query and retained positions are retained in the canonical tool result and omitted from this model projection.",
                    value.matches.total_matches,
                    value.matches.matches.len(),
                    value.matches.omitted_matches,
                    value.matches.work,
                ),
                None,
            ),
        }
    }
}
