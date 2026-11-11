use super::*;

/// Replaceable mapping from tool results into model-visible context.
pub trait ToolProjection: acyclic_stream::ProviderPlatform {
    /// Produces the complete serialized `ToolResultContent` envelope without effects.
    fn project(
        &self,
        invocation: &ToolInvocation,
        result: &ToolResult,
        work: acyclic_fs::NativeWork<'_>,
    ) -> Result<(Value, acyclic_fs::NativeCustody)>;

    /// Rejects an unaudited projector before dispatch, not after its effects.
    fn supports_native_projection(&self, _work: acyclic_fs::NativeWork<'_>) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if _work.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "tool projection has no native producer",
            });
        }
        Ok(())
    }
}

/// First-party complete JSON projection for registered SDK tools, including MCP.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonToolProjection;

impl ToolProjection for JsonToolProjection {
    fn project(
        &self,
        _invocation: &ToolInvocation,
        result: &ToolResult,
        work: acyclic_fs::NativeWork<'_>,
    ) -> Result<(Value, acyclic_fs::NativeCustody)> {
        schema::project_json(&result.value, work)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_projection(&self, work: acyclic_fs::NativeWork<'_>) -> Result<()> {
        if let Some(work) = work {
            work.remaining()?;
        }
        Ok(())
    }
}
