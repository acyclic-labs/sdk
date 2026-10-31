//! Callable metadata projection into the original tool registry.

use super::*;
use crate::{
    executor::PreparationIo,
    tool::{SkillRuntime, Tool, ToolDefinition, ToolRegistry},
};

// The fixed original JsonToolProjection envelope accepts any successful JSON
// value. Decode its literal through the ordinary bounded codec on native work.
const PROJECTION: &[u8] = br#"{"type":"object","properties":{"kind":{"const":"json"},"value":true},"required":["kind","value"],"additionalProperties":false}"#;

impl SkillMetadata {
    /// Registers one admitted callable declaration on the original registry.
    /// Instruction-only metadata returns false. No source or script is read;
    /// the caller must publish the catalog and registry at its existing atomic
    /// admission boundary before making this revision available to a request.
    #[doc(hidden)]
    pub fn register_callable(
        &self,
        registry: &mut ToolRegistry,
        custody: &mut crate::NativeCustody,
        work: crate::NativeWork<'_>,
    ) -> Result<bool> {
        let io = work.map(PreparationIo::from_native_work).transpose()?;
        let Some(tool) = self.callable_tool(io.as_ref())? else {
            return Ok(false);
        };
        let payload = if let Some(io) = io {
            use crate::filesystem::UploadRecord as _;
            let retained = crate::core::owned_sum([
                std::mem::size_of::<ToolDefinition>() as u64,
                tool.definition.logical_owned_bytes()?,
                tool.skill_source
                    .as_ref()
                    .map_or(Ok(0), |source| source.logical_owned_bytes())?,
                crate::executor::filesystem_skill_marker_bytes(),
            ])?;
            Some(io.into_native_custody(retained)?)
        } else {
            None
        };
        registry.register((tool, payload), custody, work)?;
        Ok(true)
    }

    fn callable_tool(&self, io: Option<&PreparationIo>) -> Result<Option<Tool>> {
        let fields = crate::executor::with_history_io(io, |work| self.call_fields(work))?;
        let Some(fields) = fields else {
            return Ok(None);
        };
        let runtime: SkillRuntime = match io {
            Some(io) => io.value_decode(fields.runtime, crate::wire::ErrorCode::Invalid)?,
            None => SkillRuntime::deserialize(fields.runtime)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        };
        let revision = crate::executor::with_history_io(io, |work| {
            let digest = crate::executor::json::canonical_json_digest(self, u64::MAX, work)?;
            crate::conversation::content_label("skill-", digest, work)
        })?;
        if let Some(io) = io {
            io.retain_owned(std::mem::size_of::<ToolDefinition>() as u64)?;
        }
        let projection_schema = match io {
            Some(io) => crate::filesystem::retain_decoded(io.decode(
                std::marker::PhantomData::<serde_json::Value>,
                PROJECTION,
                crate::wire::ErrorCode::Invalid,
            )?)?,
            None => serde_json::from_slice(PROJECTION)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        };
        let definition = ToolDefinition {
            name: crate::executor::stock_string_copy(&self.name, io)?,
            revision,
            description: crate::executor::stock_string_copy(&self.description, io)?,
            input_schema: crate::model::clone_value(fields.input_schema, io)?,
            output_schema: serde_json::Value::Bool(true),
            projection_schema,
        };
        Tool::filesystem_skill(definition, self.source.copy(io)?, runtime, io).map(Some)
    }
}
