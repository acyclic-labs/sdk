//! The immutable source selection used by one executor composition.

use super::*;
use crate::{
    Error, OperationId,
    executor::PreparationIo,
    tool::{ToolDefinition, ToolRegistry},
};
use std::collections::BTreeSet;

/// Pinned instructions, available skills, selected invocations and exact tool schemas.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ContextManifest {
    /// Instructions are ordered from broad to specific scope; later entries take precedence.
    pub discovery: DiscoveredContext,
    /// Names in invocation order, resolving only through this manifest's pinned skill catalog.
    pub selected_skills: Vec<String>,
    /// Complete selected tool catalog before caller attenuation, with exact executor revisions.
    pub tools: Vec<ToolDefinition>,
}

impl ContextManifest {
    /// Captures the registry selection without copying any executors or granting authority.
    pub fn new(
        discovery: DiscoveredContext,
        selected_skills: Vec<String>,
        tools: &ToolRegistry,
    ) -> Result<Self> {
        Ok(Self {
            discovery,
            selected_skills,
            tools: tools.definitions(None)?,
        })
    }

    /// Validates one bounded selection before reading skill bodies or changing model visibility.
    pub fn validate(&self, limits: Limits) -> Result<()> {
        limits.validate(&mut None)?;
        crate::executor::json::json_byte_length(self, limits.render_bytes, &mut None)?;
        self.discovery.validate(&mut None)?;
        for instruction in &self.discovery.instructions {
            limits.validate_file(instruction, &mut None)?;
        }
        let mut names = BTreeSet::new();
        for name in &self.selected_skills {
            if !names.insert(name) {
                return Err(Error::Conflict("manifest repeats a selected skill".into()));
            }
        }
        for skill in &self.discovery.skills {
            names.remove(&skill.name);
        }
        if !names.is_empty() {
            return Err(Error::NotFound(
                "manifest selected skill is not in its catalog".into(),
            ));
        }
        names.clear();
        for tool in &self.tools {
            tool.validate(&mut None)?;
            if !names.insert(&tool.name) {
                return Err(Error::Conflict("manifest repeats a tool name".into()));
            }
        }
        let count = self
            .discovery
            .instructions
            .len()
            .checked_add(usize::from(!self.discovery.skills.is_empty()))
            .and_then(|count| count.checked_add(self.selected_skills.len()))
            .ok_or_else(|| Error::Invalid("manifest message count overflows".into()))?;
        if count > limits.context_messages {
            return Err(Error::Invalid(
                "manifest exceeds context message bound".into(),
            ));
        }
        Ok(())
    }

    /// Encodes the validated selection with canonical object keys and explicit array order.
    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>> {
        self.validate(limits)?;
        crate::contract::canonical_json_bytes(self)
    }

    /// Decodes bounded, untrusted JSON through the existing lexical admission boundary.
    pub fn decode(bytes: &[u8], limits: Limits) -> Result<Self> {
        use crate::executor::json::{JsonBounds, json_from_slice_bounded};
        limits.validate(&mut None)?;
        let manifest: Self = json_from_slice_bounded(
            std::marker::PhantomData,
            bytes,
            JsonBounds {
                maximum_bytes: limits.render_bytes,
                maximum_items: limits.render_bytes,
                maximum_depth: 256,
            },
            limits.render_bytes,
            None,
            None,
            &mut |_| Ok(()),
        )?;
        manifest.validate(limits)?;
        Ok(manifest)
    }

    /// Publishes the sole manifest through its caller-selected volume provider.
    pub async fn publish(
        &self,
        publisher: &dyn ContentPublisher,
        operation: OperationId,
        path: &str,
        limits: Limits,
    ) -> Result<FileRef> {
        let bytes = self.encode(limits)?;
        crate::conversation::validate_content_path(path, &mut None)?;
        if path.len() > limits.path_bytes || bytes.len() as u64 > limits.file_bytes {
            return Err(Error::Invalid(
                "context manifest publication exceeds file bounds".into(),
            ));
        }
        let (reference, _) = publisher
            .stage(
                operation,
                path,
                &bytes,
                "application/json",
                "context-manifest.json",
                None,
            )?
            .await?;
        reference.descriptor().verify(&bytes, &mut None)?;
        limits.validate_file(&reference, &mut None)?;
        if reference.volume() != publisher.volume()
            || reference.path() != path
            || reference.descriptor().media_type() != "application/json"
        {
            return Err(Error::Storage(
                "manifest publisher changed its destination".into(),
            ));
        }
        Ok(reference)
    }

    async fn load(
        reference: &FileRef,
        reader: &dyn ContentResidencyVerifier,
        limits: Limits,
    ) -> Result<Self> {
        limits.validate(&mut None)?;
        limits.validate_file(reference, &mut None)?;
        if reference.descriptor().media_type() != "application/json"
            || reference.descriptor().byte_length() > limits.render_bytes
        {
            return Err(Error::Invalid(
                "context manifest exceeds JSON/render bounds".into(),
            ));
        }
        let bytes = reader.read(reference, limits.render_bytes, None)?.await?;
        reference.descriptor().verify(&bytes, &mut None)?;
        Self::decode(&bytes, limits)
    }

    async fn context(
        &self,
        reference: FileRef,
        reader: &dyn ContentResidencyVerifier,
        limits: Limits,
    ) -> Result<Context> {
        let mut messages = self.discovery.messages(limits, None)?;
        let mut remaining = limits
            .render_bytes
            .checked_sub(crate::executor::json::json_byte_length(
                &messages,
                limits.render_bytes,
                &mut None,
            )?)
            .ok_or_else(|| Error::Invalid("manifest exceeds context byte bound".into()))?;
        let skills: BTreeMap<_, _> = if self.selected_skills.is_empty() {
            BTreeMap::new()
        } else {
            self.discovery
                .skills
                .iter()
                .map(|skill| (&skill.name, skill))
                .collect()
        };
        for name in &self.selected_skills {
            let skill = skills
                .get(name)
                .ok_or_else(|| Error::NotFound("manifest selected skill disappeared".into()))?;
            let message = skill
                .invoke(reader, limits.file_bytes.min(remaining))
                .await?;
            let bytes = crate::executor::json::json_byte_length(&message, remaining, &mut None)?;
            remaining = remaining
                .checked_sub(bytes)
                .ok_or_else(|| Error::Invalid("manifest exceeds context byte bound".into()))?;
            messages.push(message);
        }
        let context = Context {
            messages,
            metadata: BTreeMap::from([("context.manifest".into(), reference)]),
            current_input_index: None,
        };
        validate_projected_context(&context, limits, None)?;
        Ok(context)
    }

    /// Binds one immutable selection to its registered executors and reusable context stage.
    pub async fn bind(
        reference: FileRef,
        reader: &dyn ContentResidencyVerifier,
        mut tools: ToolRegistry,
        limits: Limits,
    ) -> Result<(ToolRegistry, Arc<dyn ContextStage>)> {
        let manifest = Self::load(&reference, reader, limits).await?;
        // Reject mismatched executors before fetching any selected skill bodies.
        tools.pin_model_definitions(&manifest.tools)?;
        let context = manifest.context(reference, reader, limits).await?;
        Ok((tools, Arc::new(ManifestStage { context })))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

struct ManifestStage {
    context: Context,
}

impl ContextStage for ManifestStage {
    fn name(&self) -> &str {
        "context-manifest"
    }

    fn contract(&self, io: Option<&PreparationIo>) -> Result<Value> {
        context_contract(&self.context.metadata, io)
    }

    fn apply<'a>(
        &'a self,
        _: &'a ContextInput,
        context: Context,
        limits: Limits,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Context>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            selection::place_snapshot(
                context,
                &self.context,
                ContextPlacement::Prepend,
                limits,
                io,
            )
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool {
        true
    }
}
