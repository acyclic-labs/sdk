//! The immutable source selection used by one executor composition.

use super::*;
use crate::{
    Error, OperationId,
    executor::PreparationIo,
    tool::{ToolDefinition, ToolRegistry},
};

mod binding;

/// Pinned instructions, available skills, selected invocations and exact tool schemas.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ContextManifest {
    /// Declarative conventions pinned in this manifest file, before discovered instructions.
    #[serde(default)]
    pub conventions: Vec<String>,
    /// Instructions are ordered from broad to specific scope; later entries take precedence.
    pub discovery: DiscoveredContext,
    /// Names in invocation order, resolving only through this manifest's pinned skill catalog.
    pub selected_skills: Vec<String>,
    /// Pinned configuration inputs (such as hooks and MCP settings), never prompt text.
    #[serde(default)]
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire[]"))]
    pub sources: Vec<FileRef>,
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
        Self::for_files(discovery, selected_skills, tools, true, true)
    }

    pub(crate) fn for_files(
        discovery: DiscoveredContext,
        selected_skills: Vec<String>,
        tools: &ToolRegistry,
        goal: bool,
        plans: bool,
    ) -> Result<Self> {
        let defaults: [&str; 2] = serde_json::from_str(include_str!("instructions.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(Self {
            conventions: defaults
                .into_iter()
                .zip([goal, plans])
                .filter(|(_, enabled)| *enabled)
                .map(|(text, _)| text.to_owned())
                .collect(),
            discovery,
            selected_skills,
            sources: Vec::new(),
            tools: tools.definitions(None)?,
        })
    }

    /// Validates one bounded selection before reading skill bodies or changing model visibility.
    pub fn validate(&self, limits: Limits) -> Result<()> {
        self.validate_observed(limits, None)
    }

    fn validate_observed(&self, limits: Limits, io: Option<&PreparationIo>) -> Result<()> {
        use crate::{executor::stock_boundary, wire::ErrorCode};
        crate::executor::with_history_io(io, |work| -> Result<()> {
            limits.validate(work)?;
            crate::executor::json::json_byte_length(self, limits.render_bytes, work)?;
            self.discovery.validate(work)?;
            for file in self
                .discovery
                .instructions
                .iter()
                .map(|instruction| &**instruction)
                .chain(&self.sources)
            {
                limits.validate_file(file, work)?;
            }
            for tool in &self.tools {
                tool.validate(work)?;
            }
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let mut names = crate::core::reserve_copy_vector::<&str>(
                self.selected_skills.len().max(self.tools.len()),
                &mut quota,
            )?;
            names.extend(self.selected_skills.iter().map(String::as_str));
            names.sort_unstable();
            if names.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(stock_boundary(
                    io,
                    ErrorCode::Conflict,
                    "manifest repeats a selected skill",
                ));
            }
            let mut matched = 0;
            for skill in &self.discovery.skills {
                if skill.source.path.len() > limits.path_bytes {
                    return Err(stock_boundary(
                        io,
                        ErrorCode::Invalid,
                        "manifest skill path exceeds path bound",
                    ));
                }
                matched += usize::from(names.binary_search(&skill.name.as_str()).is_ok());
            }
            if matched != names.len() {
                return Err(stock_boundary(
                    io,
                    ErrorCode::NotFound,
                    "manifest selected skill is not in its catalog",
                ));
            }
            names.clear();
            names.extend(self.tools.iter().map(|tool| tool.name.as_str()));
            names.sort_unstable();
            if names.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(stock_boundary(
                    io,
                    ErrorCode::Conflict,
                    "manifest repeats a tool name",
                ));
            }
            let count = self
                .discovery
                .instructions
                .len()
                .checked_add(self.conventions.len())
                .and_then(|count| count.checked_add(usize::from(!self.discovery.skills.is_empty())))
                .and_then(|count| count.checked_add(self.selected_skills.len()))
                .ok_or_else(|| {
                    stock_boundary(io, ErrorCode::Invalid, "manifest message count overflows")
                })?;
            if count > limits.context_messages {
                return Err(stock_boundary(
                    io,
                    ErrorCode::Invalid,
                    "manifest exceeds context message bound",
                ));
            }
            Ok(())
        })
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

    /// Binds one immutable selection to its registered executors and reusable context stage.
    pub async fn bind(
        reference: FileRef,
        reader: &dyn ContentResidencyVerifier,
        tools: ToolRegistry,
        limits: Limits,
    ) -> Result<(ToolRegistry, Arc<dyn ContextStage>)> {
        Self::bind_observed(reference, reader, tools, limits, None).await
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

struct ManifestStage {
    context: Context,
    sources: Vec<FileRef>,
}

impl ContextStage for ManifestStage {
    fn name(&self) -> &str {
        "context-manifest"
    }

    fn configuration_sources(&self) -> &[FileRef] {
        &self.sources
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
