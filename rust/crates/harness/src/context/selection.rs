//! Pinned source adapters for the existing ordered context pipeline.

use super::{Context, ContextInput, ContextPlacement, ContextStage};
use crate::{
    Error, Result,
    conversation::{ContentResidencyVerifier, FileRef, Limits},
    model::ModelMessage,
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// An authoritative attribute state, shared by prompt rebuilds and updates.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
pub struct ContextAttribute {
    /// Registered, namespaced application type (roles are ordinary attributes).
    pub type_name: String,
    /// Exact type/renderer definition revision.
    pub type_revision: String,
    /// Schema of this attribute type.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmModelJsonSchema"))]
    pub schema: Value,
    /// Exact authoritative state revision.
    pub state_revision: String,
    /// State satisfying the registered schema.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmModelJsonValue"))]
    pub value: Value,
}

impl ContextAttribute {
    /// Captures a native typed value without changing its authoritative state.
    pub fn typed<T: Serialize>(
        type_name: String,
        type_revision: String,
        schema: Value,
        state_revision: String,
        value: &T,
    ) -> Result<Self> {
        let attribute = Self {
            type_name,
            type_revision,
            schema,
            state_revision,
            value: serde_json::to_value(value)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        };
        attribute.validate()?;
        Ok(attribute)
    }

    /// Validates type identity, revision and state using the existing schema boundary.
    pub fn validate(&self) -> Result<()> {
        if crate::contract::canonical_json_bytes(self)?.len() as u64
            > crate::model::MAX_MODEL_REQUEST_BYTES
        {
            return Err(Error::Invalid(
                "context attribute exceeds wire bound".into(),
            ));
        }
        for label in [&self.type_name, &self.type_revision, &self.state_revision] {
            crate::contract::validate_component_label(label, "context attribute identity")?;
        }
        crate::tool::validate_value(&self.schema, &self.value, "context attribute")
    }
}

/// A pinned input; a file reference conveys no read authority.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
pub enum ContextSourceValue {
    /// History, instructions, skills, catalogs or resources in immutable storage.
    File {
        /// Exact generation and descriptor.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
        file: FileRef,
    },
    /// Schema-validated application state.
    Attribute {
        /// Immutable typed state record.
        attribute: ContextAttribute,
    },
}

/// A declared representation; summary creation remains an admitted model operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
pub enum ContextRepresentation {
    /// Render the selected content.
    Full,
    /// Render a separately supplied, pinned summary.
    Summary,
    /// Render an explicit reference.
    Reference,
}

/// Explicit bounded selection from a pinned source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
pub enum ContextExtent {
    /// The complete source.
    Whole,
    /// Half-open byte span of a file. Renderers define decoding semantics.
    Span {
        /// Inclusive start.
        start: u64,
        /// Exclusive end.
        end: u64,
    },
}

/// Host-approved selection and representation. Model suggestions need host policy approval.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
pub struct ContextSelection {
    /// Pinned authoritative input.
    pub source: ContextSourceValue,
    /// Exact selected extent.
    pub extent: ContextExtent,
    /// Requested representation.
    pub representation: ContextRepresentation,
}

impl ContextSelection {
    /// Validates structure without granting authority or executing a renderer.
    pub fn validate(&self, limits: Limits) -> Result<()> {
        limits.validate()?;
        if crate::contract::canonical_json_bytes(self)?.len() as u64 > limits.render_bytes {
            return Err(Error::Invalid(
                "context selection exceeds input bound".into(),
            ));
        }
        match &self.source {
            ContextSourceValue::File { file } => {
                limits.validate_file(file)?;
                if let ContextExtent::Span { start, end } = self.extent
                    && (start >= end || end > file.descriptor().byte_length())
                {
                    return Err(Error::Invalid("context span is outside pinned file".into()));
                }
            }
            ContextSourceValue::Attribute { attribute } => {
                attribute.validate()?;
                if self.extent != ContextExtent::Whole {
                    return Err(Error::Invalid("attributes require whole selection".into()));
                }
            }
        }
        Ok(())
    }
}

/// Rendering mode for the same immutable source state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
pub enum ContextRenderMode {
    /// Rebuild the component in its configured prompt position.
    Prompt,
    /// Append an authoritative notification after cached context.
    Update,
}

/// Replaceable typed renderer; transformations compose as ordinary later stages.
pub trait ContextRenderer: Send + Sync {
    /// Immutable implementation identity included in execution admission.
    fn contract(&self) -> Value;
    /// Renders a host-approved selection; must honor supplied finite limits.
    fn render<'a>(
        &'a self,
        selection: &'a ContextSelection,
        mode: ContextRenderMode,
        input: &'a ContextInput,
        limits: Limits,
    ) -> BoxFuture<'a, Result<Vec<ModelMessage>>>;
}

/// Selection/renderer adapter, reusing the existing stage engine and content verifier.
pub struct SelectionStage {
    name: String,
    selection: ContextSelection,
    renderer: Arc<dyn ContextRenderer>,
    verifier: Arc<dyn ContentResidencyVerifier>,
    mode: ContextRenderMode,
    placement: ContextPlacement,
    limits: Limits,
}

impl SelectionStage {
    /// Registers a pinned source and renderer. Construction starts no reads/effects.
    pub fn new(
        name: String,
        selection: ContextSelection,
        renderer: Arc<dyn ContextRenderer>,
        verifier: Arc<dyn ContentResidencyVerifier>,
        mode: ContextRenderMode,
        placement: ContextPlacement,
        limits: Limits,
    ) -> Result<Self> {
        crate::contract::validate_component_label(&name, "selection stage")?;
        selection.validate(limits)?;
        if !renderer.contract().is_object() {
            return Err(Error::Invalid(
                "context renderer contract must be an object".into(),
            ));
        }
        Ok(Self {
            name,
            selection,
            renderer,
            verifier,
            mode,
            placement,
            limits,
        })
    }
}

impl ContextStage for SelectionStage {
    fn name(&self) -> &str {
        &self.name
    }

    fn contract(&self) -> Value {
        serde_json::json!({"name": self.name, "selection": self.selection,
            "renderer": self.renderer.contract(), "mode": self.mode,
            "placement": match self.placement { ContextPlacement::Prepend => "prepend", ContextPlacement::Append => "append" },
            "limits": self.limits})
    }

    fn apply<'a>(
        &'a self,
        input: &'a ContextInput,
        context: Context,
    ) -> BoxFuture<'a, Result<Context>> {
        Box::pin(async move {
            if let ContextSourceValue::File { file } = &self.selection.source {
                self.limits.validate_file(file)?;
                self.verifier.verify(file).await?;
            }
            let messages = self
                .renderer
                .render(&self.selection, self.mode, input, self.limits)
                .await?;
            apply_context_projection(context, messages, self.mode, self.placement, self.limits)
        })
    }
}

/// Validates and places renderer output; shared by native stages and narrow WASM bindings.
/// This is a projection only and grants no authority or model admission.
pub fn apply_context_projection(
    context: Context,
    messages: Vec<ModelMessage>,
    mode: ContextRenderMode,
    placement: ContextPlacement,
    limits: Limits,
) -> Result<Context> {
    let placement = if mode == ContextRenderMode::Update {
        ContextPlacement::Append
    } else {
        placement
    };
    let context = place_messages(context, messages, placement);
    validate_projected_context(&context, limits)?;
    Ok(context)
}

pub(super) fn place_messages(
    mut context: Context,
    mut messages: Vec<ModelMessage>,
    placement: ContextPlacement,
) -> Context {
    match placement {
        ContextPlacement::Prepend => {
            messages.append(&mut context.messages);
            context.messages = messages;
        }
        ContextPlacement::Append => context.messages.append(&mut messages),
    }
    context
}

/// Validates finite projected context bounds, never silently omitting mandatory data.
pub fn validate_projected_context(context: &Context, limits: Limits) -> Result<()> {
    limits.validate()?;
    if context.messages.len() > limits.context_messages
        || context.metadata.len() > limits.attachments
        || crate::contract::canonical_json_bytes(context)?.len() as u64 > limits.render_bytes
    {
        return Err(Error::Invalid(
            "context projection exceeded declared bounds".into(),
        ));
    }
    for message in &context.messages {
        message.content.validate_limits(limits)?;
    }
    for file in context.metadata.values() {
        limits.validate_file(file)?;
    }
    Ok(())
}
