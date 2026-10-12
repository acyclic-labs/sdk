//! Pinned source adapters for the existing ordered context pipeline.

use super::{Context, ContextInput, ContextPlacement, ContextStage};
use crate::{
    Error, Result,
    conversation::{ContentResidencyVerifier, FileRef, Limits},
    model::ModelMessage,
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// An authoritative attribute state, shared by prompt rebuilds and updates.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
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
        self.validate_with_work(&mut None)
    }

    fn validate_with_work(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.preflight_source(work)?;
        self.validate_preflighted(work)
    }

    fn preflight_source(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if selection_has_native_work(work) {
            self.validate_identity(work)?;
            observe_selection_value(&self.schema, work)?;
            observe_selection_value(&self.value, work)?;
        }
        Ok(())
    }

    fn validate_identity(
        &self,
        work: &Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        for label in [&self.type_name, &self.type_revision, &self.state_revision] {
            if !crate::is_valid_component_label(label) {
                return Err(selection_boundary(work,
                    "context attribute identity is invalid"));
            }
        }
        Ok(())
    }

    fn validate_preflighted(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        crate::contract::validate_trusted_json_byte_bound(
            self, crate::model::MAX_MODEL_REQUEST_BYTES, work,
        )?;
        self.validate_identity(work)?;
        crate::contract::validate_json_schema_value_with_work(
            &self.schema, &self.value, "context attribute", work,
        )
    }

    /// Validates the same pinned state using the original admitted native work.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn validate_with_native_work(
        &self,
        native: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut work = crate::store::history::native_work_io(native)
            .map_err(super::native_context_work_error)?;
        self.validate_with_work(&mut Some(&mut work))
    }
}

/// A pinned input; a file reference conveys no read authority.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
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
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
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
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
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
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
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
        self.validate_with_work(limits, &mut None)
    }

    fn validate_with_work(
        &self,
        limits: Limits,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        limits.validate(work)?;
        let native = selection_has_native_work(work);
        if native {
            if let Some(observer) = work.as_deref_mut() {
                observer.check_cancellation()?;
            }
            match &self.source {
                ContextSourceValue::File { file } => limits.validate_file(file, work)?,
                ContextSourceValue::Attribute { attribute } => attribute.preflight_source(work)?,
            }
        }
        crate::contract::validate_trusted_json_byte_bound(self, limits.render_bytes, work)?;
        match &self.source {
            ContextSourceValue::File { file } => {
                if !native {
                    limits.validate_file(file, work)?;
                }
                if let ContextExtent::Span { start, end } = self.extent
                    && (start >= end || end > file.descriptor().byte_length())
                {
                    return Err(selection_boundary(work,
                        "context span is outside pinned file"));
                }
            }
            ContextSourceValue::Attribute { attribute } => {
                attribute.validate_preflighted(work)?;
                if self.extent != ContextExtent::Whole {
                    return Err(selection_boundary(work,
                        "attributes require whole selection"));
                }
            }
        }
        Ok(())
    }

    /// Validates structure without granting new authority on the original ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn validate_with_native_work(
        &self,
        limits: Limits,
        native: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut work = crate::store::history::native_work_io(native)
            .map_err(super::native_context_work_error)?;
        self.validate_with_work(limits, &mut Some(&mut work))
    }

}

fn selection_has_native_work(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    { work.as_deref().is_some_and(|observer| observer.original_native_work().is_some()) }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    { let _ = work; false }
}

fn selection_boundary(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if selection_has_native_work(work) {
        return crate::executor::native_boundary(crate::wire::ErrorCode::Invalid, reason);
    }
    let _ = work;
    Error::Invalid(reason.into())
}

// Audit the actual borrowed SDK Value collections before counting their closed DTO serialization.
fn observe_selection_value(
    value: &Value,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if selection_has_native_work(work) {
        let observer = work.as_deref_mut().expect("native selection observer");
        let remaining = observer.remaining()?;
        crate::contract::json_owned_value_shape_bounded(
            value,
            crate::contract::JsonBounds {
                maximum_bytes: remaining.bytes_encoded,
                maximum_items: remaining.items_examined,
                maximum_depth: 256,
            },
            crate::contract::JsonValueSchema::None,
            &mut |charge| observer.codec_charge(charge),
        )?;
        return Ok(());
    }
    let _ = (value, work);
    Ok(())
}

/// Rendering mode for the same immutable source state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ContextRenderMode {
    /// Rebuild the component in its configured prompt position.
    Prompt,
    /// Append an authoritative notification after cached context.
    Update,
}

/// Replaceable typed renderer; transformations compose as ordinary later stages.
pub trait ContextRenderer: acyclic_stream::ProviderPlatform {
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

    /// Explicit capability; ordinary renderers are rejected before a callback or box.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool { false }

    /// Constructs the same immutable identity with its actual native custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn contract_with_native_work(
        &self,
        _: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        Err(crate::executor::native_boundary(crate::wire::ErrorCode::Unsupported,
            "context renderer has no native contract capability"))
    }

    /// Renders the same selection after funding the concrete future frame.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn render_native<'a>(
        &'a self,
        _: &'a ContextSelection,
        _: ContextRenderMode,
        _: &'a ContextInput,
        _: Limits,
        _: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<ModelMessage>, acyclic_fs::NativeAllocation)>> {
        Err(crate::executor::native_boundary(crate::wire::ErrorCode::Unsupported,
            "context renderer has no native capability"))
    }
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
        limits: Limits,
    ) -> BoxFuture<'a, Result<Context>> {
        Box::pin(async move {
            let limits = super::restrict_context_limits(self.limits, limits)?;
            super::validate_projected_context_with_io(&context, limits, None)?;
            self.selection.validate(limits)?;
            if let ContextSourceValue::File { file } = &self.selection.source {
                limits.validate_file(file, &mut None)?;
                self.verifier.verify(file).await?;
            }
            let messages = self
                .renderer
                .render(&self.selection, self.mode, input, limits)
                .await?;
            apply_context_projection(context, messages, self.mode, self.placement, limits)
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool { self.renderer.supports_native() }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn contract_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        self.validate_native(work)?;
        work.remaining().map_err(super::native_context_work_error)?;
        let renderer = self.renderer.contract_with_native_work(work)?;
        if !renderer.0.is_object() {
            return Err(crate::executor::native_boundary(crate::wire::ErrorCode::Invalid,
                "context renderer contract must be an object"));
        }
        let mut source_io = crate::store::history::native_work_io(work)
            .map_err(super::native_context_work_error)?;
        observe_selection_value(&renderer.0, &mut Some(&mut source_io))?;
        #[derive(Serialize)]
        struct Contract<'a> {
            name: &'a str,
            selection: &'a ContextSelection,
            renderer: &'a Value,
            mode: ContextRenderMode,
            placement: &'a str,
            limits: Limits,
        }
        let placement = match self.placement {
            ContextPlacement::Prepend => "prepend",
            ContextPlacement::Append => "append",
        };
        // Borrow the renderer's actual guarded payload until the funded copy finishes.
        let contract = super::context_contract_native(&Contract {
            name: &self.name,
            selection: &self.selection,
            renderer: &renderer.0,
            mode: self.mode,
            placement,
            limits: self.limits,
        }, work)?;
        drop(renderer);
        Ok(contract)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn validate_native(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining().map_err(super::native_context_work_error)?;
        if !crate::is_valid_component_label(&self.name) {
            return Err(crate::executor::native_boundary(crate::wire::ErrorCode::Invalid,
                "selection stage identity is invalid"));
        }
        if !self.renderer.supports_native() {
            return Err(crate::executor::native_boundary(crate::wire::ErrorCode::Unsupported,
                "context renderer has no native capability"));
        }
        self.selection.validate_with_native_work(self.limits, work)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn apply_native<'a>(
        &'a self,
        input: &'a ContextInput,
        context: (Context, acyclic_fs::NativeAllocation),
        limits: Limits,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Context, acyclic_fs::NativeAllocation)>> {
        work.remaining().map_err(super::native_context_work_error)?;
        if !crate::is_valid_component_label(&self.name) {
            return Err(crate::executor::native_boundary(crate::wire::ErrorCode::Invalid,
                "selection stage identity is invalid"));
        }
        if !self.renderer.supports_native() {
            return Err(crate::executor::native_boundary(crate::wire::ErrorCode::Unsupported,
                "context renderer has no native capability"));
        }
        crate::NativeFutureBundle::new(work, || async move {
            let limits = super::restrict_context_limits_native(self.limits, limits, work)?;
            super::validate_projected_context_with_io(&context.0, limits, Some(&crate::executor::PreparationIo::from_native_work(work)?))?;
            self.selection.validate_with_native_work(limits, work)?;
            if let ContextSourceValue::File { file } = &self.selection.source {
                limits.validate_file_with_native_work(file, work)?;
                work.remaining().map_err(super::native_context_work_error)?;
                self.verifier.verify_with_native_work(file, limits.file_bytes, work)?.await?;
            }
            work.remaining().map_err(super::native_context_work_error)?;
            let messages = self.renderer.render_native(
                &self.selection, self.mode, input, limits, work,
            )?.await?;
            apply_context_projection_native(context, messages, self.mode, self.placement, limits, work)
        })
    }
}

/// Validates and places renderer output; shared by native stages and narrow WASM bindings.
pub fn apply_context_projection(
    context: Context,
    messages: Vec<ModelMessage>,
    mode: ContextRenderMode,
    placement: ContextPlacement,
    limits: Limits,
) -> Result<Context> {
    let placement = projection_placement(mode, placement);
    let context = place_messages_with_io(context, messages, placement, limits, None)?;
    super::validate_projected_context_with_io(&context, limits, None)?;
    Ok(context)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn apply_context_projection_native(
    context: (Context, acyclic_fs::NativeAllocation),
    messages: (Vec<ModelMessage>, acyclic_fs::NativeAllocation),
    mode: ContextRenderMode,
    placement: ContextPlacement,
    limits: Limits,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Context, acyclic_fs::NativeAllocation)> {
    let context = place_messages_native(context, messages, projection_placement(mode, placement), limits, work)?;
    super::validate_projected_context_with_io(&context.0, limits, Some(&crate::executor::PreparationIo::from_native_work(work)?))?;
    Ok(context)
}

fn projection_placement(mode: ContextRenderMode, placement: ContextPlacement) -> ContextPlacement {
    if mode == ContextRenderMode::Update { ContextPlacement::Append } else { placement }
}

pub(crate) fn place_messages_with_io(
    context: Context, messages: Vec<ModelMessage>, placement: ContextPlacement,
    limits: Limits, io: Option<&crate::executor::PreparationIo>,
) -> Result<Context> {
    place_context(
        context, Context { messages, ..Context::default() }, placement, limits, io,
    )
}

pub(super) fn place_context(
    context: Context,
    contribution: Context,
    placement: ContextPlacement,
    limits: Limits,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<Context> {
    let marker = preflight_context_composition(&context, &contribution, placement, limits, io)?;
    compose_context_with_io(context, contribution, placement, marker, io)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn place_messages_native(
    context: (Context, acyclic_fs::NativeAllocation),
    messages: (Vec<ModelMessage>, acyclic_fs::NativeAllocation),
    placement: ContextPlacement,
    limits: Limits,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Context, acyclic_fs::NativeAllocation)> {
    place_context_native(context, (Context { messages: messages.0, ..Context::default() }, messages.1),
        placement, limits, work)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn place_context_native(
    context: (Context, acyclic_fs::NativeAllocation),
    contribution: (Context, acyclic_fs::NativeAllocation),
    placement: ContextPlacement,
    limits: Limits,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Context, acyclic_fs::NativeAllocation)> {
    let io = crate::executor::PreparationIo::from_native_work(work)?;
    let context_data = context.0;
    let contribution_data = contribution.0;
    io.absorb_native_custody(context.1)?;
    io.absorb_native_custody(contribution.1)?;
    let result = place_context(context_data, contribution_data, placement, limits, Some(&io))?;
    let retained = io.owned_bytes()?;
    Ok((result, io.into_native_custody(retained)?))
}

pub(super) fn place_snapshot(
    context: Context,
    snapshot: &Context,
    placement: ContextPlacement,
    limits: Limits,
) -> Result<Context> {
    let marker = preflight_context_composition(&context, snapshot, placement, limits, None)?;
    // Clone only after the complete message/metadata composition fits.
    compose_context_with_io(
        context,
        snapshot.clone(),
        placement,
        marker,
        None,
    )
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn place_snapshot_native(
    context: (Context, acyclic_fs::NativeAllocation),
    snapshot: &Context,
    placement: ContextPlacement,
    limits: Limits,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Context, acyclic_fs::NativeAllocation)> {
    let io = crate::executor::PreparationIo::from_native_work(work)?;
    let context_data = context.0;
    io.absorb_native_custody(context.1)?;
    let marker = preflight_context_composition(&context_data, snapshot, placement, limits, Some(&io))?;
    // Preflight the borrowed pinned source before funding and copying any source payload.
    let (snapshot, guard) = super::clone_context_native(snapshot, work)?;
    io.absorb_native_custody(guard)?;
    let result = compose_context_with_io(context_data, snapshot, placement, marker, Some(&io))?;
    let retained = io.owned_bytes()?;
    Ok((result, io.into_native_custody(retained)?))
}

struct JoinedMetadata<'a>(
    &'a std::collections::BTreeMap<String, FileRef>,
    &'a std::collections::BTreeMap<String, FileRef>,
);

impl Serialize for JoinedMetadata<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap as _;
        let mut map = serializer.serialize_map(None)?;
        for (name, file) in self.0.iter().chain(self.1) {
            map.serialize_entry(name, file)?;
        }
        map.end()
    }
}

fn preflight_context_composition(
    context: &Context,
    contribution: &Context,
    placement: ContextPlacement,
    limits: Limits,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<Option<u32>> {
    super::validate_context_projection_with_io(context, limits, io)?;
    super::validate_context_projection_with_io(contribution, limits, io)?;
    if context
        .messages
        .len()
        .checked_add(contribution.messages.len())
        .is_none_or(|count| count > limits.context_messages)
        || context
            .metadata
            .len()
            .checked_add(contribution.metadata.len())
            .is_none_or(|count| count > limits.attachments)
    {
        return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
            "context composition exceeds declared count bounds"));
    }
    if contribution
        .metadata
        .keys()
        .any(|name| context.metadata.contains_key(name))
    {
        return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Conflict,
            "context composition repeats a metadata key"));
    }
    let current_input_index = match (
        context.current_input_index,
        contribution.current_input_index,
    ) {
        (Some(_), Some(_)) => {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                "context composition has two active inputs"));
        }
        (Some(index), None) if placement == ContextPlacement::Prepend => Some(
            u32::try_from(contribution.messages.len())
                .ok()
                .and_then(|count| index.checked_add(count))
                .ok_or_else(|| {
                    crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                        "current input index exceeds portable count")
                })?,
        ),
        (None, Some(index)) if placement == ContextPlacement::Append => Some(
            u32::try_from(context.messages.len())
                .ok()
                .and_then(|count| index.checked_add(count))
                .ok_or_else(|| {
                    crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                        "current input index exceeds portable count")
                })?,
        ),
        (Some(index), None) | (None, Some(index)) => Some(index),
        (None, None) => None,
    };
    let (first, second) = match placement {
        ContextPlacement::Prepend => (
            contribution.messages.as_slice(),
            context.messages.as_slice(),
        ),
        ContextPlacement::Append => (
            context.messages.as_slice(),
            contribution.messages.as_slice(),
        ),
    };
    #[derive(Serialize)]
    struct JoinedContext<'a> {
        messages: super::ContextMessages<'a>,
        metadata: JoinedMetadata<'a>,
        current_input_index: Option<u32>,
    }
    super::validate_context_json_with_io(
        &JoinedContext {
            messages: super::ContextMessages {
                first,
                user: None,
                second,
            },
            metadata: JoinedMetadata(&context.metadata, &contribution.metadata),
            current_input_index,
        },
        limits.render_bytes,
        io,
    )?;
    Ok(current_input_index)
}

fn compose_context_with_io(
    mut context: Context,
    mut contribution: Context,
    placement: ContextPlacement,
    marker: Option<u32>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<Context> {
    context.current_input_index = marker;
    // Payloads and logical map entries move under their existing custody; opaque std nodes are excluded.
    context.metadata.extend(contribution.metadata);
    match placement {
        ContextPlacement::Prepend => {
            super::append_context_messages_with_io(&mut contribution.messages, &mut context.messages, io)?;
            context.messages = contribution.messages;
        }
        ContextPlacement::Append => {
            super::append_context_messages_with_io(&mut context.messages, &mut contribution.messages, io)?;
        }
    }
    Ok(context)
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod native_source_tests {
    use super::*;

    #[test]
    fn native_attribute_rejects_deep_source_before_schema_compilation_or_copy() {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
        let cancellation = CancellationToken::new();
        let budget = WorkBudget {
            allocation_operations: 1024,
            peak_allocation_bytes: 1_048_576,
            bytes_copied: 1_048_576,
            bytes_encoded: 1_048_576,
            items_examined: 8192,
            ..WorkBudget::default()
        };
        let mut value = Value::Null;
        for _ in 0..257 {
            value = Value::Array(vec![value]);
        }
        let mut attribute = ContextAttribute {
            type_name: "application:state".into(),
            type_revision: "1".into(),
            schema: Value::Bool(true),
            state_revision: "1".into(),
            value,
        };
        // The new finite native API does not narrow the original ordinary API.
        attribute.validate().expect("ordinary deep attribute remains legal");
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("original ledger");
        let before = work.receipt();
        let mut observer = crate::store::history::native_work_io(&work).expect("original observer");
        assert!(attribute.validate_with_work(&mut Some(&mut observer)).is_err());
        assert_eq!(work.receipt().bytes_copied, before.bytes_copied);
        assert_eq!(work.receipt().allocation_operations, before.allocation_operations);
        attribute.value = Value::Array(vec![Value::Null]);
        let control = NativeMaterializationWork::new(budget, &cancellation).expect("control ledger");
        let mut observer = crate::store::history::native_work_io(&control).expect("control observer");
        attribute.validate_with_work(&mut Some(&mut observer)).expect("bounded source is valid");
    }
}
