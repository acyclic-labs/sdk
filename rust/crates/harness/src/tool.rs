//! Independently replaceable tool definitions, executors, and projections.

use crate::{
    Error, InteractionId, OperationId, Result,
    core::{AuthorityVerifier, Scope},
    registry::validate_component_label,
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

pub mod edit;
pub mod files;
pub mod patch;
pub mod schema;
pub mod text;
pub mod text_files;

// Decode the admitted JSON by reference: typed outputs own only their actual
// fields, without first cloning the complete arguments or canonical result tree.
fn decode<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T> {
    T::deserialize(value).map_err(|error| Error::Invalid(error.to_string()))
}

/// Decodes the same borrowed Value visitor under the original audited DTO quota.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn decode_with_native_work<T: crate::filesystem::UploadRecord>(
    value: &Value,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(T, acyclic_fs::NativeAllocation)> {
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    let value: T = io.value_decode(value, crate::wire::ErrorCode::Invalid)?;
    let retained = (std::mem::size_of::<T>() as u64)
        .checked_add(value.logical_owned_bytes()?)
        .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "tool DTO ownership overflow" })?;
    let custody = io.into_native_custody(retained)?;
    Ok((value, custody))
}

/// Copies the actual Value graph, including literal serde-private map keys.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn clone_value_with_native_work(
    value: &Value,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Value, acyclic_fs::NativeAllocation)> {
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    io.retain_owned(std::mem::size_of::<Value>() as u64)?;
    let copied = crate::model::model_value_clone_with_io(value, Some(&io))?;
    let retained = value_owned_bytes(&copied)?;
    let custody = io.into_native_custody(retained)?;
    Ok((copied, custody))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn clone_definition_with_native_work(
    value: &ToolDefinition,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(ToolDefinition, acyclic_fs::NativeAllocation)> {
    use crate::filesystem::UploadRecord as _;
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    io.retain_owned(std::mem::size_of::<ToolDefinition>() as u64)?;
    let copied = crate::model::model_tool_definition_clone_with_io(value, Some(&io))?;
    let retained = (std::mem::size_of::<ToolDefinition>() as u64)
        .checked_add(copied.logical_owned_bytes()?)
        .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "tool definition ownership overflow" })?;
    let custody = io.into_native_custody(retained)?;
    Ok((copied, custody))
}

/// Builds the canonical result tree through the existing bounded SDK codec.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn value_with_native_work<T: Serialize>(
    value: &T,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Value, acyclic_fs::NativeAllocation)> {
    value_with_native_work_bounded(value, work.remaining()?.bytes_encoded, work)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn value_with_native_work_bounded<T: Serialize>(
    value: &T,
    maximum_bytes: u64,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Value, acyclic_fs::NativeAllocation)> {
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    let value = io.value_json(value, maximum_bytes)?;
    let retained = value_owned_bytes(&value)?;
    let custody = io.into_native_custody(retained)?;
    Ok((value, custody))
}

/// Logical SDK-owned Value slots and actual deep buffer capacities, not serde
/// allocator nodes, provider metadata, or resident process memory.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn value_owned_bytes(value: &Value) -> Result<u64> {
    fn add(left: u64, right: u64) -> Result<u64> {
        left.checked_add(right).ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "tool JSON ownership overflow",
        })
    }
    fn deep(value: &Value) -> Result<u64> {
        match value {
            Value::String(text) => Ok(text.capacity() as u64),
            Value::Array(values) => {
                let slots = (values.capacity() as u64)
                    .checked_mul(std::mem::size_of::<Value>() as u64)
                    .ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "tool JSON array ownership overflow",
                    })?;
                values.iter().try_fold(slots, |bytes, value| add(bytes, deep(value)?))
            }
            Value::Object(values) => values.iter().try_fold(0, |bytes, (key, value)| {
                add(add(add(bytes, std::mem::size_of::<(String, Value)>() as u64)?,
                    key.capacity() as u64)?, deep(value)?)
            }),
            Value::Null | Value::Bool(_) | Value::Number(_) => Ok(0),
        }
    }
    add(std::mem::size_of::<Value>() as u64, deep(value)?)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl crate::filesystem::UploadRecord for ToolDefinition {
    fn logical_owned_bytes(&self) -> Result<u64> {
        let mut bytes = 0_u64;
        for text in [&self.name, &self.revision, &self.description] {
            bytes = bytes.checked_add(text.capacity() as u64).ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "tool definition ownership overflow",
            })?;
        }
        for value in [&self.input_schema, &self.output_schema, &self.projection_schema] {
            bytes = bytes.checked_add(value_owned_bytes(value)? - std::mem::size_of::<Value>() as u64)
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "tool definition schema ownership overflow",
                })?;
        }
        Ok(bytes)
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl crate::filesystem::UploadRecord for ToolInvocation {
    fn logical_owned_bytes(&self) -> Result<u64> {
        let deep = value_owned_bytes(&self.arguments)? - std::mem::size_of::<Value>() as u64;
        (self.call_id.capacity() as u64)
            .checked_add(self.name.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(deep))
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "tool invocation ownership overflow",
            })
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl crate::filesystem::UploadRecord for ToolResult {
    fn logical_owned_bytes(&self) -> Result<u64> {
        Ok(value_owned_bytes(&self.value)? - std::mem::size_of::<Value>() as u64)
    }
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for ToolInvocation {
    fn maximum_slot_bytes() -> u64 {
        std::mem::size_of::<Self>().max(std::mem::size_of::<(String, Value)>()) as u64
    }
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for ToolResult {
    fn maximum_slot_bytes() -> u64 {
        std::mem::size_of::<Self>().max(std::mem::size_of::<(String, Value)>()) as u64
    }
}

/// Model-visible tool definition with immutable schemas.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolDefinition {
    /// Stable bounded tool name; local names and explicit namespaces are both valid.
    pub name: String,
    /// Immutable revision of definition, executor, and projection semantics.
    pub revision: String,
    /// Model-visible description.
    pub description: String,
    /// JSON Schema for invocation arguments.
    pub input_schema: Value,
    /// JSON Schema for the canonical successful executor value.
    pub output_schema: Value,
    /// JSON Schema for the model-facing projection, independently of the result.
    pub projection_schema: Value,
}

impl ToolDefinition {
    /// Validates the name and all three independently pinned schemas.
    pub fn validate(&self) -> Result<()> {
        self.validate_with_io(None)
    }

    pub(crate) fn validate_with_io(
        &self,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            let work = acyclic_fs::WorkCounters {
                items_examined: (self.name.len() as u64).saturating_add(self.revision.len() as u64).saturating_add(1),
                ..Default::default()
            };
            io.preflight(work)?;
            io.charge(work)?;
        }
        validate_tool_name(&self.name)?;
        validate_component_label(&self.revision, "tool revision")?;
        if self.name.contains('@') || self.revision.contains('@') {
            return Err(Error::Invalid(
                "tool name and revision cannot contain the version separator".into(),
            ));
        }
        for schema in [
            &self.input_schema,
            &self.output_schema,
            &self.projection_schema,
        ] {
            crate::contract::compile_json_schema_with_io(schema, "tool", io)?;
        }
        Ok(())
    }

    /// Validates and decodes the complete model-visible projection envelope.
    /// Bare values are rejected even when an unconstrained schema accepts them.
    pub(crate) fn validate_projection(
        &self,
        value: &Value,
    ) -> Result<crate::model::ToolResultContent> {
        validate_value(&self.projection_schema, value, "tool projection")?;
        crate::model::ToolResultContent::deserialize(value).map_err(|error| {
            Error::Invalid(format!("tool projection envelope is invalid: {error}"))
        })
    }

    /// Validates the same complete projection envelope through the original
    /// schema and audited DTO codec, without an ordinary allocating decode.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn validate_projection_with_native_work(
        &self,
        value: &Value,
        limits: crate::conversation::Limits,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(crate::model::ToolResultContent, acyclic_fs::NativeAllocation)> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        crate::contract::validate_json_schema_value_with_io(
            &self.projection_schema, value, "tool projection", Some(&io))?;
        let content: crate::model::ToolResultContent =
            io.value_decode(value, crate::wire::ErrorCode::Invalid)?;
        content.validate_limits_with_io(limits, Some(&io))?;
        let retained = content.logical_owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        Ok((content, custody))
    }

    /// Immutable approval identity for the exact model-visible definition.
    pub fn digest(&self) -> Result<[u8; 32]> {
        self.validate()?;
        crate::contract::canonical_json_digest(self)
    }
}

/// Declares an explicit JSON result envelope for the supplied value schema.
/// The value schema is a child schema; local references resolve from the complete envelope.
#[must_use]
pub fn json_projection_schema(value_schema: Value) -> Value {
    let properties = serde_json::Map::from_iter([
        ("kind".into(), serde_json::json!({"const":"json"})),
        ("value".into(), value_schema),
    ]);
    Value::Object(serde_json::Map::from_iter([
        ("type".into(), Value::String("object".into())),
        ("properties".into(), Value::Object(properties)),
        ("required".into(), serde_json::json!(["kind", "value"])),
        ("additionalProperties".into(), Value::Bool(false)),
    ]))
}

/// One admitted invocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolInvocation {
    /// Runtime-owned identity for execution and reconciliation. A provider's
    /// call ID alone is not unique across turns or tasks.
    pub operation_id: OperationId,
    /// Model-owned call identity.
    pub call_id: String,
    /// Registered tool name.
    pub name: String,
    /// Schema-validated arguments.
    pub arguments: Value,
}

impl ToolInvocation {
    /// Binds a model call to one deterministic operation within its parent turn.
    #[must_use]
    pub fn for_model_call(
        parent: OperationId,
        step: u32,
        call_id: String,
        name: String,
        arguments: Value,
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"harness:tool-call:v2");
        hasher.update(&parent.into_bytes());
        hasher.update(&step.to_be_bytes());
        hasher.update(call_id.as_bytes());
        let digest = hasher.finalize();
        let mut identity = [0_u8; 16];
        identity.copy_from_slice(&digest.as_bytes()[..16]);
        Self {
            operation_id: OperationId::from_bytes(identity),
            call_id,
            name,
            arguments,
        }
    }

    /// Moves an already-paid native provider call into the same deterministic
    /// identity; the caller retains its original payload custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn for_model_call_with_native_work(
        parent: OperationId,
        step: u32,
        call_id: String,
        name: String,
        arguments: Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        let hashed = (b"harness:tool-call:v2".len() as u64)
            .checked_add(16 + 4)
            .and_then(|bytes| bytes.checked_add(call_id.len() as u64))
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "tool call hash length overflow",
            })?;
        work.charge(acyclic_fs::WorkCounters {
            bytes_hashed: hashed,
            items_examined: 4,
            ..Default::default()
        })?;
        Ok(Self::for_model_call(parent, step, call_id, name, arguments))
    }

    /// Rejects identities that could execute successfully but fail later when
    /// their canonical conversation record is published.
    pub fn validate(&self) -> Result<()> {
        Self::validate_identity(&self.call_id, &self.name)
    }

    /// Validates an event identity before it can enter the durable model journal.
    pub fn validate_identity(call_id: &str, name: &str) -> Result<()> {
        Self::validate_identity_with_io(call_id, name, None)
    }

    /// Validates the same complete identity on its original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn validate_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        Self::validate_identity_with_io(&self.call_id, &self.name, Some(&io))
    }

    /// Validates the original model-owned identity without inventing a name.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn validate_call_id_with_native_work(
        call_id: &str,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        Self::validate_call_identity_with_io(call_id, None, Some(&io))
    }

    pub(crate) fn validate_identity_with_io(
        call_id: &str,
        name: &str,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        Self::validate_call_identity_with_io(call_id, Some(name), io)
    }

    fn validate_call_identity_with_io(
        call_id: &str,
        name: Option<&str>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let items = (call_id.len() as u64).checked_add(name.map_or(0, |name| name.len() as u64))
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "tool identity scan overflow" })?;
            work.charge(acyclic_fs::WorkCounters { items_examined: items, ..Default::default() })?;
            if name.is_some_and(|name| !crate::contract::is_valid_component_label(name)) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "tool name is invalid" });
            }
            if !valid_call_id(call_id) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "tool call identity is invalid" });
            }
            return Ok(());
        }
        if let Some(name) = name { validate_tool_name(name)?; }
        if !valid_call_id(call_id) {
            return Err(Error::Invalid("tool call identity is invalid".into()));
        }
        Ok(())
    }
}

fn valid_call_id(call_id: &str) -> bool {
    !(call_id.is_empty()
        || call_id.len() as u64 > crate::COMPONENT_LABEL_MAX_BYTES as u64
        || call_id.chars().any(char::is_control)
        || call_id.contains('/')
        || call_id.contains('\\')
        || call_id == "."
        || call_id == "..")
}

fn validate_tool_name(name: &str) -> Result<()> {
    validate_component_label(name, "tool name")
}

/// Result returned by a tool executor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolResult {
    /// Schema-validated structured value.
    pub value: Value,
}

/// Replaceable execution behavior for a tool.
pub trait ToolExecutor: acyclic_stream::ProviderPlatform {
    /// Identifies an adapter that always executes through this exact resumable
    /// machine's admitted journal and committed outbox. This is not a fallback
    /// flag: registries require full definition and machine identity equality.
    fn resumable_machine_identity(&self) -> Option<crate::workflow::MachineIdentity> {
        None
    }

    /// Checks invocation-specific resource grants before a result is replayed,
    /// dispatched, or reconciled. A scoped adapter must reject a missing scope.
    fn authorize(
        &self,
        _scope: Option<&crate::runtime::RuntimeScope>,
        _invocation: &ToolInvocation,
    ) -> Result<()> {
        Ok(())
    }

    /// Checks original task-bound resources before replay, dispatch or reconciliation.
    /// Context-independent adapters retain the same scoped authorization gate.
    fn authorize_with_context(
        &self,
        context: &crate::runtime::ToolContext,
        invocation: &ToolInvocation,
    ) -> Result<()> {
        self.authorize(Some(context.task().scope()), invocation)
    }

    /// Executes an already admitted invocation.
    fn execute<'a>(&'a self, invocation: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>>;

    /// Executes with the scoped task/tool context when admitted by the typed runtime.
    /// Existing host adapters may delegate to `execute`; contextual tools override this.
    fn execute_with_context<'a>(
        &'a self,
        _context: crate::runtime::ToolContext,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        self.execute(invocation)
    }

    /// Reconciles a previously started invocation without executing it again.
    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>>;

    /// Reconciles under the same owner-authenticated task context used for
    /// dispatch. Context-aware adapters override this rather than relying on
    /// an invocation identity as a bearer authorization.
    fn reconcile_with_context<'a>(
        &'a self,
        _context: crate::runtime::ToolContext,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        self.reconcile(invocation)
    }

    /// Rejects an unaudited implementation before the caller constructs native
    /// invocation metadata or a future. This conveys no resource authority.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "tool executor has no native producer",
        })
    }

    /// Checks the same original context before native replay or dispatch.
    /// A custom executor without an audited native gate rejects synchronously,
    /// before any ordinary authorization callback or future allocation.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn authorize_with_native_context(
        &self,
        _context: &crate::runtime::ToolContext,
        _invocation: &ToolInvocation,
        _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "tool executor has no native authorization producer",
        })
    }

    /// Executes the exact borrowed invocation on its original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn execute_with_native_context<'a>(
        &'a self,
        _context: &'a crate::runtime::ToolContext,
        _invocation: &'a ToolInvocation,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolResult, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "tool executor has no native execution producer",
        })
    }

    /// Observes the already-dispatched effect; never calls ordinary execution.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_with_native_context<'a>(
        &'a self,
        _context: &'a crate::runtime::ToolContext,
        _invocation: &'a ToolInvocation,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ToolResult>, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "tool executor has no native reconciliation producer",
        })
    }
}

/// Replaceable mapping from tool results into model-visible context.
pub trait ToolProjection: acyclic_stream::ProviderPlatform {
    /// Produces the complete serialized `ToolResultContent` envelope without effects.
    /// The independently pinned projection schema validates this full envelope.
    fn project(&self, invocation: &ToolInvocation, result: &ToolResult) -> Result<Value>;

    /// Rejects an unaudited projector before dispatch, not after its effects.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_projection(
        &self,
        _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "tool projection has no native producer",
        })
    }

    /// Produces a paid SDK projection without invoking an ordinary allocating
    /// projector. Custom projectors must explicitly supply their audited body.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn project_with_native_work(
        &self,
        _invocation: &ToolInvocation,
        _result: &ToolResult,
        _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "tool projection has no native producer",
        })
    }
}

/// Trusted journal check for a resolved approval bound to one exact tool definition.
pub trait ToolApprovalVerifier: acyclic_stream::ProviderPlatform {
    /// Rejects absent, declined, mismatched, or indeterminate approvals.
    fn verify<'a>(
        &'a self,
        interaction_id: InteractionId,
        operation_id: OperationId,
        definition_digest: [u8; 32],
    ) -> BoxFuture<'a, Result<()>>;

    /// Optional actual bounded approval replay and artifact read. Missing
    /// implementations reject before consulting any unmeasured journal.
    #[cfg(feature = "filesystem")]
    fn verify_measured<'a>(
        &'a self, _interaction_id: InteractionId, _operation_id: OperationId,
        _definition_digest: [u8; 32], _budget: acyclic_fs::WorkBudget,
        _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        Box::pin(async { Err(acyclic_fs::OperationFailure::before_work(Error::Unsupported(
            "finite original approval verification is unavailable".into()))) })
    }

    /// Verifies that same retained approval on the original native ledger.
    /// An unsupported provider rejects before future allocation or journal IO.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_with_native_work<'a>(
        &'a self,
        _interaction_id: InteractionId,
        _operation_id: OperationId,
        _definition_digest: [u8; 32],
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<BoxFuture<'a, Result<()>>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "approval verifier has no native journal observer",
        })
    }
}

/// One tool assembled from three independently replaceable values.
#[derive(Clone)]
pub struct Tool {
    /// Model-visible contract.
    pub definition: ToolDefinition,
    /// Execution implementation.
    pub executor: Arc<dyn ToolExecutor>,
    /// Context projection implementation.
    pub projection: Arc<dyn ToolProjection>,
}

#[derive(Clone, PartialEq, Eq)]
enum ModelSelection {
    Revision(String),
    Ambiguous,
    Hidden,
}

/// Deterministic registry retaining every pinned revision. A model request
/// exposes exactly one selected revision per logical tool name.
#[derive(Clone, Default)]
pub struct ToolRegistry {
    versions: BTreeMap<(String, String), Tool>,
    selected: BTreeMap<String, ModelSelection>,
    catalog_revisions: BTreeMap<String, [u8; 32]>,
}

impl ToolRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            versions: BTreeMap::new(),
            selected: BTreeMap::new(),
            catalog_revisions: BTreeMap::new(),
        }
    }

    /// Registers a tool, rejecting ambiguous replacement.
    pub fn register(&mut self, tool: Tool) -> Result<()> {
        tool.definition.validate()?;
        let name = tool.definition.name.clone();
        let revision = tool.definition.revision.clone();
        if self
            .versions
            .contains_key(&(name.clone(), revision.clone()))
        {
            return Err(Error::Conflict(format!(
                "tool {name}@{revision} is already registered"
            )));
        }
        let previous = self.versions.keys().any(|(logical, _)| logical == &name);
        self.versions.insert((name.clone(), revision.clone()), tool);
        if previous {
            if self.selected.get(&name) != Some(&ModelSelection::Hidden) {
                self.selected.insert(name, ModelSelection::Ambiguous);
            }
        } else {
            self.selected
                .insert(name, ModelSelection::Revision(revision));
        }
        Ok(())
    }

    /// Selects the one revision visible to a model under this logical name.
    pub fn select_model_version(&mut self, name: &str, revision: &str) -> Result<()> {
        if !self
            .versions
            .contains_key(&(name.to_owned(), revision.to_owned()))
        {
            return Err(Error::NotFound(format!("tool {name}@{revision}")));
        }
        self.selected.insert(
            name.to_owned(),
            ModelSelection::Revision(revision.to_owned()),
        );
        Ok(())
    }

    /// Pins complete dynamic installation identity, including empty visibility.
    /// Callers mutate a cloned registry and publish it only after batch validation.
    pub(crate) fn pin_catalog_revision(
        &mut self,
        namespace: &str,
        expected: Option<[u8; 32]>,
        revision: [u8; 32],
    ) -> Result<()> {
        if self.catalog_revisions.get(namespace).copied() != expected {
            return Err(Error::Conflict(
                "dynamic catalog installation changed".into(),
            ));
        }
        self.catalog_revisions.insert(namespace.into(), revision);
        Ok(())
    }

    /// Removes a logical tool from future model catalogs without deleting its
    /// immutable versions. Already admitted operations can still resolve their
    /// pinned version; calling `select_model_version` explicitly restores it.
    pub fn remove_from_model(&mut self, name: &str) -> Result<()> {
        if !self.versions.keys().any(|(logical, _)| logical == name) {
            return Err(Error::NotFound(format!("tool {name}")));
        }
        self.selected
            .insert(name.to_owned(), ModelSelection::Hidden);
        Ok(())
    }

    /// Installs an agent-selected tool after exact scope and durable approval checks.
    pub async fn install_scoped(
        &mut self,
        tool: Tool,
        installation_id: OperationId,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        interaction_id: InteractionId,
        approval: &dyn ToolApprovalVerifier,
    ) -> Result<()> {
        verifier.verify(scope)?;
        if !scope
            .capabilities()
            .contains(&format!("tool:install:{}", tool.definition.name))
        {
            return Err(Error::Unauthorized(
                "scope does not permit this tool installation".into(),
            ));
        }
        let digest = tool.definition.digest()?;
        approval
            .verify(interaction_id, installation_id, digest)
            .await?;
        self.register(tool)
    }

    /// Returns one assembled tool.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Tool> {
        if let Some((logical, revision)) = name.rsplit_once('@') {
            return self.get_version(logical, revision);
        }
        let ModelSelection::Revision(revision) = self.selected.get(name)? else {
            return None;
        };
        self.get_version(name, revision)
    }

    /// Resolves a pinned revision independently of the model-visible choice.
    #[must_use]
    pub fn get_version(&self, name: &str, revision: &str) -> Option<&Tool> {
        self.versions.get(&(name.to_owned(), revision.to_owned()))
    }

    /// Resolves the same selected or explicitly qualified tool with original
    /// work and paid pinned-lookup keys, without an ordinary allocating lookup.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn get_with_native_work(
        &self,
        name: &str,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&Tool>> {
        work.charge(acyclic_fs::WorkCounters {
            items_examined: name.len() as u64,
            ..Default::default()
        })?;
        if let Some((logical, revision)) = name.rsplit_once('@') {
            return self.get_version_with_native_work(logical, revision, work);
        }
        let Some(ModelSelection::Revision(revision)) = self.selected.get(name) else {
            return Ok(None);
        };
        self.get_version_with_native_work(name, revision, work)
    }

    /// Uses the same logarithmic pinned registry lookup with funded temporary
    /// key strings; no second registry or linear native lookup is introduced.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn get_version_with_native_work(
        &self,
        name: &str,
        revision: &str,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&Tool>> {
        let name_hold;
        let (name, hold) = work.display(name)?;
        name_hold = hold;
        let revision_hold;
        let (revision, hold) = work.display(revision)?;
        revision_hold = hold;
        let key = (name, revision);
        let found = self.versions.get(&key);
        drop(key);
        drop(revision_hold);
        drop(name_hold);
        Ok(found)
    }

    /// Copies only SDK-owned registry metadata on the original ledger. Executor
    /// and projector Arcs remain the exact registered provider objects.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn clone_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let mut custody = work.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let mut copied = Self::default();
        for ((name, revision), tool) in &self.versions {
            let mut definition_hold;
            let (definition, hold) =
                clone_definition_with_native_work(&tool.definition, work)?;
            definition_hold = hold;
            let mut name_hold;
            let (name, hold) = work.display(name)?;
            name_hold = hold;
            let mut revision_hold;
            let (revision, hold) = work.display(revision)?;
            revision_hold = hold;
            custody.absorb(&mut definition_hold)?;
            custody.absorb(&mut name_hold)?;
            custody.absorb(&mut revision_hold)?;
            let slots = std::mem::size_of::<(String, String)>() as u64
                + std::mem::size_of::<Arc<dyn ToolExecutor>>() as u64
                + std::mem::size_of::<Arc<dyn ToolProjection>>() as u64;
            custody.resize_retained(custody.bytes().checked_add(slots).ok_or(
                Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "native tool registry ownership overflow" })?)?;
            copied.versions.insert((name, revision), Tool {
                definition, executor: tool.executor.clone(), projection: tool.projection.clone(),
            });
        }
        for (name, selection) in &self.selected {
            let mut name_hold;
            let (name, hold) = work.display(name)?;
            name_hold = hold;
            custody.absorb(&mut name_hold)?;
            let selection = match selection {
                ModelSelection::Revision(revision) => {
                    let mut hold;
                    let (revision, owned) = work.display(revision)?;
                    hold = owned;
                    custody.absorb(&mut hold)?;
                    ModelSelection::Revision(revision)
                }
                ModelSelection::Ambiguous => ModelSelection::Ambiguous,
                ModelSelection::Hidden => ModelSelection::Hidden,
            };
            custody.resize_retained(custody.bytes().checked_add(
                std::mem::size_of::<(String, ModelSelection)>() as u64).ok_or(
                Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "native tool selection ownership overflow" })?)?;
            copied.selected.insert(name, selection);
        }
        for (name, revision) in &self.catalog_revisions {
            let mut hold;
            let (name, owned) = work.display(name)?;
            hold = owned;
            custody.absorb(&mut hold)?;
            custody.resize_retained(custody.bytes().checked_add(
                std::mem::size_of::<(String, [u8; 32])>() as u64).ok_or(
                Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "native tool catalog ownership overflow" })?)?;
            copied.catalog_revisions.insert(name, *revision);
        }
        Ok((copied, custody))
    }

    /// Returns the same canonical model selection with funded DTO copies.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn definitions_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Vec<ToolDefinition>, acyclic_fs::NativeAllocation)> {
        let bytes = self.selected.len().checked_mul(std::mem::size_of::<ToolDefinition>())
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native tool definitions capacity overflow" })? as u64;
        let metadata = std::mem::size_of::<Vec<ToolDefinition>>() as u64;
        let mut custody = work.retain_owned(metadata)?;
        let mut backing = if bytes == 0 { work.retain_owned(0)? } else { work.claim(bytes)? };
        custody.absorb(&mut backing)?;
        let mut definitions = Vec::with_capacity(self.selected.len());
        let actual = (definitions.capacity() * std::mem::size_of::<ToolDefinition>()) as u64;
        custody.reconcile_capacity(actual.checked_add(metadata).ok_or(
            Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native tool definitions ownership overflow" })?)?;
        for (name, selection) in &self.selected {
            let revision = match selection {
                ModelSelection::Revision(revision) => revision,
                ModelSelection::Hidden => continue,
                ModelSelection::Ambiguous => return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "tool requires an explicit model revision",
                }),
            };
            let tool = self.get_version_with_native_work(name, revision, work)?.ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage,
                reason: "selected tool revision is not registered",
            })?;
            let mut hold;
            let (definition, owned) =
                clone_definition_with_native_work(&tool.definition, work)?;
            hold = owned;
            hold.substitute(hold.bytes() - std::mem::size_of::<ToolDefinition>() as u64)?;
            custody.absorb(&mut hold)?;
            definitions.push(definition);
        }
        Ok((definitions, custody))
    }

    /// Returns the unambiguous model-visible selection in canonical order.
    pub fn definitions(&self) -> Result<Vec<ToolDefinition>> {
        let mut definitions = Vec::with_capacity(self.selected.len());
        for (name, selection) in &self.selected {
            let revision = match selection {
                ModelSelection::Revision(revision) => revision,
                ModelSelection::Hidden => continue,
                ModelSelection::Ambiguous => {
                    return Err(Error::Conflict(format!(
                        "tool {name} requires an explicit model revision"
                    )));
                }
            };
            let tool = self
                .get_version(name, revision)
                .ok_or_else(|| Error::Storage("selected tool revision is not registered".into()))?;
            definitions.push(tool.definition.clone());
        }
        Ok(definitions)
    }
}

pub(crate) use crate::contract::validate_json_schema_value as validate_value;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Capabilities,
        core::{AggregateKind, Authority, AuthorityIssuer},
    };
    use futures::FutureExt as _;
    use serde_json::json;

    struct Executor;
    impl ToolExecutor for Executor {
        fn execute<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
            async { Ok(ToolResult { value: Value::Null }) }.boxed()
        }
        fn reconcile<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<Option<ToolResult>>> {
            async { Ok(None) }.boxed()
        }
    }
    struct Projection;
    impl ToolProjection for Projection {
        fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
            Ok(serde_json::json!({"kind":"json","value":result.value}))
        }
    }

    #[test]
    fn model_tool_operation_identity_is_stable_and_scoped_to_parent_step_and_call() {
        let parent = OperationId::from_bytes([7; 16]);
        let make = |parent, step, call_id: &str| {
            ToolInvocation::for_model_call(
                parent,
                step,
                call_id.into(),
                "example.echo".into(),
                json!({"value": 1}),
            )
            .operation_id
        };
        let same = make(parent, 0, "call");
        assert_eq!(same, make(parent, 0, "call"));
        assert_ne!(same, make(parent, 1, "call"));
        assert_ne!(same, make(parent, 0, "other"));
        assert_ne!(same, make(OperationId::from_bytes([8; 16]), 0, "call"));
    }

    #[test]
    fn pinned_tool_revisions_require_explicit_model_selection() -> Result<()> {
        let mut tools = ToolRegistry::new();
        for revision in ["1", "2"] {
            tools.register(Tool {
                definition: ToolDefinition {
                    name: "example.echo".into(),
                    revision: revision.into(),
                    description: "Echo".into(),
                    input_schema: json!({}),
                    output_schema: json!({}),
                    projection_schema: crate::tool::json_projection_schema(json!({})),
                },
                executor: Arc::new(Executor),
                projection: Arc::new(Projection),
            })?;
        }
        assert!(tools.get("example.echo").is_none());
        assert!(matches!(tools.definitions(), Err(Error::Conflict(_))));
        assert!(tools.get_version("example.echo", "1").is_some());
        tools.select_model_version("example.echo", "2")?;
        assert_eq!(tools.definitions()?[0].revision, "2");
        assert_eq!(
            tools
                .get("example.echo")
                .map(|tool| tool.definition.revision.as_str()),
            Some("2")
        );
        tools.remove_from_model("example.echo")?;
        assert!(tools.definitions()?.is_empty());
        assert!(tools.get("example.echo").is_none());
        assert!(tools.get_version("example.echo", "1").is_some());
        assert!(tools.get_version("example.echo", "2").is_some());
        assert!(tools.get("example.echo@1").is_some());
        let mut replacement = tools
            .get_version("example.echo", "1")
            .ok_or_else(|| Error::NotFound("test tool".into()))?
            .clone();
        replacement.definition.revision = "3".into();
        tools.register(replacement)?;
        assert!(tools.definitions()?.is_empty());
        assert!(tools.get_version("example.echo", "3").is_some());
        assert!(matches!(
            tools.remove_from_model("missing"),
            Err(Error::NotFound(_))
        ));
        tools.select_model_version("example.echo", "1")?;
        assert_eq!(tools.definitions()?[0].revision, "1");
        Ok(())
    }

    struct Approved {
        id: InteractionId,
        operation: OperationId,
        digest: [u8; 32],
    }

    #[test]
    fn projection_contract_is_required_validated_and_pinned() -> Result<()> {
        let definition = ToolDefinition {
            name: "example.result".into(),
            revision: "1".into(),
            description: "Keep a structured result and project text".into(),
            input_schema: json!({"type": "null"}),
            output_schema: json!({"type": "object", "required": ["count"],
                "properties": {"count": {"type": "integer"}}, "additionalProperties": false}),
            projection_schema: crate::tool::json_projection_schema(json!({"type": "string"})),
        };
        definition.validate()?;
        let permissive = ToolDefinition {
            projection_schema: json!({}),
            ..definition.clone()
        };
        assert!(
            permissive
                .validate_projection(&json!("old bare projection"))
                .is_err()
        );
        assert!(
            permissive
                .validate_projection(&json!({"kind":"parts","parts":[
                    {"kind":"tool_call","call_id":"nested","name":"nested","arguments":{}}
                ]}))
                .is_err()
        );
        validate_value(&definition.output_schema, &json!({"count": 2}), "result")?;
        validate_value(
            &definition.projection_schema,
            &json!({"kind":"json","value":"two"}),
            "projection",
        )?;
        assert!(validate_value(&definition.output_schema, &json!("two"), "result").is_err());
        assert!(
            validate_value(
                &definition.projection_schema,
                &json!({"count": 2}),
                "projection"
            )
            .is_err()
        );
        let mut changed = definition.clone();
        changed.projection_schema = json!({"type": "integer"});
        assert_ne!(definition.digest()?, changed.digest()?);
        changed.projection_schema = json!({"type": "not-a-schema-type"});
        assert!(changed.validate().is_err());
        let mut missing =
            serde_json::to_value(definition).map_err(|error| Error::Invalid(error.to_string()))?;
        missing
            .as_object_mut()
            .expect("definition object")
            .remove("projection_schema");
        assert!(serde_json::from_value::<ToolDefinition>(missing).is_err());
        Ok(())
    }
    impl ToolApprovalVerifier for Approved {
        fn verify<'a>(
            &'a self,
            id: InteractionId,
            operation: OperationId,
            digest: [u8; 32],
        ) -> BoxFuture<'a, Result<()>> {
            async move {
                if id != self.id || operation != self.operation || digest != self.digest {
                    return Err(Error::Unauthorized(
                        "approval is not bound to this installation".into(),
                    ));
                }
                Ok(())
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn scoped_install_requires_capability_and_exact_approval_operation() -> Result<()> {
        let definition = ToolDefinition {
            name: "example.echo".into(),
            revision: "1".into(),
            description: "Echo".into(),
            input_schema: json!({"type": "object"}),
            output_schema: json!({}),
            projection_schema: crate::tool::json_projection_schema(json!({})),
        };
        let digest = definition.digest()?;
        let operation_id = OperationId::from_bytes([8; 16]);
        let issuer = AuthorityIssuer::new(
            "issuer",
            [7; 32],
            Authority {
                kind: AggregateKind::Agent,
                id: "agent".into(),
            },
        );
        let scope = issuer.root("install", Capabilities::new(["tool:install:example.echo"]));
        let interaction_id = InteractionId::from_bytes([6; 16]);
        let approval = Approved {
            id: interaction_id,
            operation: operation_id,
            digest,
        };
        let tool = Tool {
            definition,
            executor: Arc::new(Executor),
            projection: Arc::new(Projection),
        };
        let mut registry = ToolRegistry::new();
        assert!(
            registry
                .install_scoped(
                    tool.clone(),
                    OperationId::from_bytes([9; 16]),
                    &scope,
                    &issuer.verifier(),
                    interaction_id,
                    &approval
                )
                .await
                .is_err()
        );
        registry
            .install_scoped(
                tool,
                operation_id,
                &scope,
                &issuer.verifier(),
                interaction_id,
                &approval,
            )
            .await?;
        assert!(registry.get("example.echo").is_some());
        Ok(())
    }
}
