//! Independently replaceable tool definitions, executors, and projections.

use crate::{
    Error, InteractionId, OperationId, Result,
    core::{AuthorityVerifier, Scope},
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

/// Copies a Value under the original ledger and optional wire-size bound.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn clone_value(
    value: &Value,
    maximum_bytes: Option<u64>,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Value, acyclic_fs::NativeAllocation)> {
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    if let Some(maximum_bytes) = maximum_bytes {
        io.check_json_byte_bound(value, maximum_bytes)?;
    }
    io.retain_owned(std::mem::size_of::<Value>() as u64)?;
    let copied = crate::model::clone_value(value, Some(&io))?;
    let retained = crate::core::value_owned_bytes(&copied)?;
    let custody = io.into_native_custody(retained)?;
    Ok((copied, custody))
}

/// Builds the canonical result tree through the existing bounded SDK codec.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn value<T: Serialize>(
    value: &T,
    maximum_bytes: u64,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Value, acyclic_fs::NativeAllocation)> {
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    let value = io.value_json(value, maximum_bytes)?;
    let retained = (std::mem::size_of::<Value>() as u64)
        .checked_add(crate::contract::json_value_logical_owned_bytes(&value)?)
        .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "tool JSON ownership overflow" })?;
    let custody = io.into_native_custody(retained)?;
    Ok((value, custody))
}

/// Static labels of the closed tool DTO graphs, extending the SDK profile with their actual derived type and field names before serde constructs an error.
#[cfg(feature = "filesystem")]
pub(crate) const fn tool_serde_diagnostic_label_bytes() -> u64 {
    const BYTES: u64 = {
        const LABELS: &[&str] = &[
            "ToolDefinition", "ToolInvocation", "ToolResult", "ReadFileInput",
            "WriteFileInput", "EditFileInput", "PatchFileInput", "FileResult",
            "ReadText", "ReadInput", "SearchInput", "ReadResult", "SearchResult",
            "TextRange", "TextSelection", "SearchMatches", "ListFilesInput",
            "WorkflowAdmission", "MachineCheckpoint", "MachineIdentity",
            "operation_id", "call_id", "name", "revision", "description",
            "input_schema", "output_schema", "projection_schema", "arguments",
            "value", "file", "path", "text", "media_type", "display_name",
            "old_text", "new_text", "diff", "range", "query", "selection",
            "matches", "start", "end", "omitted_before", "omitted_after",
            "total_matches", "omitted_matches", "work", "expected_generation",
            "after", "maximum_entries", "request_digest", "initial", "machine",
            "version", "digest", "state",
        ];
        let mut bytes = crate::contract::preparation_serde_diagnostic_label_bytes();
        let mut index = 0;
        while index < LABELS.len() {
            bytes += LABELS[index].len() as u64 + 4;
            index += 1;
        }
        bytes
    };
    BYTES
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl crate::filesystem::UploadRecord for ToolDefinition {
    fn diagnostic_label_bytes() -> u64 { tool_serde_diagnostic_label_bytes() }
    fn logical_owned_bytes(&self) -> Result<u64> {
        let mut bytes = 0_u64;
        for text in [&self.name, &self.revision, &self.description] {
            bytes = bytes.checked_add(text.capacity() as u64).ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "tool definition ownership overflow",
            })?;
        }
        for value in [&self.input_schema, &self.output_schema, &self.projection_schema] {
            bytes = bytes.checked_add(crate::contract::json_value_logical_owned_bytes(value)?)
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
    fn diagnostic_label_bytes() -> u64 { tool_serde_diagnostic_label_bytes() }
    fn logical_owned_bytes(&self) -> Result<u64> {
        let deep = crate::contract::json_value_logical_owned_bytes(&self.arguments)?;
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
    fn diagnostic_label_bytes() -> u64 { tool_serde_diagnostic_label_bytes() }
    fn logical_owned_bytes(&self) -> Result<u64> {
        crate::contract::json_value_logical_owned_bytes(&self.value)
    }
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for ToolInvocation {
    fn diagnostic_label_bytes() -> u64 { tool_serde_diagnostic_label_bytes() }
    fn maximum_slot_bytes() -> u64 {
        std::mem::size_of::<Self>().max(std::mem::size_of::<(String, Value)>()) as u64
    }
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for ToolResult {
    fn diagnostic_label_bytes() -> u64 { tool_serde_diagnostic_label_bytes() }
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
    /// Constructs a definition from borrowed schema literals on the original ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[allow(clippy::too_many_arguments, reason = "the complete pinned tool definition")]
    pub fn new(
        name: &str,
        revision: &str,
        description: &str,
        input_schema_json: &str,
        output_schema_json: &str,
        projection_schema_json: &str,
        maximum_schema_bytes: u64,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        crate::executor::with_history_io(Some(&io), |work| Self::validate_identity_fields(name, revision, work))?;
        for schema in [input_schema_json, output_schema_json, projection_schema_json] {
            if schema.len() as u64 > maximum_schema_bytes {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "tool schema exceeds its admitted byte limit",
                });
            }
        }
        io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let definition = Self {
            name: crate::executor::stock_string_copy(name, Some(&io))?,
            revision: crate::executor::stock_string_copy(revision, Some(&io))?,
            description: crate::executor::stock_string_copy(description, Some(&io))?,
            input_schema: crate::filesystem::retain_decoded(io.decode_preparation::<Value>(
                input_schema_json.as_bytes(), crate::wire::ErrorCode::Invalid)?),
            output_schema: crate::filesystem::retain_decoded(io.decode_preparation::<Value>(
                output_schema_json.as_bytes(), crate::wire::ErrorCode::Invalid)?),
            projection_schema: crate::filesystem::retain_decoded(io.decode_preparation::<Value>(
                projection_schema_json.as_bytes(), crate::wire::ErrorCode::Invalid)?),
        };
        crate::executor::with_history_io(Some(&io), |work| definition.validate_schemas(work))?;
        use crate::filesystem::UploadRecord as _;
        let retained = (std::mem::size_of::<Self>() as u64)
            .checked_add(definition.logical_owned_bytes()?)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "tool definition ownership overflow",
            })?;
        let custody = io.into_native_custody(retained)?;
        Ok((definition, custody))
    }

    /// Validates the name and all three independently pinned schemas.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        Self::validate_identity_fields(&self.name, &self.revision, work)?;
        self.validate_schemas(work)
    }

    fn validate_schemas(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        for schema in [
            &self.input_schema,
            &self.output_schema,
            &self.projection_schema,
        ] {
            crate::contract::compile_json_schema_with_work(schema, "tool", work)?;
        }
        Ok(())
    }

    fn validate_identity_fields(
        name: &str,
        revision: &str,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.codec_charge(crate::contract::JsonCodecCharge::Work {
                bytes: 0, items: (name.len() as u64).saturating_add(revision.len() as u64).saturating_add(1),
            })?;
        }
        for (value, reason) in [(name, "tool name is invalid"), (revision, "tool revision is invalid")] {
            if !crate::contract::is_valid_component_label(value) {
                return Err(crate::effect_host::effect_error(work, crate::wire::ErrorCode::Invalid, reason));
            }
        }
        if name.contains('@') || revision.contains('@') {
            return Err(crate::effect_host::effect_error(work, crate::wire::ErrorCode::Invalid,
                "tool name and revision cannot contain the version separator"));
        }
        Ok(())
    }

    /// Validates and decodes the complete model-visible projection envelope.
    pub(crate) fn validate_projection(
        &self,
        value: &Value,
        limits: crate::conversation::Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<crate::model::ToolResultContent> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io && let Some(work) = io.original_native_work() {
            let decode = crate::filesystem::PreparationIo::from_native_work(work)?;
            validate_value(&self.projection_schema, value, "tool projection", Some(&decode))?;
            let content: crate::model::ToolResultContent =
                decode.value_decode(value, crate::wire::ErrorCode::Invalid)?;
            crate::executor::with_history_io(Some(&decode), |work| content.validate_limits(limits, work))?;
            let retained = content.logical_owned_bytes()?;
            io.absorb_native_custody(decode.into_native_custody(retained)?)?;
            return Ok(content);
        }
        validate_value(&self.projection_schema, value, "tool projection", io)?;
        let content = crate::model::ToolResultContent::deserialize(value).map_err(|error| {
            Error::Invalid(format!("tool projection envelope is invalid: {error}"))
        })?;
        crate::executor::with_history_io(io, |work| content.validate_limits(limits, work))?;
        Ok(content)
    }

    /// Immutable approval identity for the exact model-visible definition.
    pub fn digest(&self) -> Result<[u8; 32]> {
        self.validate(&mut None)?;
        crate::contract::canonical_json_digest(self)
    }
}

/// Declares an explicit JSON result envelope for the supplied value schema.
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
    /// Runtime-owned identity for execution and reconciliation.
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
    pub fn for_model_call(
        parent: OperationId,
        step: u32,
        call_id: String,
        name: String,
        arguments: Value,
        work: crate::NativeWork<'_>,
    ) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            let hashed = (b"harness:tool-call:v2".len() as u64)
                .checked_add(16 + 4)
                .and_then(|bytes| bytes.checked_add(call_id.len() as u64))
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "tool call hash length overflow",
                })?;
            work.charge(acyclic_fs::WorkCounters {
                bytes_hashed: hashed, items_examined: 4, ..Default::default()
            })?;
        }
        let _ = work;
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"harness:tool-call:v2");
        hasher.update(&parent.into_bytes());
        hasher.update(&step.to_be_bytes());
        hasher.update(call_id.as_bytes());
        let digest = hasher.finalize();
        let mut identity = [0_u8; 16];
        identity.copy_from_slice(&digest.as_bytes()[..16]);
        Ok(Self {
            operation_id: OperationId::from_bytes(identity),
            call_id,
            name,
            arguments,
        })
    }

    /// Validates a call ID and its tool name when already selected.
    pub fn validate_identity(
        call_id: &str,
        name: Option<&str>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if work.is_some() {
            let items = (call_id.len() as u64).checked_add(name.map_or(0, |name| name.len() as u64))
                .ok_or_else(|| crate::effect_host::effect_error(work, crate::wire::ErrorCode::Invalid,
                    "tool identity scan overflow"))?;
            work.as_deref_mut().expect("identity observer")
                .codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items })?;
        }
        if name.is_some_and(|name| !crate::contract::is_valid_component_label(name)) {
            return Err(crate::effect_host::effect_error(work, crate::wire::ErrorCode::Invalid,
                "tool name is invalid"));
        }
        if !valid_call_id(call_id) {
            return Err(crate::effect_host::effect_error(work, crate::wire::ErrorCode::Invalid,
                "tool call identity is invalid"));
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

/// Result returned by a tool executor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolResult {
    /// Schema-validated structured value.
    pub value: Value,
}

/// Replaceable execution behavior for a tool.
pub trait ToolExecutor: acyclic_stream::ProviderPlatform {
    /// Identifies an adapter that always executes through this exact resumable machine's admitted journal and committed outbox.
    fn resumable_machine_identity(&self) -> Option<crate::workflow::MachineIdentity> {
        None
    }

    /// Borrows the same genuine adapter's pinned machine identity on the original ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn resumable_machine_identity_with_native_work<'a>(
        &'a self,
        _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&'a crate::workflow::MachineIdentity>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "tool executor has no native resumable identity producer",
        })
    }

    /// Checks invocation-specific resource grants before a result is replayed, dispatched, or reconciled.
    fn authorize(
        &self,
        _scope: Option<&crate::runtime::RuntimeScope>,
        _invocation: &ToolInvocation,
    ) -> Result<()> {
        Ok(())
    }

    /// Checks original task-bound resources before replay, dispatch or reconciliation.
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

    /// Reconciles under the same owner-authenticated task context used for dispatch.
    fn reconcile_with_context<'a>(
        &'a self,
        _context: crate::runtime::ToolContext,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        self.reconcile(invocation)
    }

    /// Rejects an unaudited implementation before the caller constructs native invocation metadata or a future.
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

    /// Produces a paid SDK projection without invoking an ordinary allocating projector.
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

/// First-party complete JSON projection for registered SDK tools, including MCP.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonToolProjection;

impl ToolProjection for JsonToolProjection {
    fn project(&self, _invocation: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        schema::project_json(&result.value)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_projection(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn project_with_native_work(
        &self,
        _invocation: &ToolInvocation,
        result: &ToolResult,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        schema::project_json_with_native_work(&result.value, work)
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

    /// Optional actual bounded approval replay and artifact read.
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
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_with_native_work<'a>(
        &'a self,
        _interaction_id: InteractionId,
        _operation_id: OperationId,
        _definition_digest: [u8; 32],
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
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

/// Deterministic registry retaining every pinned revision.
#[derive(Clone, Default)]
pub struct ToolRegistry {
    versions: BTreeMap<(String, String), Tool>,
    selected: BTreeMap<String, ModelSelection>,
    catalog_revisions: BTreeMap<String, [u8; 32]>,
}

enum RegistrationSelection {
    Unchanged,
    ReplaceAmbiguous,
    Insert(String, ModelSelection),
}

struct PreparedToolRegistration {
    key: (String, String),
    tool: Tool,
    selection: RegistrationSelection,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl PreparedToolRegistration {
    fn additional_owned_bytes(&self) -> Result<u64> {
        let slots = std::mem::size_of::<(String, String)>()
            + std::mem::size_of::<Arc<dyn ToolExecutor>>()
            + std::mem::size_of::<Arc<dyn ToolProjection>>();
        let mut bytes = (slots as u64)
            .checked_add(self.key.0.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(self.key.1.capacity() as u64))
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid, reason: "tool registration ownership overflow",
            })?;
        if let RegistrationSelection::Insert(name, selection) = &self.selection {
            let revision = match selection {
                ModelSelection::Revision(revision) => revision.capacity() as u64,
                _ => 0,
            };
            bytes = bytes.checked_add(std::mem::size_of::<(String, ModelSelection)>() as u64)
                .and_then(|bytes| bytes.checked_add(name.capacity() as u64))
                .and_then(|bytes| bytes.checked_add(revision))
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid, reason: "tool registration ownership overflow",
                })?;
        }
        Ok(bytes)
    }
}

impl ToolRegistry {
    /// Creates an empty registry with funded metadata.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn new(
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let custody = work.retain_owned(std::mem::size_of::<Self>() as u64)?;
        Ok((Self::default(), custody))
    }

    /// Registers a tool, rejecting ambiguous replacement.
    pub fn register(&mut self, tool: Tool) -> Result<()> {
        let prepared = self.prepare_registration(tool, None)?;
        self.publish_registration(prepared);
        Ok(())
    }

    /// Moves a paid definition and the original provider objects into this registry.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[allow(clippy::too_many_arguments, reason = "paid definition and original providers")]
    pub fn register_with_native_work(
        &mut self,
        definition: ToolDefinition,
        definition_custody: acyclic_fs::NativeAllocation,
        executor: Arc<dyn ToolExecutor>,
        projection: Arc<dyn ToolProjection>,
        custody: &mut acyclic_fs::NativeAllocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        use crate::filesystem::UploadRecord as _;
        let mut definition = (definition, definition_custody);
        work.remaining()?;
        let original = self.native_owned_bytes()?;
        let definition_bytes = (std::mem::size_of::<ToolDefinition>() as u64)
            .checked_add(definition.0.logical_owned_bytes()?)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "tool definition ownership overflow",
            })?;
        if !custody.matches_work(work) || custody.bytes() < original
            || !definition.1.matches_work(work) || definition.1.bytes() < definition_bytes
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "tool registration has no original payload custody",
            });
        }
        executor.supports_native_execution(work)?;
        projection.supports_native_projection(work)?;
        let original_quota = custody.bytes();
        custody.absorb(&mut definition.1)?;
        let mut published = false;
        let result = (|| {
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            let tool = Tool { definition: definition.0, executor, projection };
            let prepared = self.prepare_registration(tool, Some(&io))?;
            let extra = prepared.additional_owned_bytes()?;
            let released = match &prepared.selection {
                RegistrationSelection::ReplaceAmbiguous => match self.selected.get(&prepared.key.0) {
                    Some(ModelSelection::Revision(revision)) => revision.capacity() as u64,
                    _ => 0,
                },
                _ => 0,
            };
            let retained = original.checked_add(definition_bytes)
                .and_then(|bytes| bytes.checked_add(extra))
                .and_then(|bytes| bytes.checked_sub(released))
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "tool registry ownership overflow",
                })?;
            let held = io.into_native_custody(extra)?;
            let mut prepared = (prepared, held);
            // Transfer every paid new field before the infallible publication.
            custody.absorb(&mut prepared.1)?;
            self.publish_registration(prepared.0);
            published = true;
            // Old selection backing is physically gone before its refund.
            custody.substitute(retained)?;
            Ok(())
        })();
        if result.is_err() && !published {
            // The closure's input and prepared buffers have physically dropped.
            custody.substitute(original_quota)?;
        }
        result
    }

    fn prepare_registration(
        &self,
        tool: Tool,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<PreparedToolRegistration> {
        crate::executor::with_history_io(io, |work| tool.definition.validate(work))?;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.retain_owned((std::mem::size_of::<(String, String)>()
                + std::mem::size_of::<Arc<dyn ToolExecutor>>()
                + std::mem::size_of::<Arc<dyn ToolProjection>>()) as u64)?;
            let counters = acyclic_fs::WorkCounters {
                items_examined: self.versions.len() as u64,
                ..Default::default()
            };
            io.preflight(counters)?;
            io.charge(counters)?;
        }
        let key = (
            crate::executor::stock_string_copy(&tool.definition.name, io)?,
            crate::executor::stock_string_copy(&tool.definition.revision, io)?,
        );
        if self.versions.contains_key(&key) {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if io.and_then(crate::executor::PreparationIo::original_native_work).is_some() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "tool revision is already registered",
                });
            }
            return Err(Error::Conflict(format!(
                "tool {}@{} is already registered", key.0, key.1,
            )));
        }
        let previous = self.versions.keys().any(|(logical, _)| logical == &key.0);
        let selection = if previous && self.selected.get(&key.0) == Some(&ModelSelection::Hidden) {
            RegistrationSelection::Unchanged
        } else if previous && self.selected.contains_key(&key.0) {
            RegistrationSelection::ReplaceAmbiguous
        } else {
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                io.retain_owned(std::mem::size_of::<(String, ModelSelection)>() as u64)?;
            }
            let name = crate::executor::stock_string_copy(&key.0, io)?;
            let selection = if previous {
                ModelSelection::Ambiguous
            } else {
                ModelSelection::Revision(crate::executor::stock_string_copy(&key.1, io)?)
            };
            RegistrationSelection::Insert(name, selection)
        };
        Ok(PreparedToolRegistration { key, tool, selection })
    }

    fn publish_registration(&mut self, prepared: PreparedToolRegistration) {
        match prepared.selection {
            RegistrationSelection::Unchanged => {}
            RegistrationSelection::ReplaceAmbiguous => {
                if let Some(selection) = self.selected.get_mut(&prepared.key.0) {
                    *selection = ModelSelection::Ambiguous;
                }
            }
            RegistrationSelection::Insert(name, selection) => {
                self.selected.insert(name, selection);
            }
        }
        self.versions.insert(prepared.key, prepared.tool);
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_owned_bytes(&self) -> Result<u64> {
        use crate::filesystem::UploadRecord as _;
        let mut bytes = std::mem::size_of::<Self>() as u64;
        for ((name, revision), tool) in &self.versions {
            let definition_deep = tool.definition.logical_owned_bytes()?;
            let row = (std::mem::size_of::<((String, String), Tool)>() as u64)
                .checked_add(name.capacity() as u64)
                .and_then(|bytes| bytes.checked_add(revision.capacity() as u64))
                .and_then(|bytes| bytes.checked_add(definition_deep));
            bytes = bytes.checked_add(row.ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid, reason: "tool registry ownership overflow",
            })?).ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid, reason: "tool registry ownership overflow",
            })?;
        }
        for (name, selection) in &self.selected {
            let deep = match selection {
                ModelSelection::Revision(revision) => revision.capacity() as u64,
                _ => 0,
            };
            bytes = bytes.checked_add(std::mem::size_of::<(String, ModelSelection)>() as u64)
                .and_then(|bytes| bytes.checked_add(name.capacity() as u64))
                .and_then(|bytes| bytes.checked_add(deep))
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid, reason: "tool registry ownership overflow",
                })?;
        }
        for name in self.catalog_revisions.keys() {
            bytes = bytes.checked_add(std::mem::size_of::<(String, [u8; 32])>() as u64)
                .and_then(|bytes| bytes.checked_add(name.capacity() as u64))
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid, reason: "tool registry ownership overflow",
                })?;
        }
        Ok(bytes)
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

    /// Removes a logical tool from future model catalogs without deleting its immutable versions.
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
        verifier.verify(scope, &mut None)?;
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
    pub(crate) fn get(
        &self, name: &str, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<&Tool>> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.charge(acyclic_fs::WorkCounters {
                items_examined: name.len() as u64, ..Default::default()
            })?;
        }
        if let Some((logical, revision)) = name.rsplit_once('@') {
            return self.get_version(logical, revision, io);
        }
        let Some(ModelSelection::Revision(revision)) = self.selected.get(name) else {
            return Ok(None);
        };
        self.get_version(name, revision, io)
    }

    /// Resolves a pinned revision independently of the model-visible choice.
    pub(crate) fn get_version(
        &self, name: &str, revision: &str,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<&Tool>> {
        let key = (
            crate::executor::stock_string_copy(name, io)?,
            crate::executor::stock_string_copy(revision, io)?,
        );
        Ok(self.versions.get(&key))
    }

    /// Copies only SDK-owned registry metadata on the original ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn clone_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        use crate::filesystem::UploadRecord as _;
        let mut custody = work.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let mut copied = Self::default();
        for ((name, revision), tool) in &self.versions {
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            io.retain_owned(std::mem::size_of::<ToolDefinition>() as u64)?;
            let definition = crate::model::clone_tool_definition(&tool.definition, Some(&io))?;
            let retained = (std::mem::size_of::<ToolDefinition>() as u64)
                .checked_add(definition.logical_owned_bytes()?)
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "tool definition ownership overflow" })?;
            let mut definition_hold = io.into_native_custody(retained)?;
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

    /// Returns the unambiguous model-visible selection in canonical order.
    pub fn definitions(&self, io: Option<&crate::executor::PreparationIo>) -> Result<Vec<ToolDefinition>> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io { io.retain_owned(std::mem::size_of::<Vec<ToolDefinition>>() as u64)?; }
        let mut definitions = crate::model::vec_with_capacity(self.selected.len(), io)?;
        for (name, selection) in &self.selected {
            let revision = match selection {
                ModelSelection::Revision(revision) => revision,
                ModelSelection::Hidden => continue,
                ModelSelection::Ambiguous => {
                    return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Conflict,
                        "tool requires an explicit model revision"));
                }
            };
            let tool = self
                .get_version(name, revision, io)?
                .ok_or_else(|| crate::executor::stock_boundary(io, crate::wire::ErrorCode::Storage,
                    "selected tool revision is not registered"))?;
            definitions.push(crate::model::clone_tool_definition(&tool.definition, io)?);
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
        conversation::Limits,
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
                None,
            )
            .expect("unbounded model call")
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
        let mut tools = ToolRegistry::default();
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
                projection: Arc::new(JsonToolProjection),
            })?;
        }
        assert!(tools.get("example.echo", None)?.is_none());
        assert!(matches!(tools.definitions(None), Err(Error::Conflict(_))));
        assert!(tools.get_version("example.echo", "1", None)?.is_some());
        tools.select_model_version("example.echo", "2")?;
        assert_eq!(tools.definitions(None)?[0].revision, "2");
        assert_eq!(
            tools
                .get("example.echo", None)?
                .map(|tool| tool.definition.revision.as_str()),
            Some("2")
        );
        tools.remove_from_model("example.echo")?;
        assert!(tools.definitions(None)?.is_empty());
        assert!(tools.get("example.echo", None)?.is_none());
        assert!(tools.get_version("example.echo", "1", None)?.is_some());
        assert!(tools.get_version("example.echo", "2", None)?.is_some());
        assert!(tools.get("example.echo@1", None)?.is_some());
        let mut replacement = tools
            .get_version("example.echo", "1", None)?
            .ok_or_else(|| Error::NotFound("test tool".into()))?
            .clone();
        replacement.definition.revision = "3".into();
        tools.register(replacement)?;
        assert!(tools.definitions(None)?.is_empty());
        assert!(tools.get_version("example.echo", "3", None)?.is_some());
        assert!(matches!(
            tools.remove_from_model("missing"),
            Err(Error::NotFound(_))
        ));
        tools.select_model_version("example.echo", "1")?;
        assert_eq!(tools.definitions(None)?[0].revision, "1");
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
        definition.validate(&mut None)?;
        let permissive = ToolDefinition {
            projection_schema: json!({}),
            ..definition.clone()
        };
        assert!(
            permissive
                .validate_projection(&json!("old bare projection"), Limits::default(), None)
                .is_err()
        );
        assert!(
            permissive
                .validate_projection(&json!({"kind":"parts","parts":[
                    {"kind":"tool_call","call_id":"nested","name":"nested","arguments":{}}
                ]}), Limits::default(), None)
                .is_err()
        );
        validate_value(&definition.output_schema, &json!({"count": 2}), "result", None)?;
        validate_value(
            &definition.projection_schema,
            &json!({"kind":"json","value":"two"}),
            "projection",
         None,)?;
        assert!(validate_value(&definition.output_schema, &json!("two"), "result", None).is_err());
        assert!(
            validate_value(
                &definition.projection_schema,
                &json!({"count": 2}),
                "projection"
            , None)
            .is_err()
        );
        let mut changed = definition.clone();
        changed.projection_schema = json!({"type": "integer"});
        assert_ne!(definition.digest()?, changed.digest()?);
        changed.projection_schema = json!({"type": "not-a-schema-type"});
        assert!(changed.validate(&mut None).is_err());
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
        let scope = issuer.issue("install", &Capabilities::new(["tool:install:example.echo"]), None, None, &mut None).expect("scope proof");
        let interaction_id = InteractionId::from_bytes([6; 16]);
        let approval = Approved {
            id: interaction_id,
            operation: operation_id,
            digest,
        };
        let tool = Tool {
            definition,
            executor: Arc::new(Executor),
            projection: Arc::new(JsonToolProjection),
        };
        let mut registry = ToolRegistry::default();
        assert!(
            registry
                .install_scoped(
                    tool.clone(),
                    OperationId::from_bytes([9; 16]),
                    &scope,
                    &issuer.verifier(&mut None).expect("verifier copy"),
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
                &issuer.verifier(&mut None).expect("verifier copy"),
                interaction_id,
                &approval,
            )
            .await?;
        assert!(registry.get("example.echo", None)?.is_some());
        Ok(())
    }
}
