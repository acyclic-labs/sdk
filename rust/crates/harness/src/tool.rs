//! Independently replaceable tool definitions, executors, and projections.

use crate::{
    Error, InteractionId, OperationId, Result,
    core::{AuthorityVerifier, Scope},
    registry::validate_component_label,
};
use crate::conversation::FileRef;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

/// Version of the structured model-visible admission feedback envelope.
pub const TOOL_REJECTION_FEEDBACK_VERSION: u32 = 2;
const TOOL_REJECTION_ERROR_MAX_BYTES: usize = 2_048;

/// Durable, model-visible evidence that a tool call was refused before effect
/// admission. The call, argument, and schema digests make this feedback
/// usable only for the exact rejected call; a forged result cannot authorize a
/// different historical call.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRejectionFeedback {
    /// Version of this envelope.
    pub version: u32,
    /// Stable rejection class (for example `invalid_arguments`).
    pub kind: String,
    /// Model-owned call identity.
    pub call_id: String,
    /// Registered tool name as requested by the model.
    pub name: String,
    /// Canonical digest of the rejected arguments.
    pub arguments_digest: [u8; 32],
    /// Canonical digest of the pinned input schema used for validation.
    pub schema_digest: [u8; 32],
    /// Digest of the exact bounded error text shown to the model.
    pub error_digest: [u8; 32],
}

impl ToolRejectionFeedback {
    /// Creates feedback for a schema-invalid invocation.
    pub fn invalid_arguments(
        invocation: &ToolInvocation,
        schema: &Value,
        error: &str,
    ) -> Result<Self> {
        validate_rejection_error(error)?;
        Ok(Self {
            version: TOOL_REJECTION_FEEDBACK_VERSION,
            kind: "invalid_arguments".into(),
            call_id: invocation.call_id.clone(),
            name: invocation.name.clone(),
            arguments_digest: crate::contract::canonical_json_digest(&invocation.arguments)?,
            schema_digest: crate::contract::canonical_json_digest(schema)?,
            error_digest: crate::contract::canonical_json_digest(&error)?,
        })
    }

    /// Validates the envelope and returns its canonical model value.
    pub fn to_model_value(&self, error: &str) -> Result<Value> {
        validate_rejection_error(error)?;
        if self.version != TOOL_REJECTION_FEEDBACK_VERSION || self.kind != "invalid_arguments" {
            return Err(Error::Invalid(
                "tool rejection feedback version or kind is invalid".into(),
            ));
        }
        Self::validate_identity(&self.call_id, &self.name)?;
        if self.error_digest != crate::contract::canonical_json_digest(&error)? {
            return Err(Error::Conflict(
                "tool rejection feedback error differs from its authenticated payload".into(),
            ));
        }
        Ok(serde_json::json!({
            "kind": "tool_rejection",
            "error": error,
            "rejection": self,
        }))
    }

    /// Extracts and validates feedback from a model-visible tool result.
    pub fn from_model_value(value: &Value) -> Result<Option<Self>> {
        let Some(object) = value.as_object() else {
            return Ok(None);
        };
        if object.get("kind") != Some(&Value::String("tool_rejection".into())) {
            return Ok(None);
        }
        if object.len() != 3 || !object.contains_key("error") || !object.contains_key("rejection") {
            return Err(Error::Invalid(
                "tool rejection feedback envelope has unexpected fields".into(),
            ));
        }
        let error = object.get("error").and_then(Value::as_str).ok_or_else(|| {
            Error::Invalid("tool rejection feedback error is not a string".into())
        })?;
        validate_rejection_error(error)?;
        let rejection = object
            .get("rejection")
            .ok_or_else(|| Error::Invalid("tool rejection feedback is missing".into()))?;
        let feedback: Self = serde_json::from_value(rejection.clone())
            .map_err(|error| Error::Invalid(format!("invalid tool rejection feedback: {error}")))?;
        if feedback.version != TOOL_REJECTION_FEEDBACK_VERSION
            || feedback.kind != "invalid_arguments"
        {
            return Err(Error::Invalid(
                "tool rejection feedback version or kind is invalid".into(),
            ));
        }
        if feedback.error_digest != crate::contract::canonical_json_digest(&error)? {
            return Err(Error::Conflict(
                "tool rejection feedback error digest does not match its payload".into(),
            ));
        }
        Self::validate_identity(&feedback.call_id, &feedback.name)?;
        Ok(Some(feedback))
    }

    fn validate_identity(call_id: &str, name: &str) -> Result<()> {
        ToolInvocation::validate_identity(call_id, name)
    }
}

fn validate_rejection_error(error: &str) -> Result<()> {
    if error.is_empty()
        || error.len() > TOOL_REJECTION_ERROR_MAX_BYTES
        || error.chars().any(char::is_control)
    {
        return Err(Error::Invalid(
            "tool rejection feedback error is outside the bounded text contract".into(),
        ));
    }
    Ok(())
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
    /// JSON Schema for the successful result before model projection.
    pub output_schema: Value,
    /// JSON Schema for the value returned to the model after projection.
    ///
    /// This is intentionally separate from `output_schema`: executors may
    /// return a rich private value while a projection exposes a narrower,
    /// stable model contract.
    pub model_output_schema: Value,
}

impl ToolDefinition {
    /// Validates the name and all pinned schemas.
    pub fn validate(&self) -> Result<()> {
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
            &self.model_output_schema,
        ] {
            jsonschema::validator_for(schema)
                .map_err(|error| Error::Invalid(format!("invalid tool schema: {error}")))?;
        }
        validate_model_output_reference_schema(&self.model_output_schema, 0)?;
        Ok(())
    }

    /// Immutable approval identity for the exact model-visible definition.
    pub fn digest(&self) -> Result<[u8; 32]> {
        self.validate()?;
        crate::contract::canonical_json_digest(self)
    }

    /// Extracts immutable content references declared by the model-output
    /// schema. Tool output is model-visible JSON, so references must be
    /// declared by the pinned schema rather than discovered by scanning
    /// arbitrary objects.
    pub(crate) fn model_output_file_refs(&self, value: &Value) -> Result<Vec<FileRef>> {
        self.validate()?;
        let validator = jsonschema::validator_for(&self.model_output_schema)
            .map_err(|error| Error::Invalid(format!("invalid model output schema: {error}")))?;
        if let Err(error) = validator.validate(value) {
            return Err(Error::Invalid(format!("tool projection does not match schema: {error}")));
        }
        let mut refs = Vec::new();
        collect_declared_file_refs(&self.model_output_schema, value, &mut refs, 0)?;
        Ok(refs)
    }
}

const MAX_DECLARED_FILE_REF_DEPTH: usize = 32;
const MAX_DECLARED_FILE_REFS: usize = 256;
const MAX_REFERENCE_SCHEMA_DEPTH: usize = 32;

const UNSUPPORTED_REFERENCE_SCHEMA_KEYWORDS: &[&str] = &[
    "$ref",
    "$dynamicRef",
    "allOf",
    "anyOf",
    "oneOf",
    "not",
    "if",
    "then",
    "else",
    "dependentSchemas",
    "dependentRequired",
    "patternProperties",
    "prefixItems",
    "contains",
    "propertyNames",
    "unevaluatedProperties",
    "unevaluatedItems",
    "$defs",
    "definitions",
];

fn schema_contains_file_ref_annotation(schema: &Value, depth: usize) -> Result<bool> {
    if depth > MAX_REFERENCE_SCHEMA_DEPTH {
        return Err(Error::Invalid(
            "model-output reference schema exceeds depth limit".into(),
        ));
    }
    match schema {
        Value::Object(object) => {
            if object
                .get("x-acyclic-file-ref")
                .and_then(Value::as_bool)
                == Some(true)
            {
                return Ok(true);
            }
            let mut children = Vec::new();
            for key in [
                "items",
                "additionalProperties",
                "contains",
                "propertyNames",
                "unevaluatedProperties",
                "unevaluatedItems",
                "not",
                "if",
                "then",
                "else",
            ] {
                if let Some(value) = object.get(key) {
                    children.push(value);
                }
            }
            for key in [
                "properties",
                "patternProperties",
                "$defs",
                "definitions",
                "dependentSchemas",
            ] {
                if let Some(values) = object.get(key).and_then(Value::as_object) {
                    children.extend(values.values());
                }
            }
            for key in ["allOf", "anyOf", "oneOf", "prefixItems"] {
                if let Some(values) = object.get(key).and_then(Value::as_array) {
                    children.extend(values);
                }
            }
            children.into_iter().try_fold(false, |found, value| {
                Ok(found || schema_contains_file_ref_annotation(value, depth + 1)?)
            })
        }
        Value::Array(values) => values.iter().try_fold(false, |found, value| {
            Ok(found || schema_contains_file_ref_annotation(value, depth + 1)?)
        }),
        _ => Ok(false),
    }
}

fn validate_model_output_reference_schema(schema: &Value, depth: usize) -> Result<()> {
    if depth > MAX_REFERENCE_SCHEMA_DEPTH {
        return Err(Error::Invalid(
            "model-output reference schema exceeds depth limit".into(),
        ));
    }
    let Some(object) = schema.as_object() else {
        return Ok(());
    };
    if let Some(annotation) = object.get("x-acyclic-file-ref")
        && annotation != &Value::Bool(true)
    {
        return Err(Error::Invalid(
            "x-acyclic-file-ref must be true when present".into(),
        ));
    }
    let local_annotation = object
        .get("x-acyclic-file-ref")
        .and_then(Value::as_bool)
        == Some(true);
    for keyword in UNSUPPORTED_REFERENCE_SCHEMA_KEYWORDS {
        let Some(value) = object.get(*keyword) else { continue; };
        let contains_annotation = if matches!(
            *keyword,
            "patternProperties" | "$defs" | "definitions" | "dependentSchemas"
        ) {
            value
                .as_object()
                .map(|values| {
                    values.values().try_fold(false, |found, child| {
                        Ok::<bool, Error>(found
                            || schema_contains_file_ref_annotation(child, depth + 1)?)
                    })
                })
                .transpose()?
                .unwrap_or(false)
        } else {
            schema_contains_file_ref_annotation(value, depth + 1)?
        };
        if local_annotation || contains_annotation {
            return Err(Error::Invalid(format!(
                "unsupported model-output reference schema keyword {keyword}"
            )));
        }
    }
    if let Some(additional) = object.get("additionalProperties")
        && additional.is_object()
        && schema_contains_file_ref_annotation(additional, depth + 1)?
    {
        return Err(Error::Invalid(
            "dynamic model-output properties cannot carry file references".into(),
        ));
    }
    if let Some(properties) = object.get("properties").and_then(Value::as_object) {
        for child in properties.values() {
            validate_model_output_reference_schema(child, depth + 1)?;
        }
    }
    if let Some(items) = object.get("items") {
        if items.is_object() {
            validate_model_output_reference_schema(items, depth + 1)?;
        } else if items.is_array() && schema_contains_file_ref_annotation(items, depth + 1)? {
            return Err(Error::Invalid(
                "tuple item schemas cannot carry file references".into(),
            ));
        }
    }
    Ok(())
}

fn collect_declared_file_refs(
    schema: &Value,
    value: &Value,
    refs: &mut Vec<FileRef>,
    depth: usize,
) -> Result<()> {
    if depth > MAX_DECLARED_FILE_REF_DEPTH {
        return Err(Error::Invalid("tool output reference schema exceeds depth limit".into()));
    }
    let Some(schema_object) = schema.as_object() else { return Ok(()); };
    if !schema_contains_file_ref_annotation(schema, 0)? {
        return Ok(());
    }
    if schema_object.get("x-acyclic-file-ref").and_then(Value::as_bool) == Some(true) {
        let reference: FileRef = serde_json::from_value(value.clone())
            .map_err(|error| Error::Invalid(format!("declared tool output FileRef is invalid: {error}")))?;
        reference.validate()?;
        if refs.len() >= MAX_DECLARED_FILE_REFS {
            return Err(Error::Invalid("tool output declares too many file references".into()));
        }
        refs.push(reference);
        return Ok(());
    }
    if let Some(properties) = schema_object.get("properties").and_then(Value::as_object) {
        if let Some(object) = value.as_object() {
            for (name, child_schema) in properties {
                if let Some(child) = object.get(name) {
                    collect_declared_file_refs(child_schema, child, refs, depth + 1)?;
                }
            }
        }
    }
    if let Some(items) = schema_object.get("items") {
        let Some(array) = value.as_array() else { return Ok(()); };
        if items.is_boolean() { return Ok(()); }
        if array.len() > MAX_DECLARED_FILE_REFS {
            return Err(Error::Invalid("tool output reference array exceeds limit".into()));
        }
        for child in array {
            collect_declared_file_refs(items, child, refs, depth + 1)?;
        }
    }
    Ok(())
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
        let digest = blake3::hash(
            &[
                b"harness:tool-call:v2".as_slice(),
                parent.into_bytes().as_slice(),
                &step.to_be_bytes(),
                call_id.as_bytes(),
            ]
            .concat(),
        );
        let mut identity = [0_u8; 16];
        identity.copy_from_slice(&digest.as_bytes()[..16]);
        Self {
            operation_id: OperationId::from_bytes(identity),
            call_id,
            name,
            arguments,
        }
    }

    /// Rejects identities that could execute successfully but fail later when
    /// their canonical conversation record is published.
    pub fn validate(&self) -> Result<()> {
        Self::validate_identity(&self.call_id, &self.name)
    }

    /// Validates an event identity before it can enter the durable model journal.
    pub fn validate_identity(call_id: &str, name: &str) -> Result<()> {
        validate_tool_name(name)?;
        if call_id.is_empty()
            || call_id.len() > 255
            || call_id.chars().any(char::is_control)
            || call_id.contains('/')
            || call_id.contains('\\')
            || call_id == "."
            || call_id == ".."
        {
            return Err(Error::Invalid("tool call identity is invalid".into()));
        }
        Ok(())
    }
}

fn validate_tool_name(name: &str) -> Result<()> {
    validate_component_label(name, "tool name")
}

/// Runtime provenance for a model tool batch. This metadata is never added to
/// model-visible arguments or conversation content.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelToolContext {
    /// Owning durable model turn.
    pub parent_operation: OperationId,
    /// Zero-based step whose complete exchange admits child activation.
    pub step: u32,
    /// Authenticated durable task that owns this model turn, when the host
    /// admitted the turn as part of a task. This is runtime provenance only;
    /// it is never accepted from model-visible content.
    #[serde(default)]
    pub task_id: Option<crate::TaskId>,
}

impl ModelToolContext {
    /// Rejects an invocation routed from another turn, step, or call identity.
    pub fn validate_invocation(&self, invocation: &ToolInvocation) -> Result<()> {
        invocation.validate()?;
        let expected = ToolInvocation::for_model_call(
            self.parent_operation,
            self.step,
            invocation.call_id.clone(),
            invocation.name.clone(),
            invocation.arguments.clone(),
        );
        if expected.operation_id != invocation.operation_id {
            return Err(Error::Conflict(
                "model tool provenance differs from invocation".into(),
            ));
        }
        Ok(())
    }

    /// Stable completed-batch publication dependency. A fork may be admitted
    /// before this operation completes, but cannot dispatch a child model yet.
    #[must_use]
    pub fn publication_operation(&self) -> OperationId {
        let digest = blake3::hash(
            &[
                b"harness:model-batch-publication:v1".as_slice(),
                self.parent_operation.into_bytes().as_slice(),
                &self.step.to_be_bytes(),
            ]
            .concat(),
        );
        let mut identity = [0_u8; 16];
        identity.copy_from_slice(&digest.as_bytes()[..16]);
        OperationId::from_bytes(identity)
    }
}

/// Result returned by a tool executor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolResult {
    /// Schema-validated structured value.
    pub value: Value,
}

/// Replaceable execution behavior for a tool.
pub trait ToolExecutor: Send + Sync {
    /// Checks invocation-specific resource grants before a result is replayed,
    /// dispatched, or reconciled. A scoped adapter must reject a missing scope.
    fn authorize(
        &self,
        _scope: Option<&crate::runtime::RuntimeScope>,
        _invocation: &ToolInvocation,
    ) -> Result<()> {
        Ok(())
    }

    /// Executes an already admitted invocation.
    fn execute<'a>(&'a self, invocation: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>>;

    /// Executes under the model batch's runtime provenance after durable admission.
    /// Context-aware tools use this to bind deferred child activation to the
    /// completed exchange. Legacy adapters receive the original invocation.
    fn execute_in_model_batch<'a>(
        &'a self,
        context: ModelToolContext,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            context.validate_invocation(&invocation)?;
            self.execute(invocation).await
        })
    }

    /// Reconciles with exactly the original model batch provenance. The default
    /// never retries execution when the outcome is unknown.
    fn reconcile_in_model_batch<'a>(
        &'a self,
        context: ModelToolContext,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            context.validate_invocation(&invocation)?;
            self.reconcile(invocation).await
        })
    }

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
}

/// Replaceable mapping from tool results into model-visible context.
pub trait ToolProjection: Send + Sync {
    /// Projects an invocation/result pair without side effects.
    fn project(&self, invocation: &ToolInvocation, result: &ToolResult) -> Result<Value>;
}

/// Trusted journal check for a resolved approval bound to one exact tool definition.
pub trait ToolApprovalVerifier: Send + Sync {
    /// Rejects absent, declined, mismatched, or indeterminate approvals.
    fn verify<'a>(
        &'a self,
        interaction_id: InteractionId,
        operation_id: OperationId,
        definition_digest: [u8; 32],
    ) -> BoxFuture<'a, Result<()>>;
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

/// Deterministic registry retaining every pinned revision. A model request
/// exposes exactly one selected revision per logical tool name.
#[derive(Clone, Default)]
pub struct ToolRegistry {
    versions: BTreeMap<(String, String), Tool>,
    selected: BTreeMap<String, String>,
}

impl ToolRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            versions: BTreeMap::new(),
            selected: BTreeMap::new(),
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
            self.selected.remove(&name);
        } else {
            self.selected.insert(name, revision);
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
        self.selected.insert(name.to_owned(), revision.to_owned());
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
        let revision = self.selected.get(name)?;
        self.get_version(name, revision)
    }

    /// Resolves a pinned revision independently of the model-visible choice.
    #[must_use]
    pub fn get_version(&self, name: &str, revision: &str) -> Option<&Tool> {
        self.versions.get(&(name.to_owned(), revision.to_owned()))
    }

    /// Returns the unambiguous model-visible selection in canonical order.
    pub fn definitions(&self) -> Result<Vec<ToolDefinition>> {
        let names = self
            .versions
            .keys()
            .map(|(name, _)| name)
            .collect::<std::collections::BTreeSet<_>>();
        let mut definitions = Vec::with_capacity(names.len());
        for name in names {
            let revision = self.selected.get(name).ok_or_else(|| {
                Error::Conflict(format!("tool {name} requires an explicit model revision"))
            })?;
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
            Ok(result.value.clone())
        }
    }

    fn reference_definition(schema: Value) -> ToolDefinition {
        ToolDefinition {
            name: "example.references".into(),
            revision: "1".into(),
            description: "test".into(),
            input_schema: json!({"type": "object"}),
            output_schema: json!({"type": "object"}),
            model_output_schema: schema,
        }
    }

    #[test]
    fn model_output_reference_walker_rejects_forgery_and_unsupported_schema() {
        let valid_complex = reference_definition(json!({
            "oneOf": [{"type": "null"}, {"type": "string"}],
            "additionalProperties": true,
            "type": "null",
            "properties": {"ignored": {"type": "object"}}
        }));
        assert!(valid_complex.model_output_file_refs(&Value::Null).unwrap().is_empty());

        let forged = reference_definition(json!({
            "type": "object",
            "properties": {"file": {"type": "object", "x-acyclic-file-ref": true}}
        }));
        assert!(matches!(
            forged.model_output_file_refs(&json!({"file": {"path": "forged"}})),
            Err(Error::Invalid(_))
        ));
        for (keyword, schema) in [
            (
                "oneOf",
                json!({"oneOf": [{"type": "object", "properties": {"file": {"x-acyclic-file-ref": true}}}]}),
            ),
            (
                "prefixItems",
                json!({"prefixItems": [{"type": "object", "properties": {"file": {"x-acyclic-file-ref": true}}}]}),
            ),
            (
                "patternProperties",
                json!({"patternProperties": {"^file$": {"type": "object", "properties": {"nested": {"x-acyclic-file-ref": true}}}}}),
            ),
        ] {
            let definition = reference_definition(schema);
            let result = definition.model_output_file_refs(&Value::Null);
            assert!(matches!(result, Err(Error::Invalid(ref error)) if error.contains(keyword)), "{keyword}: {result:?}");
        }
        let annotated_defs = reference_definition(json!({
            "$ref": "#/$defs/file",
            "$defs": {"file": {"type": "object", "x-acyclic-file-ref": true}}
        }));
        assert!(matches!(
            annotated_defs.model_output_file_refs(&Value::Null),
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            reference_definition(json!({"$ref": []})).validate(),
            Err(Error::Invalid(_))
        ));
    }

    #[test]
    fn reference_limits_apply_only_to_annotated_values() -> Result<()> {
        let ordinary = reference_definition(json!({
            "type": "array",
            "items": {"type": "integer"}
        }));
        let ordinary_values = Value::Array((0..257).map(|value| json!(value)).collect());
        assert!(ordinary.model_output_file_refs(&ordinary_values)?.is_empty());

        let const_object = reference_definition(json!({
            "const": {"x-acyclic-file-ref": true},
            "type": "object"
        }));
        assert!(const_object
            .model_output_file_refs(&json!({"x-acyclic-file-ref": true}))?
            .is_empty());

        let reference = FileRef::new(
            crate::conversation::VolumeRef::new(
                crate::resources::ProviderRef::new("test", "filesystem", "2")?,
                "volume",
                crate::conversation::VolumeClass::Project,
                crate::conversation::VolumeOwner::Project("test".into()),
            )?,
            "file.txt",
            "generation-1",
            crate::conversation::FileDescriptor::from_bytes(b"x", "text/plain")?,
            "file.txt",
        )?;
        let annotated = reference_definition(json!({
            "type": "array",
            "items": {"type": "object", "x-acyclic-file-ref": true}
        }));
        let encoded = serde_json::to_value(&reference)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let values = Value::Array(
            std::iter::repeat_n(encoded, MAX_DECLARED_FILE_REFS + 1).collect(),
        );
        let result = annotated.model_output_file_refs(&values);
        assert!(matches!(result, Err(Error::Invalid(ref error)) if error.contains("exceeds limit")), "{result:?}");
        Ok(())
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
    fn rejection_feedback_has_a_bounded_strict_envelope() -> Result<()> {
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "call".into(),
            name: "example.echo".into(),
            arguments: json!({"value": 1}),
        };
        let feedback = ToolRejectionFeedback::invalid_arguments(
            &invocation,
            &json!({"type": "object"}),
            "invalid",
        )?;
        let value = feedback.to_model_value("invalid")?;
        assert_eq!(
            ToolRejectionFeedback::from_model_value(&value)?,
            Some(feedback.clone())
        );
        let mut forged = value.clone();
        forged["injected"] = json!(true);
        assert!(matches!(
            ToolRejectionFeedback::from_model_value(&forged),
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            ToolRejectionFeedback::from_model_value(&json!({
                "kind": "tool_rejection",
                "rejection": value["rejection"]
            })),
            Err(Error::Invalid(_))
        ));
        assert_eq!(
            ToolRejectionFeedback::from_model_value(&json!({"rejection": "business-value"}))?,
            None
        );
        let mut changed_error = value.clone();
        changed_error["error"] = json!("another error");
        assert!(matches!(
            ToolRejectionFeedback::from_model_value(&changed_error),
            Err(Error::Conflict(_))
        ));
        assert!(matches!(
            feedback.to_model_value(&"x".repeat(2_049)),
            Err(Error::Invalid(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn model_batch_context_refuses_cross_turn_step_and_call_routing() -> Result<()> {
        let context = ModelToolContext {
            parent_operation: OperationId::new(),
            step: 2,
            task_id: None,
        };
        let make = |parent, step, call: &str| {
            ToolInvocation::for_model_call(
                parent,
                step,
                call.into(),
                "example.echo".into(),
                Value::Null,
            )
        };
        let invocation = make(context.parent_operation, context.step, "call");
        context.validate_invocation(&invocation)?;
        let decoded: ModelToolContext =
            serde_json::from_value(serde_json::to_value(context).unwrap()).unwrap();
        assert_eq!(decoded, context);
        for mut invalid in [
            make(OperationId::new(), context.step, "call"),
            make(context.parent_operation, context.step + 1, "call"),
            make(context.parent_operation, context.step, "other"),
        ] {
            invalid.call_id = "call".into();
            assert!(matches!(
                Executor
                    .execute_in_model_batch(context, invalid.clone())
                    .await,
                Err(Error::Conflict(_))
            ));
            assert!(matches!(
                Executor.reconcile_in_model_batch(context, invalid).await,
                Err(Error::Conflict(_))
            ));
        }
        assert_eq!(
            context.publication_operation(),
            decoded.publication_operation()
        );
        assert_ne!(
            context.publication_operation(),
            make(context.parent_operation, context.step, "publication").operation_id
        );
        assert_ne!(
            context.publication_operation(),
            ModelToolContext {
                step: context.step + 1,
                ..context
            }
            .publication_operation()
        );
        Ok(())
    }

    #[test]
    fn model_tool_context_task_binding_is_optional_for_legacy_data_and_round_trips() -> Result<()> {
        let operation = OperationId::from_bytes([21; 16]);
        let legacy = serde_json::json!({
            "parent_operation": operation,
            "step": 3,
        });
        let decoded: ModelToolContext =
            serde_json::from_value(legacy).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(decoded.task_id, None);

        let bound = ModelToolContext {
            parent_operation: operation,
            step: 3,
            task_id: Some(crate::TaskId::from_bytes([22; 16])),
        };
        let encoded =
            serde_json::to_value(bound).map_err(|error| Error::Invalid(error.to_string()))?;
        let round_trip: ModelToolContext =
            serde_json::from_value(encoded).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(round_trip, bound);
        assert_eq!(
            round_trip.publication_operation(),
            decoded.publication_operation()
        );
        Ok(())
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
                    model_output_schema: json!({}),
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
        Ok(())
    }

    struct Approved {
        id: InteractionId,
        operation: OperationId,
        digest: [u8; 32],
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
            model_output_schema: json!({}),
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
