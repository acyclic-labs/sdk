//! Optional MCP clients and dynamic tools over the ordinary Harness tool owner.
//!
//! Catalog replacement is explicit and atomic. A transport is a host capability;
//! neither constructing a client nor publishing a catalog performs remote I/O.

use crate::{
    Error, OperationId, Result,
    runtime::ToolContext,
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry,
        ToolResult,
    },
};
use acyclic_stream::BoxProviderFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc};

pub mod http;
mod rpc;
pub mod stdio;

/// Protocol revision implemented by this client, without legacy fallback.
pub const PROTOCOL_VERSION: &str = "2025-11-25";

/// Negotiated server initialization, retained with the admitted session.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "camelCase")]
pub struct McpInitializeResult {
    /// Explicit supported protocol revision, with no silent downgrade.
    pub protocol_version: String,
    /// Server-advertised feature set; it does not grant host authority.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmToolJsonValue"))]
    pub capabilities: Value,
    /// Server implementation name and version.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmToolJsonValue"))]
    pub server_info: Value,
    /// Optional server instructions, treated as external content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

impl McpInitializeResult {
    /// Rejects unsupported negotiation before the next protocol request.
    pub fn validate(&self) -> Result<()> {
        if self.protocol_version != PROTOCOL_VERSION
            || !self.capabilities.is_object()
            || self
                .server_info
                .get("name")
                .and_then(Value::as_str)
                .is_none()
            || self
                .server_info
                .get("version")
                .and_then(Value::as_str)
                .is_none()
        {
            return Err(Error::Unsupported(
                "MCP initialization contract unsupported".into(),
            ));
        }
        Ok(())
    }
}

/// One bounded discovery page. A host must fetch the complete catalog before
/// replacing its model-visible selection; a page is not a replacement catalog.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "camelCase")]
pub struct McpToolsPage {
    /// Remote contracts in this page.
    pub tools: Vec<McpToolDefinition>,
    /// Opaque server cursor for the next explicitly admitted discovery request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Remote JSON Schema contract returned by `tools/list`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "camelCase")]
pub struct McpToolDefinition {
    /// Server-local name, preserved independently of the Harness namespace.
    pub name: String,
    /// Optional human-facing title.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Description supplied by the server.
    #[serde(default)]
    pub description: String,
    /// Runtime-validated arguments.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmToolJsonSchema"))]
    pub input_schema: Value,
    /// Runtime-validated structured output when supplied by the server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "wasm", tsify(type = "WasmToolJsonSchema"))]
    pub output_schema: Option<Value>,
}

/// Canonical MCP result; content stays ordered and may be multimodal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "camelCase")]
pub struct McpToolResult {
    /// Protocol content blocks. Validation occurs at the protocol boundary.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmToolJsonValue[]"))]
    pub content: Vec<Value>,
    /// Structured value checked against the remote output contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "wasm", tsify(type = "WasmToolJsonValue"))]
    pub structured_content: Option<Value>,
    /// A tool failure is a recorded result, rather than a transport failure.
    #[serde(default)]
    pub is_error: bool,
    /// Opaque protocol metadata retained for the owning consumer.
    #[serde(default, rename = "_meta", skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "wasm", tsify(type = "WasmToolJsonValue"))]
    pub meta: Option<Value>,
}

/// Pinned host transport for one server revision. Dispatch is invoked only by
/// an admitted Harness tool. Reconciliation must query the exact operation;
/// it must never resend a possibly applied remote request.
pub trait McpToolTransport: acyclic_stream::ProviderPlatform {
    /// Calls one admitted remote tool.
    fn call<'a>(
        &'a self,
        operation: OperationId,
        name: &'a str,
        arguments: Value,
    ) -> BoxProviderFuture<'a, Result<McpToolResult>>;
    /// Calls under the enclosing task's scope and exact tool identity. Hosts
    /// needing task authority override this; operation-only transports keep
    /// their existing behavior.
    fn call_with_context<'a>(
        &'a self,
        context: ToolContext,
        name: &'a str,
        arguments: Value,
    ) -> BoxProviderFuture<'a, Result<McpToolResult>> {
        self.call(context.operation_id(), name, arguments)
    }
    /// Queries a prior operation without dispatching it again.
    fn reconcile<'a>(
        &'a self,
        operation: OperationId,
    ) -> BoxProviderFuture<'a, Result<Option<McpToolResult>>>;
    /// Queries under the same scoped task context without repeating dispatch.
    fn reconcile_with_context<'a>(
        &'a self,
        context: ToolContext,
    ) -> BoxProviderFuture<'a, Result<Option<McpToolResult>>> {
        self.reconcile(context.operation_id())
    }
}

/// Explicit model schema exposure for an installed complete catalog.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum McpSchemaExposure {
    /// Publish every schema at the next admission.
    Eager,
    /// Publish only these distinct server-local names. Hidden revisions remain
    /// available for explicit typed host admission and existing in-flight calls.
    Selected {
        /// Exact names from the complete catalog; empty means no model schemas.
        names: Vec<String>,
    },
}

/// Host-selected local discovery policy; neither variant grants call authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum McpDiscoveryPolicy {
    /// Decline discovery, including direct local search calls.
    Disabled,
    /// Permit bounded deterministic search over this pinned complete catalog.
    Search,
}

/// Bounded immutable remote catalog. The existing `ToolRegistry` is the only
/// execution registry; this value is a validated registration input.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct McpCatalog {
    /// Consumer-selected namespace; names do not convey remote authority.
    pub server: String,
    /// Exact transport/configuration/catalog revision selected by the host.
    pub revision: String,
    /// Explicit schema selection pinned with this configuration revision.
    pub schema_exposure: McpSchemaExposure,
    /// Explicit local discovery selection; notifications cannot change it.
    pub discovery: McpDiscoveryPolicy,
    /// Complete bounded catalog, not a partially fetched `tools/list` page.
    pub tools: Vec<McpToolDefinition>,
}

impl McpCatalog {
    /// Validates a complete catalog before any registry change.
    pub fn validate(&self, maximum_tools: u32, maximum_bytes: u32) -> Result<()> {
        crate::registry::validate_component_label(&self.server, "MCP server")?;
        if self.server.contains('.') {
            return Err(Error::Invalid(
                "MCP server namespace cannot contain a dot".into(),
            ));
        }
        crate::registry::validate_component_label(&self.revision, "MCP revision")?;
        if maximum_tools == 0
            || maximum_bytes == 0
            || self.tools.len() as u64 > u64::from(maximum_tools)
            || serde_json::to_vec(self)
                .map_err(|e| Error::Invalid(e.to_string()))?
                .len() as u64
                > u64::from(maximum_bytes)
        {
            return Err(Error::Invalid("MCP catalog allowance exceeded".into()));
        }
        let mut names = BTreeSet::new();
        for tool in &self.tools {
            if !names.insert(&tool.name) {
                return Err(Error::Conflict("duplicate MCP tool name".into()));
            }
            self.definition(tool)?.validate()?;
            if tool.input_schema.get("type") != Some(&json!("object")) {
                return Err(Error::Invalid("MCP input schema must be an object".into()));
            }
            if let Some(schema) = &tool.output_schema {
                if schema.get("type") != Some(&json!("object")) {
                    return Err(Error::Invalid(
                        "MCP output schema must describe an object".into(),
                    ));
                }
                crate::contract::compile_json_schema(schema, "MCP output")?;
            }
        }
        if let McpSchemaExposure::Selected { names: selected } = &self.schema_exposure {
            let mut unique = BTreeSet::new();
            for name in selected {
                if !names.contains(name) {
                    return Err(Error::Invalid("MCP exposure names an absent tool".into()));
                }
                if !unique.insert(name) {
                    return Err(Error::Conflict("duplicate MCP exposure name".into()));
                }
            }
        }
        Ok(())
    }

    fn selected_tools(&self) -> impl Iterator<Item = &McpToolDefinition> {
        let names: Option<BTreeSet<&str>> = match &self.schema_exposure {
            McpSchemaExposure::Eager => None,
            McpSchemaExposure::Selected { names } => {
                Some(names.iter().map(String::as_str).collect())
            }
        };
        self.tools.iter().filter(move |tool| {
            names
                .as_ref()
                .is_none_or(|names| names.contains(tool.name.as_str()))
        })
    }

    /// Returns only host-selected model schemas without changing authority or
    /// the complete discovery catalog. Both policies must validate first.
    pub fn model_definitions(
        &self,
        maximum_tools: u32,
        maximum_bytes: u32,
    ) -> Result<Vec<ToolDefinition>> {
        self.validate(maximum_tools, maximum_bytes)?;
        let mut definitions = self
            .selected_tools()
            .map(|remote| self.definition(remote))
            .collect::<Result<Vec<_>>>()?;
        definitions.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(definitions)
    }

    fn definition(&self, remote: &McpToolDefinition) -> Result<ToolDefinition> {
        crate::registry::validate_component_label(&remote.name, "MCP tool")?;
        let mut output_schema = serde_json::Map::from_iter([
            ("type".into(), json!("object")),
            ("required".into(), json!(["content", "isError"])),
            (
                "properties".into(),
                json!({"content":{"type":"array","items":{"type":"object"}},
                "structuredContent":{},"isError":{"type":"boolean"},"_meta":{"type":"object"}}),
            ),
            ("additionalProperties".into(), json!(false)),
        ]);
        if let Some(schema) = &remote.output_schema {
            output_schema.insert(
                "if".into(),
                json!({"properties":{"isError":{"const":false}}}),
            );
            output_schema.insert(
                "then".into(),
                json!({"required":["structuredContent"],"properties":{"structuredContent":schema}}),
            );
        }
        Ok(ToolDefinition {
            name: format!("mcp.{}.{}", self.server, remote.name),
            revision: self.revision.clone(),
            description: remote.description.clone(),
            input_schema: remote.input_schema.clone(),
            output_schema: Value::Object(output_schema),
        })
    }

    /// Publishes the complete next-admission catalog, retaining every old
    /// executor/revision for already admitted calls. Invalid replacements leave
    /// the old registry unchanged. `previous` must be the installed catalog.
    pub fn install(
        &self,
        registry: &mut ToolRegistry,
        previous: Option<&Self>,
        transport: &Arc<dyn McpToolTransport>,
        projection: &Arc<dyn ToolProjection>,
        maximum_tools: u32,
        maximum_bytes: u32,
    ) -> Result<()> {
        self.validate(maximum_tools, maximum_bytes)?;
        if previous.is_some_and(|old| old.server != self.server) {
            return Err(Error::Conflict(
                "MCP replacement changes server namespace".into(),
            ));
        }
        let prefix = format!("mcp.{}.", self.server);
        let selected = registry
            .definitions()?
            .into_iter()
            .filter(|definition| definition.name.starts_with(&prefix))
            .map(|definition| (definition.name.clone(), definition))
            .collect::<std::collections::BTreeMap<_, _>>();
        let expected = previous
            .into_iter()
            .flat_map(|old| old.selected_tools().map(|remote| old.definition(remote)))
            .map(|definition| definition.map(|definition| (definition.name.clone(), definition)))
            .collect::<Result<std::collections::BTreeMap<_, _>>>()?;
        if selected != expected {
            return Err(Error::Conflict(
                "MCP previous catalog is not the complete selection".into(),
            ));
        }
        let mut next = registry.clone();
        next.pin_catalog_revision(
            &prefix,
            previous
                .map(crate::contract::canonical_json_digest)
                .transpose()?,
            crate::contract::canonical_json_digest(self)?,
        )?;
        if let Some(old) = previous {
            for remote in &old.tools {
                let definition = old.definition(remote)?;
                next.withdraw_model_tool(&definition.name);
            }
        }
        for remote in &self.tools {
            let definition = self.definition(remote)?;
            next.register(Tool {
                definition: definition.clone(),
                executor: Arc::new(RemoteTool {
                    server: self.server.clone(),
                    remote: remote.clone(),
                    transport: Arc::clone(transport),
                }),
                projection: Arc::clone(projection),
            })?;
            next.withdraw_model_tool(&definition.name);
        }
        for remote in self.selected_tools() {
            let definition = self.definition(remote)?;
            next.select_model_version(&definition.name, &definition.revision)?;
        }
        *registry = next;
        Ok(())
    }

    /// Bounded deterministic discovery over an already pinned catalog. Search
    /// grants no permission to call the returned definitions.
    pub fn search(
        &self,
        query: &str,
        after: Option<&str>,
        maximum_results: u32,
        maximum_tools: u32,
        maximum_bytes: u32,
    ) -> Result<Vec<ToolDefinition>> {
        self.validate(maximum_tools, maximum_bytes)?;
        if self.discovery == McpDiscoveryPolicy::Disabled {
            return Err(Error::Unsupported(
                "MCP discovery disabled by host policy".into(),
            ));
        }
        if maximum_results == 0 {
            return Err(Error::Invalid(
                "MCP search needs a positive result allowance".into(),
            ));
        }
        let query = query.to_lowercase();
        let mut tools = self
            .tools
            .iter()
            .filter(|tool| {
                after.is_none_or(|cursor| tool.name.as_str() > cursor)
                    && (tool.name.to_lowercase().contains(&query)
                        || tool.description.to_lowercase().contains(&query))
            })
            .collect::<Vec<_>>();
        tools.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        tools
            .into_iter()
            .take(maximum_results as usize)
            .map(|tool| self.definition(tool))
            .collect()
    }
}

struct RemoteTool {
    server: String,
    remote: McpToolDefinition,
    transport: Arc<dyn McpToolTransport>,
}

impl RemoteTool {
    fn authorize_context(&self, context: &ToolContext, invocation: &ToolInvocation) -> Result<()> {
        if context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
        {
            return Err(Error::Unauthorized(
                "MCP tool context identity mismatch".into(),
            ));
        }
        self.authorize(Some(context.task().scope()), invocation)
    }

    fn result(&self, result: McpToolResult) -> Result<ToolResult> {
        if !result.is_error
            && let Some(schema) = &self.remote.output_schema
        {
            let value = result
                .structured_content
                .as_ref()
                .ok_or_else(|| Error::Invalid("MCP structured result missing".into()))?;
            crate::contract::validate_json_schema_value(schema, value, "MCP output")?;
        }
        let value =
            serde_json::to_value(result).map_err(|error| Error::Invalid(error.to_string()))?;
        rpc::validate_tool_result(&value)?;
        Ok(ToolResult { value })
    }
}

impl ToolExecutor for RemoteTool {
    fn authorize(
        &self,
        scope: Option<&crate::runtime::RuntimeScope>,
        _: &ToolInvocation,
    ) -> Result<()> {
        if !scope.is_some_and(|scope| {
            scope
                .grants()
                .contains(&format!("mcp:call:{}", self.server))
        }) {
            return Err(Error::Unauthorized("MCP server capability absent".into()));
        }
        Ok(())
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            self.result(
                self.transport
                    .call(
                        invocation.operation_id,
                        &self.remote.name,
                        invocation.arguments,
                    )
                    .await?,
            )
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            self.transport
                .reconcile(invocation.operation_id)
                .await?
                .map(|result| self.result(result))
                .transpose()
        })
    }

    fn execute_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            self.authorize_context(&context, &invocation)?;
            self.result(
                self.transport
                    .call_with_context(context, &self.remote.name, invocation.arguments)
                    .await?,
            )
        })
    }

    fn reconcile_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            self.authorize_context(&context, &invocation)?;
            self.transport
                .reconcile_with_context(context)
                .await?
                .map(|result| self.result(result))
                .transpose()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Capabilities, Outcome,
        conversation::Limits,
        runtime::{Bindings, RuntimeScope, TaskContext, TaskDefinition},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Transport(AtomicUsize);
    impl McpToolTransport for Transport {
        fn call<'a>(
            &'a self,
            _: OperationId,
            _: &'a str,
            _: Value,
        ) -> BoxProviderFuture<'a, Result<McpToolResult>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Ok(McpToolResult {
                    content: vec![json!({"type":"text","text":"ok"})],
                    structured_content: Some(json!({"value":1})),
                    is_error: false,
                    meta: None,
                })
            })
        }
        fn reconcile<'a>(
            &'a self,
            _: OperationId,
        ) -> BoxProviderFuture<'a, Result<Option<McpToolResult>>> {
            Box::pin(async { Ok(None) })
        }
    }
    struct Projection;
    impl ToolProjection for Projection {
        fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
            Ok(result.value.clone())
        }
    }
    fn catalog(revision: &str, names: &[&str]) -> McpCatalog {
        McpCatalog { server:"fixture".into(), revision:revision.into(), schema_exposure:McpSchemaExposure::Eager,
            discovery:McpDiscoveryPolicy::Search, tools:names.iter().map(|name| McpToolDefinition {
            name:(*name).into(), title:None, description:format!("Find {name}"), input_schema:json!({"type":"object"}),
            output_schema:Some(json!({"type":"object","required":["value"],"properties":{"value":{"type":"integer"}}})) }).collect() }
    }

    #[derive(Default)]
    struct ContextTransport {
        calls: AtomicUsize,
        reconciliations: AtomicUsize,
    }

    fn context_result(context: &ToolContext, structured_content: Value) -> McpToolResult {
        McpToolResult {
            content: vec![],
            structured_content: Some(structured_content),
            is_error: false,
            meta: Some(json!({
                "task": context.task().id(),
                "operation": context.operation_id(),
                "call": context.call_id(),
                "serverGrant": context.task().scope().grants().contains("mcp:call:fixture"),
                "durableTask": context.task().durable_task_id(),
            })),
        }
    }

    impl McpToolTransport for ContextTransport {
        fn call<'a>(
            &'a self,
            _: OperationId,
            _: &'a str,
            _: Value,
        ) -> BoxProviderFuture<'a, Result<McpToolResult>> {
            Box::pin(async { Err(Error::Unsupported("task context required".into())) })
        }
        fn reconcile<'a>(
            &'a self,
            _: OperationId,
        ) -> BoxProviderFuture<'a, Result<Option<McpToolResult>>> {
            Box::pin(async { Err(Error::Unsupported("task context required".into())) })
        }
        fn call_with_context<'a>(
            &'a self,
            context: ToolContext,
            name: &'a str,
            arguments: Value,
        ) -> BoxProviderFuture<'a, Result<McpToolResult>> {
            Box::pin(async move {
                assert_eq!(name, "a");
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(context_result(&context, arguments))
            })
        }
        fn reconcile_with_context<'a>(
            &'a self,
            context: ToolContext,
        ) -> BoxProviderFuture<'a, Result<Option<McpToolResult>>> {
            self.reconciliations.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(Some(context_result(&context, json!({"value":1})))) })
        }
    }

    async fn check_context_dispatch(task: TaskContext, tool: &Tool) -> Result<()> {
        let operation = OperationId::from_bytes([17; 16]);
        let context = ToolContext::new(task.clone(), operation, "provider-call")?;
        let invocation = ToolInvocation {
            operation_id: operation,
            call_id: "provider-call".into(),
            name: tool.definition.name.clone(),
            arguments: json!({"value":1}),
        };
        let expected = json!({"task":task.id(), "operation":operation,
            "call":"provider-call", "serverGrant":true, "durableTask":null});
        let result = tool
            .executor
            .execute_with_context(context.clone(), invocation.clone())
            .await?;
        assert_eq!(result.value["_meta"], expected);
        let recovered = tool
            .executor
            .reconcile_with_context(context.clone(), invocation.clone())
            .await?
            .ok_or_else(|| Error::NotFound("context result".into()))?;
        assert_eq!(recovered.value["_meta"], expected);
        let denied = task.scoped(Capabilities::new([] as [String; 0]), Limits::default())?;
        for wrong in [
            ToolContext::new(
                task.clone(),
                OperationId::from_bytes([18; 16]),
                "provider-call",
            )?,
            ToolContext::new(task, operation, "different-call")?,
            ToolContext::new(denied, operation, "provider-call")?,
        ] {
            assert!(matches!(
                tool.executor
                    .execute_with_context(wrong.clone(), invocation.clone())
                    .await,
                Err(Error::Unauthorized(_))
            ));
            assert!(matches!(
                tool.executor
                    .reconcile_with_context(wrong, invocation.clone())
                    .await,
                Err(Error::Unauthorized(_))
            ));
        }
        let mut invalid = invocation;
        invalid.arguments = json!({"value":"wrong"});
        assert!(matches!(
            tool.executor.execute_with_context(context, invalid).await,
            Err(Error::Invalid(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn admitted_mcp_tools_preserve_context_and_reject_identity_or_scope_changes() -> Result<()>
    {
        let transport = Arc::new(ContextTransport::default());
        let bound: Arc<dyn McpToolTransport> = transport.clone();
        let projection: Arc<dyn ToolProjection> = Arc::new(Projection);
        let mut registry = ToolRegistry::new();
        catalog("1", &["a"]).install(&mut registry, None, &bound, &projection, 8, 8192)?;
        let tool = registry
            .get("mcp.fixture.a")
            .cloned()
            .ok_or_else(|| Error::NotFound("context fixture".into()))?;
        let legacy = Arc::new(Transport::default());
        let legacy_bound: Arc<dyn McpToolTransport> = legacy.clone();
        let mut legacy_registry = ToolRegistry::new();
        catalog("1", &["a"]).install(
            &mut legacy_registry,
            None,
            &legacy_bound,
            &projection,
            8,
            8192,
        )?;
        let legacy_tool = legacy_registry
            .get("mcp.fixture.a")
            .cloned()
            .ok_or_else(|| Error::NotFound("legacy fixture".into()))?;
        let definition = TaskDefinition::live("mcp.context", "1", move |task, (): ()| {
            let tool = tool.clone();
            let legacy_tool = legacy_tool.clone();
            async move {
                check_context_dispatch(task.clone(), &tool).await?;
                let operation = OperationId::from_bytes([19; 16]);
                let context = ToolContext::new(task, operation, "legacy-call")?;
                let invocation = ToolInvocation {
                    operation_id: operation,
                    call_id: "legacy-call".into(),
                    name: legacy_tool.definition.name.clone(),
                    arguments: json!({}),
                };
                assert!(
                    legacy_tool
                        .executor
                        .reconcile_with_context(context.clone(), invocation.clone())
                        .await?
                        .is_none()
                );
                assert_eq!(
                    legacy_tool
                        .executor
                        .execute_with_context(context, invocation)
                        .await?
                        .value["structuredContent"]["value"],
                    1
                );
                Ok(1_u32)
            }
        })?;
        let mut bindings = Bindings::local();
        bindings.tasks.register(definition)?;
        bindings.scope = RuntimeScope::new(
            Capabilities::new(["task:spawn:mcp.context@1", "mcp:call:fixture"]),
            Limits::default(),
        )?;
        let harness = bindings.build()?;
        let definition = harness.task::<(), u32>("mcp.context")?;
        assert_eq!(
            harness.spawn(&definition, ()).await?.result().await?,
            Outcome::Succeeded(1)
        );
        assert_eq!(transport.calls.load(Ordering::SeqCst), 2);
        assert_eq!(transport.reconciliations.load(Ordering::SeqCst), 1);
        assert_eq!(legacy.0.load(Ordering::SeqCst), 1);
        Ok(())
    }
    #[test]
    fn catalog_reload_is_atomic_and_retains_pinned_removed_tools() -> Result<()> {
        let transport = Arc::new(Transport::default());
        let bound: Arc<dyn McpToolTransport> = transport.clone();
        let projection: Arc<dyn ToolProjection> = Arc::new(Projection);
        let mut registry = ToolRegistry::new();
        let old = catalog("1", &["a", "b"]);
        old.install(&mut registry, None, &bound, &projection, 8, 8192)?;
        let pinned = registry
            .get("mcp.fixture.a")
            .cloned()
            .ok_or_else(|| Error::NotFound("fixture".into()))?;
        let mut invalid = catalog("2", &["a", "a"]);
        assert!(
            invalid
                .install(&mut registry, Some(&old), &bound, &projection, 8, 8192)
                .is_err()
        );
        assert_eq!(registry.definitions()?.len(), 2);
        invalid.tools.pop();
        invalid.install(&mut registry, Some(&old), &bound, &projection, 8, 8192)?;
        assert_eq!(registry.definitions()?.len(), 1);
        assert_eq!(registry.definitions()?[0].revision, "2");
        assert!(registry.get("mcp.fixture.b").is_none());
        assert!(registry.get_version("mcp.fixture.b", "1").is_some());
        assert_eq!(pinned.definition.revision, "1");
        assert_eq!(transport.0.load(Ordering::SeqCst), 0);
        // A stale replacement cannot withdraw a later selection.
        assert!(
            catalog("3", &["c"])
                .install(&mut registry, Some(&old), &bound, &projection, 8, 8192)
                .is_err()
        );
        assert_eq!(registry.definitions()?[0].revision, "2");
        Ok(())
    }
    #[tokio::test]
    async fn search_grants_no_authority_and_reconciliation_never_dispatches() -> Result<()> {
        let catalog = catalog("1", &["c", "a", "b"]);
        assert_eq!(
            catalog.search("find", None, 1, 8, 8192)?[0].name,
            "mcp.fixture.a"
        );
        assert_eq!(
            catalog.search("find", Some("a"), 1, 8, 8192)?[0].name,
            "mcp.fixture.b"
        );
        let transport = Arc::new(Transport::default());
        let bound: Arc<dyn McpToolTransport> = transport.clone();
        let projection: Arc<dyn ToolProjection> = Arc::new(Projection);
        let mut registry = ToolRegistry::new();
        catalog.install(&mut registry, None, &bound, &projection, 8, 8192)?;
        let tool = registry
            .get("mcp.fixture.a")
            .ok_or_else(|| Error::NotFound("fixture".into()))?;
        let invocation = ToolInvocation {
            operation_id: OperationId::from_bytes([1; 16]),
            call_id: "call".into(),
            name: tool.definition.name.clone(),
            arguments: json!({}),
        };
        assert!(tool.executor.authorize(None, &invocation).is_err());
        let scope = RuntimeScope::new(Capabilities::new(["mcp:call:other"]), Limits::default())?;
        assert!(tool.executor.authorize(Some(&scope), &invocation).is_err());
        let scope = RuntimeScope::new(Capabilities::new(["mcp:call:fixture"]), Limits::default())?;
        tool.executor.authorize(Some(&scope), &invocation)?;
        assert!(tool.executor.reconcile(invocation.clone()).await?.is_none());
        assert_eq!(transport.0.load(Ordering::SeqCst), 0);
        assert_eq!(
            tool.executor.execute(invocation).await?.value["structuredContent"]["value"],
            1
        );
        assert_eq!(transport.0.load(Ordering::SeqCst), 1);
        Ok(())
    }
    #[test]
    fn schema_bounds_namespace_and_result_negative_controls() {
        let mut input = catalog("1", &["a"]);
        assert!(input.validate(1, 1).is_err());
        input.server = "fixture.other".into();
        assert!(input.validate(1, 8192).is_err());
        input.server = "fixture".into();
        input.tools[0].input_schema = json!({"type":"unknown"});
        assert!(input.validate(1, 8192).is_err());
        let tool = RemoteTool {
            server: "fixture".into(),
            remote: catalog("1", &["a"]).tools.remove(0),
            transport: Arc::new(Transport::default()),
        };
        assert!(
            tool.result(McpToolResult {
                content: vec![],
                structured_content: None,
                is_error: false,
                meta: None
            })
            .is_err()
        );
        assert!(
            tool.result(McpToolResult {
                content: vec![],
                structured_content: Some(json!({"value":"wrong"})),
                is_error: false,
                meta: None
            })
            .is_err()
        );
        assert!(
            tool.result(McpToolResult {
                content: vec![],
                structured_content: None,
                is_error: true,
                meta: None
            })
            .is_ok()
        );
    }

    #[tokio::test]
    async fn reload_during_remote_call_retains_admitted_transport() -> Result<()> {
        struct PendingTransport {
            response: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<McpToolResult>>>,
        }
        impl McpToolTransport for PendingTransport {
            fn call<'a>(
                &'a self,
                _: OperationId,
                _: &'a str,
                _: Value,
            ) -> BoxProviderFuture<'a, Result<McpToolResult>> {
                Box::pin(async move {
                    let response = self
                        .response
                        .lock()
                        .map_err(|_| Error::Invalid("fixture lock".into()))?
                        .take()
                        .ok_or_else(|| Error::Invalid("fixture dispatched twice".into()))?;
                    response
                        .await
                        .map_err(|_| Error::Invalid("fixture canceled".into()))
                })
            }
            fn reconcile<'a>(
                &'a self,
                _: OperationId,
            ) -> BoxProviderFuture<'a, Result<Option<McpToolResult>>> {
                Box::pin(async { Ok(None) })
            }
        }
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let old_transport: Arc<dyn McpToolTransport> = Arc::new(PendingTransport {
            response: std::sync::Mutex::new(Some(receiver)),
        });
        let new_transport: Arc<dyn McpToolTransport> = Arc::new(Transport::default());
        let projection: Arc<dyn ToolProjection> = Arc::new(Projection);
        let mut registry = ToolRegistry::new();
        let old_catalog = catalog("1", &["a"]);
        old_catalog.install(&mut registry, None, &old_transport, &projection, 8, 8192)?;
        let admitted = registry
            .get("mcp.fixture.a")
            .cloned()
            .ok_or_else(|| Error::NotFound("fixture".into()))?;
        let invocation = ToolInvocation {
            operation_id: OperationId::from_bytes([3; 16]),
            call_id: "in-flight".into(),
            name: "mcp.fixture.a".into(),
            arguments: json!({}),
        };
        let scope = RuntimeScope::new(Capabilities::new(["mcp:call:fixture"]), Limits::default())?;
        admitted.executor.authorize(Some(&scope), &invocation)?;
        let mut call = admitted.executor.execute(invocation.clone());
        assert!(futures::poll!(&mut call).is_pending());
        catalog("2", &["a"]).install(
            &mut registry,
            Some(&old_catalog),
            &new_transport,
            &projection,
            8,
            8192,
        )?;
        sender
            .send(McpToolResult {
                content: vec![],
                structured_content: Some(json!({"value":11})),
                is_error: false,
                meta: None,
            })
            .map_err(|_| Error::Invalid("fixture dropped".into()))?;
        assert_eq!(call.await?.value["structuredContent"]["value"], 11);
        let next = registry
            .get("mcp.fixture.a")
            .ok_or_else(|| Error::NotFound("fixture".into()))?;
        assert_eq!(next.definition.revision, "2");
        assert_eq!(
            next.executor.execute(invocation).await?.value["structuredContent"]["value"],
            1
        );
        Ok(())
    }

    #[test]
    fn all_catalog_replacement_subsets_preserve_admitted_revisions() -> Result<()> {
        let transport: Arc<dyn McpToolTransport> = Arc::new(Transport::default());
        let projection: Arc<dyn ToolProjection> = Arc::new(Projection);
        let names = ["a", "b", "c"];
        // 8 initial subsets x 8 replacement subsets of three logical names.
        // This checks production registry transitions, not a separate model.
        for initial in 0_u8..8 {
            for replacement in 0_u8..8 {
                let selected = |mask: u8| {
                    names
                        .iter()
                        .enumerate()
                        .filter_map(|(index, name)| (mask & (1 << index) != 0).then_some(*name))
                        .collect::<Vec<_>>()
                };
                let old = catalog("1", &selected(initial));
                let new = catalog("2", &selected(replacement));
                let mut registry = ToolRegistry::new();
                old.install(&mut registry, None, &transport, &projection, 3, 8192)?;
                new.install(&mut registry, Some(&old), &transport, &projection, 3, 8192)?;
                assert_eq!(
                    registry.definitions()?.len(),
                    replacement.count_ones() as usize
                );
                for (index, name) in names.iter().enumerate() {
                    let name = format!("mcp.fixture.{name}");
                    assert_eq!(
                        registry.get_version(&name, "1").is_some(),
                        initial & (1 << index) != 0
                    );
                    assert_eq!(
                        registry.get(&name).is_some(),
                        replacement & (1 << index) != 0
                    );
                }
                // The registry stamp also rejects stale empty catalog inputs.
                {
                    let before = registry.definitions()?;
                    assert!(
                        catalog("3", &[])
                            .install(&mut registry, Some(&old), &transport, &projection, 3, 8192)
                            .is_err()
                    );
                    assert_eq!(before, registry.definitions()?);
                }
            }
        }
        Ok(())
    }

    #[test]
    fn every_selected_exposure_reload_is_atomic_including_empty_visibility() -> Result<()> {
        let transport: Arc<dyn McpToolTransport> = Arc::new(Transport::default());
        let projection: Arc<dyn ToolProjection> = Arc::new(Projection);
        let names = ["a", "b", "c"];
        let selected = |revision: &str, mask: u8| {
            let mut value = catalog(revision, &names);
            value.schema_exposure = McpSchemaExposure::Selected {
                names: names
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| mask & (1 << *index) != 0)
                    .map(|(_, name)| (*name).into())
                    .collect(),
            };
            value
        };
        for initial in 0_u8..8 {
            for replacement in 0_u8..8 {
                let old = selected("1", initial);
                let new = selected("2", replacement);
                let mut registry = ToolRegistry::new();
                old.install(&mut registry, None, &transport, &projection, 3, 8192)?;
                assert_eq!(registry.definitions()?, old.model_definitions(3, 8192)?);
                new.install(&mut registry, Some(&old), &transport, &projection, 3, 8192)?;
                assert_eq!(registry.definitions()?, new.model_definitions(3, 8192)?);
                assert_eq!(
                    registry.definitions()?.len(),
                    replacement.count_ones() as usize
                );
                assert_eq!(new.search("", None, 3, 3, 8192)?.len(), 3);
                for name in names {
                    assert!(
                        registry
                            .get_version(&format!("mcp.fixture.{name}"), "1")
                            .is_some()
                    );
                    assert!(
                        registry
                            .get_version(&format!("mcp.fixture.{name}"), "2")
                            .is_some()
                    );
                }
                let before = registry.definitions()?;
                assert!(
                    selected("3", replacement)
                        .install(&mut registry, Some(&old), &transport, &projection, 3, 8192)
                        .is_err()
                );
                assert!(
                    selected("3", replacement)
                        .install(&mut registry, None, &transport, &projection, 3, 8192)
                        .is_err()
                );
                assert_eq!(registry.definitions()?, before);
            }
        }
        let mut invalid = selected("1", 1);
        let mut registry = ToolRegistry::new();
        for names in [vec!["absent".into()], vec!["a".into(), "a".into()]] {
            invalid.schema_exposure = McpSchemaExposure::Selected { names };
            assert!(
                invalid
                    .install(&mut registry, None, &transport, &projection, 3, 8192)
                    .is_err()
            );
            assert!(registry.definitions()?.is_empty());
        }
        let mut disabled = selected("1", 0);
        disabled.discovery = McpDiscoveryPolicy::Disabled;
        disabled.install(&mut registry, None, &transport, &projection, 3, 8192)?;
        assert!(registry.definitions()?.is_empty());
        assert!(matches!(
            disabled.search("", None, 3, 3, 8192),
            Err(Error::Unsupported(_))
        ));
        Ok(())
    }
}
