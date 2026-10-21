use super::{McpToolDefinition, McpToolTransport, remote::RemoteTool};
use crate::{
    Error, InteractionId, OperationId, Result,
    core::{AuthorityVerifier, Scope},
    tool::{Tool, ToolApprovalVerifier, ToolDefinition, ToolProjection, ToolRegistry},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc};

/// Model schema selection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum McpSchemaExposure {
    /// Publish all schemas.
    Eager,
    /// Publish named schemas.
    Selected {
        /// Distinct server-local names.
        names: Vec<String>,
    },
}

/// Local discovery policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum McpDiscoveryPolicy {
    /// Reject discovery.
    Disabled,
    /// Permit bounded search.
    Search,
}

/// Complete immutable remote catalog.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct McpCatalog {
    /// Host namespace.
    pub server: String,
    /// Pinned configuration revision.
    pub revision: String,
    /// Model schema selection.
    pub schema_exposure: McpSchemaExposure,
    /// Local discovery policy.
    pub discovery: McpDiscoveryPolicy,
    /// Host result projection schema.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmToolJsonSchema"))]
    pub projection_schema: Value,
    /// Complete tool contracts.
    pub tools: Vec<McpToolDefinition>,
}

impl McpCatalog {
    /// Digests the replacement, predecessor and allowances.
    pub fn installation_digest(
        &self,
        previous: Option<&Self>,
        maximum_tools: u32,
        maximum_bytes: u32,
    ) -> Result<[u8; 32]> {
        self.validate(maximum_tools, maximum_bytes)?;
        if previous.is_some_and(|old| old.server != self.server) {
            return Err(Error::Conflict(
                "MCP replacement changes server namespace".into(),
            ));
        }
        crate::executor::json::canonical_json_digest(
            &(
                "harness/mcp-catalog-install/v1",
                self,
                previous
                    .map(|old| {
                        crate::executor::json::canonical_json_digest(old, u64::MAX, &mut None)
                    })
                    .transpose()?,
                maximum_tools,
                maximum_bytes,
            ),
            u64::MAX,
            &mut None,
        )
    }

    /// Verifies signed scope and approval before installation.
    pub async fn install_scoped(
        &self,
        registry: &mut ToolRegistry,
        previous: Option<&Self>,
        remote: (&Arc<dyn McpToolTransport>, &Arc<dyn ToolProjection>),
        operation: (OperationId, InteractionId),
        authorization: (&Scope, &AuthorityVerifier, &dyn ToolApprovalVerifier),
        bounds: (u32, u32),
    ) -> Result<()> {
        let (transport, projection) = remote;
        let (operation, interaction) = operation;
        let (scope, verifier, approval) = authorization;
        let (maximum_tools, maximum_bytes) = bounds;
        self.validate_transport_binding(transport.as_ref())?;
        verifier.verify(scope, &mut None)?;
        if !scope
            .capabilities()
            .contains(&format!("mcp:install:{}", self.server))
        {
            return Err(Error::Unauthorized(
                "scope does not permit MCP catalog installation".into(),
            ));
        }
        approval
            .verify(
                interaction,
                operation,
                self.installation_digest(previous, maximum_tools, maximum_bytes)?,
            )
            .await?;
        self.install(
            registry,
            previous,
            transport,
            projection,
            maximum_tools,
            maximum_bytes,
        )
    }

    /// Validates the complete catalog before mutation.
    pub fn validate(&self, maximum_tools: u32, maximum_bytes: u32) -> Result<()> {
        self.validate_checked(maximum_tools, maximum_bytes, &|| Ok(()))
    }

    fn validate_checked(
        &self,
        maximum_tools: u32,
        maximum_bytes: u32,
        check: &impl Fn() -> Result<()>,
    ) -> Result<()> {
        check()?;
        crate::registry::validate_component_label(&self.server, "MCP server")?;
        crate::registry::validate_component_label(&self.revision, "MCP revision")?;
        if self.server.contains('.') {
            return Err(Error::Invalid(
                "MCP server namespace cannot contain a dot".into(),
            ));
        }
        if maximum_tools == 0
            || maximum_bytes == 0
            || self.tools.len() as u64 > u64::from(maximum_tools)
        {
            return Err(Error::Invalid("MCP catalog allowance exceeded".into()));
        }
        crate::executor::json::json_byte_length(self, u64::from(maximum_bytes), &mut None)?;
        crate::executor::compile_json_schema(&self.projection_schema, "MCP projection", &mut None)?;
        check()?;
        let mut names = BTreeSet::new();
        for tool in &self.tools {
            check()?;
            if !names.insert(&tool.name) {
                return Err(Error::Conflict("duplicate MCP tool name".into()));
            }
            self.definition(tool)?.validate_checked(&mut None, check)?;
            if tool.input_schema.get("type").and_then(Value::as_str) != Some("object") {
                return Err(Error::Invalid("MCP input schema must be an object".into()));
            }
            if let Some(schema) = &tool.output_schema {
                if schema.get("type").and_then(Value::as_str) != Some("object") {
                    return Err(Error::Invalid(
                        "MCP output schema must describe an object".into(),
                    ));
                }
                crate::executor::compile_json_schema(schema, "MCP output", &mut None)?;
                check()?;
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

    fn selected(&self, name: &str) -> bool {
        match &self.schema_exposure {
            McpSchemaExposure::Eager => true,
            McpSchemaExposure::Selected { names } => names.iter().any(|selected| selected == name),
        }
    }

    fn definition(&self, remote: &McpToolDefinition) -> Result<ToolDefinition> {
        crate::registry::validate_component_label(&remote.name, "MCP tool")?;
        let mut output = serde_json::Map::from_iter([
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
            output.insert(
                "if".into(),
                json!({"properties":{"isError":{"const":false}}}),
            );
            output.insert(
                "then".into(),
                json!({"required":["structuredContent"],"properties":{"structuredContent":schema}}),
            );
        }
        Ok(ToolDefinition {
            name: format!("mcp.{}.{}", self.server, remote.name),
            revision: self.revision.clone(),
            description: remote.description.clone(),
            input_schema: remote.input_schema.clone(),
            output_schema: Value::Object(output),
            projection_schema: self.projection_schema.clone(),
        })
    }

    fn validate_transport_binding(&self, transport: &dyn McpToolTransport) -> Result<()> {
        if let Some(policy) = transport.callback_policy() {
            policy.validate()?;
            if policy.server != self.server || policy.revision != self.revision {
                return Err(Error::Conflict(
                    "MCP callback route differs from catalog revision".into(),
                ));
            }
        }
        Ok(())
    }

    /// Returns selected model schemas in name order.
    pub fn model_definitions(
        &self,
        maximum_tools: u32,
        maximum_bytes: u32,
    ) -> Result<Vec<ToolDefinition>> {
        self.validate(maximum_tools, maximum_bytes)?;
        let mut definitions = self
            .tools
            .iter()
            .filter(|tool| self.selected(&tool.name))
            .map(|tool| self.definition(tool))
            .collect::<Result<Vec<_>>>()?;
        definitions.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(definitions)
    }

    /// Replaces future selection atomically, retaining admitted revisions.
    pub fn install(
        &self,
        registry: &mut ToolRegistry,
        previous: Option<&Self>,
        transport: &Arc<dyn McpToolTransport>,
        projection: &Arc<dyn ToolProjection>,
        maximum_tools: u32,
        maximum_bytes: u32,
    ) -> Result<()> {
        self.install_checked(
            registry,
            previous,
            transport,
            projection,
            (maximum_tools, maximum_bytes),
            &|| Ok(()),
        )
    }

    pub(super) fn install_checked(
        &self,
        registry: &mut ToolRegistry,
        previous: Option<&Self>,
        transport: &Arc<dyn McpToolTransport>,
        projection: &Arc<dyn ToolProjection>,
        bounds: (u32, u32),
        check: &impl Fn() -> Result<()>,
    ) -> Result<()> {
        self.validate_checked(bounds.0, bounds.1, check)?;
        check()?;
        self.validate_transport_binding(transport.as_ref())?;
        if previous.is_some_and(|old| old.server != self.server) {
            return Err(Error::Conflict(
                "MCP replacement changes server namespace".into(),
            ));
        }
        let prefix = format!("mcp.{}.", self.server);
        let mut selected = registry.definitions(None)?;
        selected.retain(|tool| tool.name.starts_with(&prefix));
        let mut expected = Vec::new();
        if let Some(old) = previous {
            for tool in &old.tools {
                if old.selected(&tool.name) {
                    expected.push(old.definition(tool)?);
                }
            }
        }
        expected.sort_by(|a, b| a.name.cmp(&b.name));
        if selected != expected {
            return Err(Error::Conflict(
                "MCP previous catalog is not the complete selection".into(),
            ));
        }
        let mut next = registry.clone();
        next.pin_catalog_revision(
            &prefix,
            previous
                .map(|old| crate::executor::json::canonical_json_digest(old, u64::MAX, &mut None))
                .transpose()?,
            crate::executor::json::canonical_json_digest(self, u64::MAX, &mut None)?,
        )?;
        if let Some(old) = previous {
            for remote in &old.tools {
                next.remove_from_model(&format!("mcp.{}.{}", self.server, remote.name))?;
            }
        }
        for remote in &self.tools {
            check()?;
            let definition = self.definition(remote)?;
            let name = definition.name.clone();
            next.register_checked(
                (
                    Tool {
                        hook_names: &[],
                        definition,
                        executor: Arc::new(RemoteTool {
                            server: self.server.clone(),
                            name: remote.name.clone(),
                            output_schema: remote.output_schema.clone(),
                            transport: Arc::clone(transport),
                        }),
                        projection: Arc::clone(projection),
                    },
                    None,
                ),
                &mut None,
                None,
                check,
            )?;
            if self.selected(&remote.name) {
                next.select_model_version(&name, &self.revision)?;
            } else {
                next.remove_from_model(&name)?;
            }
        }
        check()?;
        *registry = next;
        Ok(())
    }

    /// Searches the pinned catalog without granting authority.
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
