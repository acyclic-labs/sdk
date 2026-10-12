use super::{McpToolDefinition, McpToolTransport, remote::RemoteTool};
use crate::{
    Error, InteractionId, OperationId, Result,
    core::{AuthorityVerifier, Scope},
    tool::{Tool, ToolApprovalVerifier, ToolDefinition, ToolProjection, ToolRegistry},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Model schema exposure for the complete catalog.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum McpSchemaExposure {
    /// Publish all tool schemas.
    Eager,
    /// Publish the named tool schemas.
    Selected {
        /// Distinct server-local tool names.
        names: Vec<String>,
    },
}

/// Host-selected local discovery policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum McpDiscoveryPolicy {
    /// Reject discovery.
    Disabled,
    /// Permit bounded catalog search.
    Search,
}

/// Complete immutable remote catalog.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct McpCatalog {
    /// Host-selected namespace.
    pub server: String,
    /// Pinned configuration revision.
    pub revision: String,
    /// Model schema selection.
    pub schema_exposure: McpSchemaExposure,
    /// Local discovery selection.
    pub discovery: McpDiscoveryPolicy,
    /// Host-selected result projection schema.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmToolJsonSchema"))]
    pub projection_schema: Value,
    /// Complete remote tool contracts.
    pub tools: Vec<McpToolDefinition>,
}

/// Signed authority and exact durable approval for catalog installation.
pub struct McpCatalogInstallation<'a> {
    /// Approved installation operation.
    pub operation: OperationId,
    /// Retained approval interaction.
    pub interaction: InteractionId,
    /// Signed installation scope.
    pub scope: &'a Scope,
    /// Scope verifier.
    pub verifier: &'a AuthorityVerifier,
    /// Durable approval verifier.
    pub approval: &'a dyn ToolApprovalVerifier,
    /// Complete catalog tool allowance.
    pub maximum_tools: u32,
    /// Complete catalog byte allowance.
    pub maximum_bytes: u32,
}

impl McpCatalog {
    /// Pins the replacement, predecessor and finite allowances in the approval.
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
        crate::contract::canonical_json_digest(&(
            "harness/mcp-catalog-install/v1",
            self,
            previous
                .map(crate::contract::canonical_json_digest)
                .transpose()?,
            maximum_tools,
            maximum_bytes,
        ))
    }

    /// Installs after verifying signed scope and exact approval.
    pub async fn install_scoped(
        &self,
        registry: &mut ToolRegistry,
        previous: Option<&Self>,
        transport: &Arc<dyn McpToolTransport>,
        projection: &Arc<dyn ToolProjection>,
        installation: McpCatalogInstallation<'_>,
    ) -> Result<()> {
        self.validate_transport_binding(transport.as_ref())?;
        installation
            .verifier
            .verify(installation.scope, &mut None)?;
        if !installation
            .scope
            .capabilities()
            .contains(&format!("mcp:install:{}", self.server))
        {
            return Err(Error::Unauthorized(
                "scope does not permit MCP catalog installation".into(),
            ));
        }
        let digest = self.installation_digest(
            previous,
            installation.maximum_tools,
            installation.maximum_bytes,
        )?;
        installation
            .approval
            .verify(installation.interaction, installation.operation, digest)
            .await?;
        self.install(
            registry,
            previous,
            transport,
            projection,
            installation.maximum_tools,
            installation.maximum_bytes,
        )
    }

    /// Validates the complete catalog before registry mutation.
    pub fn validate(&self, maximum_tools: u32, maximum_bytes: u32) -> Result<()> {
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
            || serde_json::to_vec(self)
                .map_err(|e| Error::Invalid(e.to_string()))?
                .len() as u64
                > u64::from(maximum_bytes)
        {
            return Err(Error::Invalid("MCP catalog allowance exceeded".into()));
        }
        crate::contract::compile_json_schema(&self.projection_schema, "MCP projection", &mut None)?;
        let mut names = BTreeSet::new();
        for tool in &self.tools {
            if !names.insert(&tool.name) {
                return Err(Error::Conflict("duplicate MCP tool name".into()));
            }
            self.definition(tool)?.validate(&mut None)?;
            if tool.input_schema.get("type").and_then(Value::as_str) != Some("object") {
                return Err(Error::Invalid("MCP input schema must be an object".into()));
            }
            if let Some(schema) = &tool.output_schema {
                if schema.get("type").and_then(Value::as_str) != Some("object") {
                    return Err(Error::Invalid(
                        "MCP output schema must describe an object".into(),
                    ));
                }
                crate::contract::compile_json_schema(schema, "MCP output", &mut None)?;
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
            McpSchemaExposure::Selected { names } => names.iter().any(|value| value == name),
        }
    }

    /// Returns the selected model schemas in name order.
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

    fn definition(&self, remote: &McpToolDefinition) -> Result<ToolDefinition> {
        crate::registry::validate_component_label(&remote.name, "MCP tool")?;
        let mut output = json!({"type":"object", "required":["content","isError"],
            "properties":{"content":{"type":"array","items":{"type":"object"}},
                "structuredContent":{}, "isError":{"type":"boolean"}, "_meta":{"type":"object"}},
            "additionalProperties":false});
        if let Some(schema) = &remote.output_schema {
            output["if"] = json!({"properties":{"isError":{"const":false}}});
            output["then"] =
                json!({"required":["structuredContent"],"properties":{"structuredContent":schema}});
        }
        Ok(ToolDefinition {
            name: format!("mcp.{}.{}", self.server, remote.name),
            revision: self.revision.clone(),
            description: remote.description.clone(),
            input_schema: remote.input_schema.clone(),
            output_schema: output,
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

    /// Atomically replaces future selection while retaining admitted revisions.
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
        self.validate_transport_binding(transport.as_ref())?;
        if previous.is_some_and(|old| old.server != self.server) {
            return Err(Error::Conflict(
                "MCP replacement changes server namespace".into(),
            ));
        }
        let prefix = format!("mcp.{}.", self.server);
        let selected = registry
            .definitions(None)?
            .into_iter()
            .filter(|tool| tool.name.starts_with(&prefix))
            .map(|tool| (tool.name.clone(), tool))
            .collect::<BTreeMap<_, _>>();
        let expected = previous
            .into_iter()
            .flat_map(|old| {
                old.tools
                    .iter()
                    .filter(move |tool| old.selected(&tool.name))
                    .map(move |tool| old.definition(tool))
            })
            .map(|tool| tool.map(|tool| (tool.name.clone(), tool)))
            .collect::<Result<BTreeMap<_, _>>>()?;
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
                next.remove_from_model(&format!("mcp.{}.{}", self.server, remote.name))?;
            }
        }
        for remote in &self.tools {
            let definition = self.definition(remote)?;
            let name = definition.name.clone();
            next.register(Tool {
                definition,
                executor: Arc::new(RemoteTool {
                    server: self.server.clone(),
                    name: remote.name.clone(),
                    output_schema: remote.output_schema.clone(),
                    transport: Arc::clone(transport),
                }),
                projection: Arc::clone(projection),
            })?;
            if self.selected(&remote.name) {
                next.select_model_version(&name, &self.revision)?;
            } else {
                next.remove_from_model(&name)?;
            }
        }
        *registry = next;
        Ok(())
    }

    /// Searches the pinned complete catalog without granting call authority.
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
