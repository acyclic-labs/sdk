//! Independently replaceable tool definitions, executors, and projections.

use crate::{
    Error, InteractionId, OperationId, Result,
    core::{AuthorityVerifier, Scope},
    registry::validate_component_label,
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

/// Model-visible tool definition with immutable schemas.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolDefinition {
    /// Stable bounded tool name; local names and explicit namespaces are both valid.
    pub name: String,
    /// Immutable revision of definition, executor, and projection semantics.
    pub revision: String,
    /// Model-visible description.
    pub description: String,
    /// JSON Schema for invocation arguments.
    pub input_schema: Value,
    /// JSON Schema for the successful result.
    pub output_schema: Value,
}

impl ToolDefinition {
    /// Validates the name and both schemas.
    pub fn validate(&self) -> Result<()> {
        validate_tool_name(&self.name)?;
        validate_component_label(&self.revision, "tool revision")?;
        if self.name.contains('@') || self.revision.contains('@') {
            return Err(Error::Invalid(
                "tool name and revision cannot contain the version separator".into(),
            ));
        }
        for schema in [&self.input_schema, &self.output_schema] {
            jsonschema::validator_for(schema)
                .map_err(|error| Error::Invalid(format!("invalid tool schema: {error}")))?;
        }
        Ok(())
    }

    /// Immutable approval identity for the exact model-visible definition.
    pub fn digest(&self) -> Result<[u8; 32]> {
        self.validate()?;
        crate::contract::canonical_json_digest(self)
    }
}

/// One admitted invocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolInvocation {
    /// Model-owned call identity.
    pub call_id: String,
    /// Registered tool name.
    pub name: String,
    /// Schema-validated arguments.
    pub arguments: Value,
}

impl ToolInvocation {
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

/// Result returned by a tool executor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolResult {
    /// Schema-validated structured value.
    pub value: Value,
}

/// Replaceable execution behavior for a tool.
pub trait ToolExecutor: Send + Sync {
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
