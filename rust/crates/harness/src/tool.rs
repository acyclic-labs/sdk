//! Independently replaceable tool definitions, executors, and projections.

use crate::{
    Error, OperationId, Result,
    core::{AuthorityVerifier, InteractionState, Scope},
    interaction::{Interaction, InteractionResponse},
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

/// Model-visible tool definition with immutable schemas.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolDefinition {
    /// Stable namespaced tool name.
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
        if !self.name.contains('.')
            || self.name.chars().any(char::is_whitespace)
            || self.revision.trim().is_empty()
        {
            return Err(Error::Invalid("tool name must be namespaced".into()));
        }
        for schema in [&self.input_schema, &self.output_schema] {
            jsonschema::validator_for(schema)
                .map_err(|error| Error::Invalid(format!("invalid tool schema: {error}")))?;
        }
        Ok(())
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

/// Deterministic immutable-by-name registry assembled in application code.
#[derive(Clone, Default)]
pub struct ToolRegistry(BTreeMap<String, Tool>);

impl ToolRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub const fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Registers a tool, rejecting ambiguous replacement.
    pub fn register(&mut self, tool: Tool) -> Result<()> {
        tool.definition.validate()?;
        let name = tool.definition.name.clone();
        if self.0.insert(name.clone(), tool).is_some() {
            return Err(Error::Conflict(format!(
                "tool {name} is already registered"
            )));
        }
        Ok(())
    }

    /// Installs an agent-selected tool after exact scope and durable approval checks.
    pub fn install_scoped(
        &mut self,
        tool: Tool,
        installation_id: OperationId,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        approval: &InteractionState,
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
        tool.definition.validate()?;
        let digest = *blake3::hash(
            &serde_json::to_vec(&tool.definition)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        )
        .as_bytes();
        if !matches!(
            (&approval.interaction, &approval.response),
            (
                Interaction::Approval { operation_id, action_digest, .. },
                Some(InteractionResponse::Approval { approved: true, .. })
            ) if operation_id == &installation_id && action_digest == &digest
        ) {
            return Err(Error::Unauthorized(
                "tool installation lacks an exact durable approval".into(),
            ));
        }
        self.register(tool)
    }

    /// Returns one assembled tool.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Tool> {
        self.0.get(name)
    }

    /// Returns definitions in canonical name order.
    #[must_use]
    pub fn definitions(&self) -> Vec<ToolDefinition> {
        self.0
            .values()
            .map(|tool| tool.definition.clone())
            .collect()
    }
}

pub(crate) fn validate_value(schema: &Value, value: &Value, label: &str) -> Result<()> {
    jsonschema::validator_for(schema)
        .map_err(|error| Error::Invalid(format!("invalid {label} schema: {error}")))?
        .validate(value)
        .map_err(|error| Error::Invalid(format!("{label} failed validation: {error}")))
}

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
    fn scoped_install_requires_capability_and_exact_approval_operation() -> Result<()> {
        let definition = ToolDefinition {
            name: "example.echo".into(),
            revision: "1".into(),
            description: "Echo".into(),
            input_schema: json!({"type": "object"}),
            output_schema: json!({}),
        };
        let digest = *blake3::hash(
            &serde_json::to_vec(&definition).map_err(|error| Error::Invalid(error.to_string()))?,
        )
        .as_bytes();
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
        let approval = InteractionState {
            interaction: Interaction::approval("Install", operation_id, digest)?,
            response: Some(InteractionResponse::Approval {
                approved: true,
                reason: None,
            }),
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
                    &approval
                )
                .is_err()
        );
        registry.install_scoped(tool, operation_id, &scope, &issuer.verifier(), &approval)?;
        assert!(registry.get("example.echo").is_some());
        Ok(())
    }
}
