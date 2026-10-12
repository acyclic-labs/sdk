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
        tools.register(
            (
                Tool {
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
                },
                None,
            ),
            &mut None,
            None,
        )?;
    }
    assert!(tools.get("example.echo", None, None)?.is_none());
    assert!(matches!(tools.definitions(None), Err(Error::Conflict(_))));
    assert!(tools.get("example.echo", Some("1"), None)?.is_some());
    tools.select_model_version("example.echo", "2")?;
    assert_eq!(tools.definitions(None)?[0].revision, "2");
    assert_eq!(
        tools
            .get("example.echo", None, None)?
            .map(|tool| tool.definition.revision.as_str()),
        Some("2")
    );
    tools.remove_from_model("example.echo")?;
    assert!(tools.definitions(None)?.is_empty());
    assert!(tools.get("example.echo", None, None)?.is_none());
    assert!(tools.get("example.echo", Some("1"), None)?.is_some());
    assert!(tools.get("example.echo", Some("2"), None)?.is_some());
    assert!(tools.get("example.echo@1", None, None)?.is_some());
    let mut replacement = tools
        .get("example.echo", Some("1"), None)?
        .ok_or_else(|| Error::NotFound("test tool".into()))?
        .clone();
    replacement.definition.revision = "3".into();
    tools.register((replacement, None), &mut None, None)?;
    assert!(tools.definitions(None)?.is_empty());
    assert!(tools.get("example.echo", Some("3"), None)?.is_some());
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
            .validate_projection(
                &json!({"kind":"parts","parts":[
                    {"kind":"tool_call","call_id":"nested","name":"nested","arguments":{}}
                ]}),
                Limits::default(),
                None
            )
            .is_err()
    );
    crate::contract::validate_json_schema_value(
        &definition.output_schema,
        &json!({"count": 2}),
        "result",
        &mut None,
    )?;
    crate::contract::validate_json_schema_value(
        &definition.projection_schema,
        &json!({"kind":"json","value":"two"}),
        "projection",
        &mut None,
    )?;
    assert!(
        crate::contract::validate_json_schema_value(
            &definition.output_schema,
            &json!("two"),
            "result",
            &mut None
        )
        .is_err()
    );
    assert!(
        crate::contract::validate_json_schema_value(
            &definition.projection_schema,
            &json!({"count": 2}),
            "projection",
            &mut None
        )
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
    let scope = issuer
        .issue(
            "install",
            &Capabilities::new(["tool:install:example.echo"]),
            None,
            None,
            &mut None,
        )
        .expect("scope proof");
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
    assert!(registry.get("example.echo", None, None)?.is_some());
    Ok(())
}
