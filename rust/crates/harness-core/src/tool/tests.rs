use super::*;
use crate::{
    Capabilities,
    conversation::Limits,
    core::{AggregateKind, Authority, AuthorityIssuer},
};
use futures::FutureExt as _;
use serde_json::json;

pub(super) fn file(bytes: &[u8]) -> Result<crate::conversation::FileRef> {
    use crate::{
        AgentId,
        conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
        resources::ProviderRef,
    };
    FileRef::new(
        VolumeRef::new(
            ProviderRef::new("test".into(), "filesystem".into(), "1".into(), &mut None)?,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([11; 16])),
            &mut None,
        )?,
        "file.txt".into(),
        "generation-1".into(),
        FileDescriptor::from_bytes(bytes, "text/plain", &mut None)?,
        "file.txt".into(),
        &mut None,
    )
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
                    hook_names: &[],
                    definition: ToolDefinition {
                        name: "example.echo".into(),
                        revision: revision.into(),
                        description: "Echo".into(),
                        input_schema: json!({}),
                        output_schema: json!({}),
                        projection_schema: crate::tool::json_projection_schema(json!({})),
                    },
                    executor: files::read()?.executor,
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
        description: "Structured result with text projection".into(),
        input_schema: json!({"type":"null"}),
        output_schema: json!({"type":"object","required":["count"],"properties":{"count":{"type":"integer"}},"additionalProperties":false}),
        projection_schema: json_projection_schema(json!({"type":"string"})),
    };
    definition.validate(&mut None)?;
    let mut changed = definition.clone();
    changed.projection_schema = json!({});
    for value in [
        json!("old bare projection"),
        json!({"kind":"parts","parts":[{"kind":"tool_call","call_id":"nested","name":"nested","arguments":{}}]}),
    ] {
        assert!(
            changed
                .validate_projection(&value, Limits::default(), None)
                .is_err()
        );
    }
    for (schema, value, valid) in [
        (&definition.output_schema, json!({"count":2}), true),
        (
            &definition.projection_schema,
            json!({"kind":"json","value":"two"}),
            true,
        ),
        (&definition.output_schema, json!("two"), false),
        (&definition.projection_schema, json!({"count":2}), false),
    ] {
        assert_eq!(
            crate::executor::validate_json_schema_value(schema, &value, "result", &mut None)
                .is_ok(),
            valid
        );
    }
    changed.projection_schema = json!({"type":"integer"});
    assert_ne!(definition.digest()?, changed.digest()?);
    changed.projection_schema = json!({"type":"not-a-schema-type"});
    assert!(changed.validate(&mut None).is_err());
    let mut missing = json!(definition);
    missing
        .as_object_mut()
        .expect("definition")
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
        hook_names: &[],
        definition,
        executor: files::read()?.executor,
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

/// A projection outside its pinned schema is the tool's mistake on both paths: it comes back as
/// the inner error for the model, never as the outer failure that stops a native turn.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn projection_schema_violation_is_the_tools_mistake_on_the_native_path() -> Result<()> {
    let definition = ToolDefinition {
        name: "example.result".into(),
        revision: "1".into(),
        description: "Structured result with text projection".into(),
        input_schema: json!({"type":"null"}),
        output_schema: json!({}),
        projection_schema: json_projection_schema(json!({"type":"string"})),
    };
    let violating = json!({"kind":"json","value":7});
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(100_000, 4 * 1024 * 1024),
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })?;
    let io = crate::executor::PreparationIo::from_native_work(&work)?;
    for io in [Some(&io), None] {
        let violation = definition
            .check_projection(&violating, Limits::default(), io)?
            .expect_err("a schema violation is the inner error");
        // The native violation keeps its funded diagnostic; its place, not its class, settles it.
        assert_eq!(
            violation.code(),
            crate::wire::ErrorCode::Invalid,
            "{violation:?}"
        );
        assert!(
            definition
                .check_projection(
                    &json!({"kind":"json","value":"seven"}),
                    Limits::default(),
                    io
                )?
                .is_ok()
        );
    }
    // Negative control: the flattened check this replaces turns the same native violation into
    // a native failure, which the executor and durable runner propagate and stop the turn.
    let flattened = definition
        .validate_projection(&violating, Limits::default(), Some(&io))
        .expect_err("the violation still fails validation");
    assert!(flattened.is_native_failure(), "{flattened:?}");
    Ok(())
}
