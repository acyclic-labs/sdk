//! Generic state-provider boundary checks, not native-media lifecycle qualification.
use acyclic_harness::{
    Capabilities, Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
    conversation::Limits,
    core::{Action, AggregateKind, Authority, AuthorityRoot, Command, Reducer, SchemaRegistry},
    runtime::{Bindings, RuntimeScope, TaskAdmissionRecord, TaskStateProvider},
};
use serde_json::Value;
use std::sync::Arc;

struct ResumedScope(RuntimeScope);

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl TaskStateProvider for ResumedScope {
    fn policy_identity(&self) -> Option<acyclic_harness::registry::ComponentIdentity> {
        None
    }
    async fn observe_admission(&self, _: TaskId) -> Result<TaskAdmissionRecord> {
        Err(Error::Unsupported("scope boundary fixture".into()))
    }
    async fn resume_scope(&self, _: TaskId, _: OperationId) -> Result<RuntimeScope> {
        Ok(self.0.clone())
    }
    async fn outcome(&self, _: TaskId) -> Result<Option<Outcome<Value>>> {
        Err(Error::Unsupported("scope boundary fixture".into()))
    }
    async fn cancel(&self, _: TaskId) -> Result<()> {
        Err(Error::Unsupported("scope boundary fixture".into()))
    }
}

fn selected_scope(id: &str) -> Result<RuntimeScope> {
    let authority = Authority {
        kind: AggregateKind::Agent,
        id: id.into(),
    };
    let issuer = AuthorityRoot::new("extension-resume", [93; 32], authority.clone());
    let mut agent = Reducer::new(
        authority,
        issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(),
    );
    agent.apply(&Command {
        operation_id: OperationId::new(),
        idempotency_key: IdempotencyKey::new(format!("selection:{id}"))?,
        expected_revision: 0,
        scope: issuer
            .bootstrap_scope(
                "owner",
                &Capabilities::new(["extension:activate"]),
                None,
                &mut None,
            )
            .expect("scope proof"),
        causal_parent: None,
        action: Action::SelectExtensions { roots: Vec::new() },
    })?;
    let scope = RuntimeScope::new(Capabilities::default(), Limits::default())?
        .with_extensions_from(&agent)?;
    assert!(scope.extensions().is_some());
    Ok(scope)
}

#[tokio::test]
async fn resumed_context_rejects_omitted_introduced_or_retargeted_extension_selection() -> Result<()>
{
    let original = selected_scope("original")?;
    let retargeted = selected_scope("retargeted")?;
    let empty = RuntimeScope::new(Capabilities::default(), Limits::default())?;
    let task = TaskId::from_bytes([94; 16]);
    let operation = OperationId::from_bytes([95; 16]);
    for (root, resumed, accepts) in [
        (original.clone(), original.clone(), true),
        (empty.clone(), empty.clone(), true),
        (original.clone(), empty.clone(), false),
        (empty, original.clone(), false),
        (original, retargeted, false),
    ] {
        let expected = resumed.extensions().cloned();
        let mut bindings = Bindings::local();
        bindings.scope = root;
        bindings.state = Some(Arc::new(ResumedScope(resumed)));
        let harness = bindings.build(None)?;
        let result = harness.durable_context(task, operation, None).await;
        if accepts {
            assert_eq!(result?.scope().extensions(), expected.as_ref());
        } else {
            assert!(matches!(result, Err(Error::Conflict(_))));
        }
    }
    Ok(())
}
