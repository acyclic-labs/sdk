//! Generic state-provider boundary checks, not native-media lifecycle qualification.
use acyclic_harness::{
    Capabilities, Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
    conversation::Limits,
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, Reducer, SchemaRegistry},
    runtime::{Bindings, RuntimeScope, TaskAdmissionRecord, TaskStateProvider},
};
use acyclic_stream::BoxProviderFuture;
use serde_json::Value;
use std::sync::Arc;

// The generic provider deliberately returns a different admission selection.
// This is fault injection at the state seam, not a fabricated native adapter.
struct ResumedScope(RuntimeScope);

impl TaskStateProvider for ResumedScope {
    fn policy_identity(&self) -> Option<acyclic_harness::registry::ComponentIdentity> {
        None
    }
    fn observe_admission<'a>(
        &'a self,
        _: TaskId,
    ) -> BoxProviderFuture<'a, Result<TaskAdmissionRecord>> {
        Box::pin(async { Err(Error::Unsupported("scope boundary fixture".into())) })
    }
    fn resume_scope<'a>(
        &'a self,
        _: TaskId,
        _: OperationId,
    ) -> BoxProviderFuture<'a, Result<RuntimeScope>> {
        Box::pin(async move { Ok(self.0.clone()) })
    }
    fn outcome<'a>(&'a self, _: TaskId) -> BoxProviderFuture<'a, Result<Option<Outcome<Value>>>> {
        Box::pin(async { Err(Error::Unsupported("scope boundary fixture".into())) })
    }
    fn cancel<'a>(&'a self, _: TaskId) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async { Err(Error::Unsupported("scope boundary fixture".into())) })
    }
}

fn selected_scope(id: &str) -> Result<RuntimeScope> {
    let authority = Authority {
        kind: AggregateKind::Agent,
        id: id.into(),
    };
    let issuer = AuthorityIssuer::new("extension-resume", [93; 32], authority.clone());
    let mut agent = Reducer::new(authority, issuer.verifier(), SchemaRegistry::new());
    agent.apply(Command {
        operation_id: OperationId::new(),
        idempotency_key: IdempotencyKey::new(format!("selection:{id}"))?,
        expected_revision: 0,
        scope: issuer.root("owner", Capabilities::new(["extension:activate"])),
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
        let harness = bindings.build()?;
        let result = harness.durable_context(task, operation).await;
        if accepts {
            assert_eq!(result?.scope().extensions(), expected.as_ref());
        } else {
            assert!(matches!(result, Err(Error::Conflict(_))));
        }
    }
    Ok(())
}
