use acyclic_harness::{Capabilities, Error, OperationId, Result, conversation::ContentPublisher};
use std::sync::Arc;

struct ResumedScope(acyclic_harness::runtime::RuntimeScope);

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl acyclic_harness::runtime::TaskStateProvider for ResumedScope {
    fn policy_identity(&self) -> Option<acyclic_harness::registry::ComponentIdentity> {
        None
    }
    async fn observe_admission(
        &self,
        _: acyclic_harness::TaskId,
    ) -> Result<acyclic_harness::runtime::TaskAdmissionRecord> {
        Err(Error::Unsupported(
            "fixture tests content authority, not task admission".into(),
        ))
    }
    async fn resume_scope(
        &self,
        _: acyclic_harness::TaskId,
        _: OperationId,
    ) -> Result<acyclic_harness::runtime::RuntimeScope> {
        Ok(self.0.clone())
    }
    async fn outcome(
        &self,
        _: acyclic_harness::TaskId,
    ) -> Result<Option<acyclic_harness::Outcome<serde_json::Value>>> {
        Ok(None)
    }
    async fn cancel(&self, _: acyclic_harness::TaskId) -> Result<()> {
        Ok(())
    }
}

pub async fn content_context(
    reader: Arc<dyn acyclic_harness::conversation::ContentResidencyVerifier>,
    writer: Option<Arc<dyn ContentPublisher>>,
    capabilities: Capabilities,
) -> Result<Arc<acyclic_harness::runtime::TaskContext>> {
    use acyclic_harness::runtime::{Bindings, ContentBindings, RuntimeScope};
    let scope = RuntimeScope::new(
        capabilities,
        acyclic_harness::conversation::Limits::default(),
    )?;
    let mut bindings = Bindings::local();
    bindings.scope = scope.clone();
    bindings.state = Some(Arc::new(ResumedScope(scope)));
    bindings.content = Some(ContentBindings { reader, writer });
    bindings
        .build(None)?
        .durable_context(acyclic_harness::TaskId::new(), OperationId::new(), None)
        .await
}
