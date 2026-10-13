//! Portable text effects use owner grants, pinned generations and retained receipts.
#![cfg(feature = "filesystem")]

use acyclic_fs::Fs;
use acyclic_harness::{
    AgentId, Capabilities, Error, IdempotencyKey, OperationId, Result,
    conversation::{
        ContentGrant, ContentPublisher, FileRef, VolumeClass, VolumeOperation, VolumeOwner,
        VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer},
    filesystem::{ExactTextReplacement, FilesystemHost, workspace_ref},
    resources::ProviderRef,
};
use acyclic_stream::BoxProviderFuture;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

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

async fn content_context(
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

struct OrdinaryPublisher {
    volume: VolumeRef,
    calls: AtomicUsize,
}

#[tokio::test]
async fn single_file_publication_never_falls_back_to_ordinary_staging() -> Result<()> {
    let ordinary = OrdinaryPublisher {
        volume: VolumeRef::new(
            ProviderRef::new("once".into(), "filesystem".into(), "2".into(), &mut None)?,
            "workspace".into(),
            VolumeClass::Project,
            VolumeOwner::Agent(AgentId::new()),
            &mut None,
        )?,
        calls: AtomicUsize::new(0),
    };
    assert!(matches!(
        ordinary.stage_once(
            OperationId::new(),
            "one.txt",
            b"one",
            "text/plain",
            "one.txt",
            None
        ),
        Err(Error::Unsupported(_))
    ));
    assert_eq!(ordinary.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

impl ContentPublisher for OrdinaryPublisher {
    fn volume(&self) -> &VolumeRef {
        &self.volume
    }

    fn stage<'a>(
        &'a self,
        _: OperationId,
        _: &'a str,
        _: &'a [u8],
        _: &'a str,
        _: &'a str,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<
        acyclic_harness::conversation::BoundedContentFuture<
            'a,
            (
                FileRef,
                Option<acyclic_harness::conversation::ContentHold<'a>>,
            ),
        >,
    > {
        acyclic_harness::conversation::BoundedContentFuture::new(io, || async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(Error::Storage(
                "ordinary publication must not be called".into(),
            ))
        })
    }
}

#[path = "portable_text_tools_cases/tools.rs"]
mod tools;

#[path = "portable_text_tools_cases/edit.rs"]
mod edit;

#[path = "portable_text_tools_cases/reopen.rs"]
mod reopen;

#[path = "portable_text_tools_cases/upload.rs"]
mod upload;
