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
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[path = "support/content_context.rs"]
mod context;
use context::content_context;

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
            None,
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
        _ceiling: Option<&'a acyclic_harness::Capabilities>,
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
