//! Portable text effects use owner grants, pinned generations and retained receipts.

use acyclic_fs::Fs;
use acyclic_harness::{
    AgentId, Capabilities, Error, IdempotencyKey, OperationId, Result,
    conversation::{
        ContentGrant, ContentPublisher, FileRef, VolumeClass, VolumeOperation, VolumeOwner,
        VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityRoot},
    filesystem::{ExactTextReplacement, FilesystemHost, workspace_ref},
    resources::ProviderRef,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::content_context::content_context;

struct OrdinaryPublisher {
    volume: VolumeRef,
    calls: AtomicUsize,
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
        access: acyclic_harness::conversation::ContentAccess<'a, 'a>,
    ) -> Result<
        acyclic_harness::conversation::BoundedContentFuture<
            'a,
            (
                FileRef,
                Option<acyclic_harness::conversation::ContentHold<'a>>,
            ),
        >,
    > {
        let acyclic_harness::conversation::ContentAccess {
            ceiling: _ceiling,
            io,
        } = access;

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
