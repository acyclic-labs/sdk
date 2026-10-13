//! Provider-owned fork preparation journal and exact Filesystem captures.

use super::{FilesystemHost, ParentProjectController, WorkspaceMutation, map_error, workspace_ref};
use crate::contract::capability;
use crate::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{ContentGrant, ContentResidencyVerifier, VolumeOperation, VolumeRef},
    core::{Authority, AuthorityVerifier, Reducer, Scope},
    fork::{
        Capture, CapturedResource, ForkCaptureProvider, ForkPreparer, ForkReport, ForkRequest,
        ForkSeed, ForkSelection, InheritedConversationPrefix, PreparedSummaryFork, ReferenceGrant,
        ResourceRevision, SharedGrant,
    },
    resources::{ProviderRef, WorkspaceRef},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{collections::BTreeSet, sync::Arc};
use uuid::Uuid;

const MAX_REQUEST_BYTES: u64 = 4 * 1024 * 1024;
const MAX_REPORT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_CAPTURE_BYTES: u64 = 4 * 1024 * 1024;

/// Prepares idempotent captures of parent history and filesystem resources.
pub struct FilesystemForkPreparer<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    parent: Reducer,
    verifier: AuthorityVerifier,
    scope: Scope,
    project: VolumeRef,
    stream_provider: ProviderRef,
    resolver: Arc<dyn ContentResidencyVerifier>,
    capture_providers: Vec<Arc<dyn ForkCaptureProvider>>,
    summary: Option<BoundSummary>,
}

struct BoundSummary {
    prepared: PreparedSummaryFork,
    journal: Arc<dyn crate::executor::ExecutionJournal>,
}

impl<A, O> FilesystemForkPreparer<A, O> {
    /// Binds a persisted parent revision, authenticated parent authority, and
    #[allow(
        clippy::too_many_arguments,
        reason = "every provider and authority is bound explicitly"
    )]
    pub fn new(
        host: Arc<FilesystemHost<A, O>>,
        parent: Reducer,
        verifier: AuthorityVerifier,
        scope: Scope,
        project: VolumeRef,
        stream_provider: ProviderRef,
        resolver: Arc<dyn ContentResidencyVerifier>,
    ) -> Result<Self> {
        stream_provider.validate(&mut None)?;
        if stream_provider.family() != "stream" {
            return Err(Error::Invalid(
                "fork preparer requires a Stream history provider".into(),
            ));
        }
        ParentProjectController::new(&host, &parent, &verifier, &scope, &project, None)?;
        Ok(Self {
            host,
            parent,
            verifier,
            scope,
            project,
            stream_provider,
            resolver,
            capture_providers: Vec::new(),
            summary: None,
        })
    }

    /// Binds an already verified original-parent Summary projection. This
    pub fn with_summary_context(
        mut self,
        prepared: PreparedSummaryFork,
        journal: Arc<dyn crate::executor::ExecutionJournal>,
    ) -> Result<Self> {
        if prepared.parent() != self.parent.authority()
            || prepared.parent_revision() != self.parent.revision()
            || journal.canonical_authority() != Some(self.parent.authority())
        {
            return Err(Error::Conflict(
                "summary capture differs from this original parent".into(),
            ));
        }
        self.summary = Some(BoundSummary { prepared, journal });
        Ok(self)
    }

    /// Registers an exact provider-owned resource capture. A provider may be
    pub fn with_capture_provider(mut self, provider: Arc<dyn ForkCaptureProvider>) -> Result<Self> {
        provider.provider().validate(&mut None)?;
        if self
            .capture_providers
            .iter()
            .any(|bound| bound.provider() == provider.provider())
        {
            return Err(Error::Invalid(
                "fork capture provider is registered twice".into(),
            ));
        }
        self.capture_providers.push(provider);
        Ok(self)
    }
}

mod created_access;
pub use created_access::FilesystemResourceCreationProvider;
mod bootstrap;
mod codec;
mod history;
mod hold;
pub use bootstrap::FilesystemRootBootstrapAllocation;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use hold::native_decoded;
pub(crate) use hold::{DecodeReservation, retain_decoded};
mod records;
pub(crate) use records::{PreparationRecord, UploadRecord, preparation_slot_bytes};
pub(crate) mod io;
pub(crate) use io::PreparationIo;
mod allocations;
mod authorization;
mod journal;
mod preparation;
#[cfg(test)]
mod tests;
use allocations::allocation_ref;
pub(crate) use journal::AllocationClaim;
use journal::*;
