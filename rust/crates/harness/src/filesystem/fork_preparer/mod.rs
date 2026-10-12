//! Provider-owned fork preparation journal and exact Filesystem captures.

use super::{FilesystemHost, ParentProjectController, WorkspaceMutation, map_error};
use crate::contract::capability;
use crate::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{ContentGrant, ContentResidencyVerifier, VolumeOperation, VolumeRef},
    core::{Authority, AuthorityVerifier, Reducer, Scope},
    fork::{
        Capture, CapturedResource, ForkCaptureProvider, ForkPreparer, ForkReport, ForkRequest,
        ForkSeed, ForkSelection, InheritedConversationPrefix, PreparedForkHistory,
        PreparedSummaryFork, ReferenceGrant, ResourceRevision, SharedGrant,
    },
    resource_preparation::{ResourceCreationOrigin, TaskResourcePreparation},
    resources::{ProviderRef, ResourceRef, WorkspaceRef},
    source_authorization::ProjectSourceReadHandle,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::{BoxProviderFuture as BoxFuture, StreamProvider};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, sync::Arc};
use uuid::Uuid;

mod allocations;
mod attempt;
mod authorization;
mod created_access;
mod journal;
mod preparation;
mod references;
pub(crate) use allocations::AllocationClaim;
use allocations::{ForkAllocationRole, creation_volumes, same_task_origin};
pub(crate) use journal::seed_digest;
use journal::*;
use references::add_summary_readers;
pub(super) use references::require_reference_donor;
pub(crate) mod bounded_io;
pub(crate) use bounded_io::PreparationIo;
pub use created_access::{FilesystemResourceCreationProvider, FilesystemRootBootstrapAllocation};

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
    history: Option<PreparedForkHistory>,
    task_preparation: Option<Arc<TaskResourcePreparation>>,
    project_source: Option<ProjectSourceReadHandle>,
    #[cfg(not(target_arch = "wasm32"))]
    native: Option<(
        acyclic_fs::NativeMaterializationWork,
        acyclic_fs::NativeAllocation,
    )>,
}

struct BoundSummary {
    prepared: PreparedSummaryFork,
    journal: Arc<dyn crate::executor::ExecutionJournal>,
}

struct PreparationAttempt<'a, A, O> {
    preparer: &'a FilesystemForkPreparer<A, O>,
    io: Option<PreparationIo>,
    task_preparation: Option<&'a TaskResourcePreparation>,
    #[cfg(not(target_arch = "wasm32"))]
    native_creation_guard: Option<Arc<crate::resource_preparation::TaskResourcePreparation>>,
}

impl<A, O> FilesystemForkPreparer<A, O> {
    /// Binds the authenticated parent, history and optional original native custody.
    #[allow(
        clippy::too_many_arguments,
        reason = "providers and authority are explicit"
    )]
    pub fn new(
        host: Arc<FilesystemHost<A, O>>,
        parent: Reducer,
        verifier: AuthorityVerifier,
        scope: Scope,
        project: VolumeRef,
        stream_provider: ProviderRef,
        resolver: Arc<dyn ContentResidencyVerifier>,
        history: Option<PreparedForkHistory>,
        project_source: Option<ProjectSourceReadHandle>,
        #[cfg(not(target_arch = "wasm32"))] native: Option<(
            acyclic_fs::NativeAllocation,
            &acyclic_fs::NativeMaterializationWork,
        )>,
    ) -> Result<Self> {
        let this = Self {
            host,
            parent,
            verifier,
            scope,
            project,
            stream_provider,
            resolver,
            capture_providers: Vec::new(),
            summary: None,
            history,
            project_source,
            task_preparation: None,
            #[cfg(not(target_arch = "wasm32"))]
            native: native.map(|(custody, work)| (work.clone(), custody)),
        };
        #[cfg(not(target_arch = "wasm32"))]
        let io = this
            .native
            .as_ref()
            .map(|(work, custody)| {
                if !custody.work().shares_allocation_context(work) || this.history.is_none() {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "native fork lacks matching custody or prepared history",
                    });
                }
                PreparationIo::from_native_work(work)
            })
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let io = None;
        let reject = |code, reason| bounded_io::failure(io.as_ref(), code, reason);
        let mut observer = io.as_ref().map(PreparationIo::history_io);
        let mut work = observer
            .as_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        this.stream_provider.validate(&mut work)?;
        if this.stream_provider.family() != "stream" {
            return Err(reject(
                crate::wire::ErrorCode::Invalid,
                "fork requires Stream history",
            ));
        }
        ParentProjectController::new(
            &this.host,
            &this.parent,
            &this.verifier,
            &this.scope,
            &this.project,
            io.as_ref(),
        )?;
        if let Some(history) = &this.history {
            history.verify(&this.verifier, &mut work)?;
            if history.parent() != this.parent.authority()
                || history.parent_revision() != this.parent.revision()
            {
                return Err(reject(
                    crate::wire::ErrorCode::Conflict,
                    "fork history differs from the bound parent",
                ));
            }
        }
        if this
            .project_source
            .as_ref()
            .is_some_and(|handle| handle.authorization().project.provider() != &this.host.provider)
        {
            return Err(reject(
                crate::wire::ErrorCode::Unauthorized,
                "fork source belongs to another provider",
            ));
        }
        Ok(this)
    }

    /// Shares this preparer on its native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(
        mut self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        if !self
            .native
            .as_ref()
            .is_some_and(|(_, guard)| guard.work().shares_allocation_context(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "fork preparer belongs to another original resource",
            });
        }
        let mut header = work
            .claim(
                std::mem::size_of::<Self>() as u64,
                u64::from((std::mem::size_of::<Self>() as u64) != 0),
                acyclic_fs::WorkCounters::default(),
            )
            .map_err(Error::from)?;
        self.native
            .as_mut()
            .expect("native custody was checked")
            .1
            .absorb(&mut header)
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: Some(header),
            })?;
        Ok(Arc::new(self))
    }

    /// Binds an already verified original-parent Summary projection.
    pub fn with_summary_context(
        mut self,
        prepared: PreparedSummaryFork,
        journal: Arc<dyn crate::executor::ExecutionJournal>,
        #[cfg(not(target_arch = "wasm32"))] custody: Option<acyclic_fs::NativeAllocation>,
    ) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        match (self.native.as_mut(), custody) {
            (Some((work, retained)), Some(mut custody))
                if custody.work().shares_allocation_context(work) =>
            {
                retained
                    .absorb(&mut custody)
                    .map_err(|cause| Error::NativeWork {
                        cause,
                        custody: Some(custody),
                    })?;
            }
            (None, None) => {}
            (_, Some(custody)) => {
                return Err(Error::NativeWork {
                    cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                    custody: Some(custody),
                });
            }
            _ => {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "summary inputs lack their original native custody",
                });
            }
        }
        if prepared.parent() != self.parent.authority()
            || prepared.parent_revision() != self.parent.revision()
            || journal.canonical_authority() != Some(self.parent.authority())
        {
            return Err(bounded_io::failure(
                None,
                crate::wire::ErrorCode::Conflict,
                "summary capture differs from this original parent",
            ));
        }
        self.summary = Some(BoundSummary { prepared, journal });
        Ok(self)
    }

    /// Pins an authenticated ordinary preparation task to this provider.
    pub fn with_task_preparation(
        mut self,
        preparation: Arc<TaskResourcePreparation>,
    ) -> Result<Self> {
        if preparation.request().provider != self.host.provider
            || preparation.request().creation_grant != capability::FORK_PUBLISH
        {
            return Err(Error::Unauthorized(
                "fork preparation task belongs to another provider or creation authority".into(),
            ));
        }
        self.task_preparation = Some(preparation);
        Ok(self)
    }

    /// Registers an exact provider-owned resource capture.
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

impl<A, O> ForkPreparer for FilesystemForkPreparer<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn parent_snapshot(&self) -> (&Authority, u64) {
        (self.parent.authority(), self.parent.revision())
    }

    fn prepare<'a>(&'a self, request: ForkRequest) -> BoxFuture<'a, Result<ForkReport>> {
        Box::pin(async move {
            let attempt = self.attempt(self.task_preparation.as_deref())?;
            self.check_request(&request, attempt.io.as_ref(), attempt.task_preparation)?;
            attempt.prepare(request).await
        })
    }

    fn reconcile<'a>(&'a self, request: ForkRequest) -> BoxFuture<'a, Result<Option<ForkReport>>> {
        Box::pin(async move {
            let this = self.attempt(self.task_preparation.as_deref())?;
            self.check_request(&request, this.io.as_ref(), this.task_preparation)?;
            if request.authorized_source.is_some() && this.task_preparation.is_none() {
                return Err(Error::Unauthorized(
                    "authorized workspace reconciliation requires its original preparation lease"
                        .into(),
                ));
            }
            this.verify_task_lease(true).await?;
            this.preparer
                .prepare_bound_summary(&request, this.io.as_ref())?;
            this.reconcile(&request).await
        })
    }
}
