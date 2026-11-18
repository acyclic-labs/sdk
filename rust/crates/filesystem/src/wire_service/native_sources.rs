//! Native source roots retain their watcher; every call supplies its own SDK workspace.
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tokio::sync::{Mutex, MutexGuard};
use tonic::Status;
use {
    crate::{
        AsyncAuthorityStore, AsyncObjectStore, CancellationToken, FilesystemSourceProvider,
        HostedSourceInvalidation, HostedSourceResult, HostedSourceState, IdempotencyKey,
        ReconcileOutcome, Source, SourceError, SourceOptions, SourceState, WatchInvalidationReason,
        WorkBudget, Workspace,
    },
    acyclic_fs_format::object_reference::GenerationId,
};

struct Directory<A: 'static, O: 'static> {
    path: PathBuf,
    attachment: Mutex<Option<Source<A, O>>>,
}

enum Attachment<'a, A: 'static, O: 'static> {
    Native(MutexGuard<'a, Option<Source<A, O>>>),
    Completed(ReconcileOutcome<A, O>),
}

/// Configured native roots over the canonical filesystem engine.
/// Roots are lazy: only a source mutation starts a watcher and authenticates a baseline.
pub struct FilesystemNativeSources<A: 'static, O: 'static> {
    directories: BTreeMap<String, Directory<A, O>>,
}

impl<A: 'static, O: 'static> Default for FilesystemNativeSources<A, O> {
    fn default() -> Self {
        Self {
            directories: BTreeMap::new(),
        }
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemNativeSources<A, O> {
    /// Validate host configuration without touching the directories or storage.
    pub fn new(configured: BTreeMap<String, PathBuf>) -> Result<Self, Status> {
        let mut directories = BTreeMap::new();
        for (workspace, path) in configured {
            if !path.is_absolute() {
                return Err(Status::invalid_argument(
                    "filesystem source directory must be absolute",
                ));
            }
            directories.insert(
                workspace,
                Directory {
                    path,
                    attachment: Mutex::new(None),
                },
            );
        }
        Ok(Self { directories })
    }

    fn directory(&self, workspace: &Workspace<A, O>) -> Result<&Directory<A, O>, Status> {
        self.directories
            .get(workspace.name().as_str())
            .ok_or_else(|| Status::failed_precondition("filesystem source is not configured"))
    }

    async fn attach<'a>(
        &'a self,
        workspace: &Workspace<A, O>,
        key: IdempotencyKey,
        operation: crate::source::SourceOperation,
    ) -> Result<Attachment<'a, A, O>, Status> {
        let directory = self.directory(workspace)?;
        let mut attached = directory.attachment.lock().await;
        if attached.is_none() {
            if let Some(outcome) =
                crate::source::completed_source_operation(workspace, key, operation)
                    .await
                    .map_err(status)?
            {
                return Ok(Attachment::Completed(outcome));
            }
            let native = workspace
                .open_directory(&directory.path, SourceOptions::default())
                .await
                .map_err(status)?;
            let source = native
                .source()
                .cloned()
                .ok_or_else(|| Status::internal("native source attachment missing"))?;
            *attached = Some(source);
        }
        Ok(Attachment::Native(attached))
    }
}

#[tonic::async_trait]
impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemSourceProvider<A, O>
    for FilesystemNativeSources<A, O>
{
    async fn state(&self, workspace: &Workspace<A, O>) -> Result<HostedSourceResult, Status> {
        let attached = self.directory(workspace)?.attachment.lock().await;
        let Some(attached) = attached.as_ref() else {
            return Ok(HostedSourceResult {
                state: HostedSourceState::NeedsRescan(
                    HostedSourceInvalidation::InitialSnapshotRequired,
                ),
                generation_id: None,
            });
        };
        let bound = attached
            .bind_workspace(workspace.clone())
            .await
            .map_err(status)?;
        let generation = bound.generation_id();
        let mut state = state(bound.state());
        if state == HostedSourceState::Clean && head(workspace).await? != generation {
            state = HostedSourceState::Conflict;
        }
        Ok(HostedSourceResult {
            state,
            generation_id: matches!(state, HostedSourceState::Clean | HostedSourceState::Sealed)
                .then_some(generation),
        })
    }

    async fn reconcile(
        &self,
        workspace: &Workspace<A, O>,
        key: IdempotencyKey,
    ) -> Result<HostedSourceResult, Status> {
        let attached = match self
            .attach(workspace, key, crate::source::SourceOperation::Reconcile)
            .await?
        {
            Attachment::Completed(outcome) => return Ok(outcome_result(outcome)),
            Attachment::Native(attached) => attached,
        };
        let attached = attached
            .as_ref()
            .ok_or_else(|| Status::internal("native source attachment missing"))?;
        let outcome = attached
            .bind_workspace(workspace.clone())
            .await
            .map_err(status)?
            .reconcile_with_key(key)
            .await
            .map_err(status)?;
        Ok(outcome_result(outcome))
    }

    async fn rescan(
        &self,
        workspace: &Workspace<A, O>,
        key: IdempotencyKey,
    ) -> Result<HostedSourceResult, Status> {
        let attached = match self
            .attach(workspace, key, crate::source::SourceOperation::Rescan)
            .await?
        {
            Attachment::Completed(outcome) => return Ok(outcome_result(outcome)),
            Attachment::Native(attached) => attached,
        };
        let attached = attached
            .as_ref()
            .ok_or_else(|| Status::internal("native source attachment missing"))?;
        let outcome = attached
            .bind_workspace(workspace.clone())
            .await
            .map_err(status)?
            .rescan_with_key(key)
            .await
            .map_err(status)?;
        Ok(outcome_result(outcome))
    }

    async fn seal(
        &self,
        workspace: &Workspace<A, O>,
        key: IdempotencyKey,
    ) -> Result<HostedSourceResult, Status> {
        let attached = match self
            .attach(workspace, key, crate::source::SourceOperation::Seal)
            .await?
        {
            Attachment::Completed(ReconcileOutcome::Clean(generation)) => {
                return Ok(HostedSourceResult {
                    state: HostedSourceState::Sealed,
                    generation_id: Some(generation.id()),
                });
            }
            Attachment::Completed(_) => {
                return Err(Status::internal("native seal receipt invalid"));
            }
            Attachment::Native(attached) => attached,
        };
        let attached = attached
            .as_ref()
            .ok_or_else(|| Status::internal("native source attachment missing"))?;
        let generation = attached
            .bind_workspace(workspace.clone())
            .await
            .map_err(status)?
            .seal_with_key(key)
            .await
            .map_err(status)?;
        Ok(HostedSourceResult {
            state: HostedSourceState::Sealed,
            generation_id: Some(generation.id()),
        })
    }
}

fn outcome_result<A: 'static, O: 'static>(outcome: ReconcileOutcome<A, O>) -> HostedSourceResult {
    let (state, generation_id) = match outcome {
        ReconcileOutcome::Clean(generation) => (HostedSourceState::Clean, Some(generation.id())),
        ReconcileOutcome::NeedsRescan(reason) => {
            (HostedSourceState::NeedsRescan(invalidation(reason)), None)
        }
        ReconcileOutcome::Conflict => (HostedSourceState::Conflict, None),
    };
    HostedSourceResult {
        state,
        generation_id,
    }
}

fn state(state: SourceState) -> HostedSourceState {
    match state {
        SourceState::Clean => HostedSourceState::Clean,
        SourceState::PendingCapture => HostedSourceState::PendingCapture,
        SourceState::NeedsRescan(reason) => HostedSourceState::NeedsRescan(invalidation(reason)),
        SourceState::Conflict => HostedSourceState::Conflict,
        SourceState::Sealed => HostedSourceState::Sealed,
    }
}

fn invalidation(reason: WatchInvalidationReason) -> HostedSourceInvalidation {
    match reason {
        WatchInvalidationReason::InitialSnapshotRequired => {
            HostedSourceInvalidation::InitialSnapshotRequired
        }
        WatchInvalidationReason::QueueOverflow => HostedSourceInvalidation::QueueOverflow,
        WatchInvalidationReason::NativeRescanRequired => {
            HostedSourceInvalidation::NativeRescanRequired
        }
        WatchInvalidationReason::BackendError => HostedSourceInvalidation::BackendError,
        WatchInvalidationReason::UnrepresentablePath => {
            HostedSourceInvalidation::UnrepresentablePath
        }
        WatchInvalidationReason::AmbiguousRename => HostedSourceInvalidation::AmbiguousRename,
        WatchInvalidationReason::RootChanged => HostedSourceInvalidation::RootChanged,
    }
}

async fn head<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
) -> Result<GenerationId, Status> {
    workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value.id())
        .map_err(|failure| {
            let mut status = Status::from(&failure.error);
            status.set_source(Arc::new(failure.error));
            status
        })
}

fn status(cause: SourceError) -> Status {
    let status = match &cause {
        SourceError::Cancelled => Status::cancelled("filesystem source cancelled"),
        SourceError::InvalidOptions | SourceError::BindingMismatch => {
            Status::invalid_argument("filesystem source binding invalid")
        }
        SourceError::Pinned
        | SourceError::Concurrent
        | SourceError::Conflict
        | SourceError::IdempotencyConflict => {
            Status::failed_precondition("filesystem source state changed")
        }
        SourceError::Engine(_) => Status::unavailable("filesystem source operation unavailable"),
    };
    {
        let mut status = status;
        status.set_source(Arc::new(cause));
        status
    }
}
