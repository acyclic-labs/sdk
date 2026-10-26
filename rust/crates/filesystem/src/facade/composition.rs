//! Volume creations and forks contributed to one caller-built Streams commit.
use super::*;
use crate::distributed::{FilesystemCommit, FilesystemCommitOutcome, StreamAuthorityStore};

/// One workspace fork in a composed commit.
pub struct ForkSpec<'g, A: 'static, O: 'static> {
    /// The new workspace's name; its identity derives from this name.
    pub destination: crate::WorkspaceName,
    /// The exact source generation.
    pub source: &'g crate::Generation<A, O>,
    /// Optional path selection; `None` forks the whole tree.
    pub paths: Option<Vec<String>>,
    /// The fork's operation identity, stable across exact retries.
    pub idempotency_key: crate::IdempotencyKey,
}

/// One fresh workspace in a composed commit.
pub struct CreateSpec<'a> {
    /// The new workspace's name; its identity derives from this name.
    pub name: crate::WorkspaceName,
    /// Immutable semantics and initial policy.
    pub initialization: VolumeInitialization<'a>,
    /// The creation's operation and initial files.
    pub creation: CreationOperation<'a>,
}

impl<P, O> Fs<StreamAuthorityStore<P>, O>
where
    P: crate::distributed::AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Adds every fork to `commit`, all-or-none with the caller's mutations.
    ///
    /// Returns each destination's workspace identity, in order. Nothing is
    /// visible until the commit is submitted and committed.
    pub async fn prepare_fork_many(
        &self,
        forks: &[ForkSpec<'_, StreamAuthorityStore<P>, O>],
        commit: &mut FilesystemCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<Vec<crate::WorkspaceId>>, WorkspaceError> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        // All-or-none, even when this future is dropped mid-way: the guard
        // removes every entry, contribution and hold this call added unless
        // the whole batch succeeded.
        let mut guard = commit.guard();
        let result = async {
            let accounting = &mut accounting;
            let mut created = Vec::with_capacity(forks.len());
            for fork in forks {
                created.push(
                    self.contribute_fork(fork, guard.commit(), accounting, cancellation)
                        .await?,
                );
            }
            Ok(created)
        }
        .await;
        if result.is_ok() {
            guard.disarm();
        }
        measured(result, &accounting)
    }

    async fn contribute_fork(
        &self,
        fork: &ForkSpec<'_, StreamAuthorityStore<P>, O>,
        commit: &mut FilesystemCommit,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::WorkspaceId, WorkspaceError> {
        let source = fork.source;
        if !self.same_deployment(&source.workspace.volume.fs) {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let path_bytes = fork.paths.as_ref().map_or(Ok(0), |paths| {
            crate::workspace::workspace_owned_strings_bytes(paths)
        })?;
        let (path_allocation, admitted) = accounting.record_actual_owned(path_bytes)?;
        let paths = Owned::new(fork.paths.clone(), path_allocation, path_bytes);
        admitted?;
        let destination_id =
            self.workspace_id_for_name(&fork.destination, accounting, cancellation)?;
        let (fork_root, source_root) = self
            .materialize_forked_generation_root(
                source,
                destination_id,
                paths,
                accounting,
                cancellation,
            )
            .await?;
        let prepared = self
            .prepare_fork_commit(
                &super::fork::ForkPublication {
                    source,
                    destination_id,
                    fork_root: &fork_root.value,
                    source_root,
                    idempotency_key: fork.idempotency_key,
                    permit: PublicationPermit::Unrestricted,
                },
                accounting,
                cancellation,
            )
            .await?;
        accounting.check()?;
        let mut authority = authority_work(accounting, cancellation)?;
        let result = self
            .inner
            .authority
            .contribute_fork(commit, &prepared.fork, &mut authority)
            .await;
        let recorded = record_authority_work(accounting, &authority);
        Self::retire_fork_commit(prepared.changes, prepared.payload_bytes, accounting)?;
        recorded?;
        commit.hold(prepared.hold);
        result.map_err(|error| WorkspaceError::from(FsError::from(error)))?;
        fork_root.retire(accounting)?;
        Ok(destination_id)
    }

    /// Adds every fresh workspace to `commit`, all-or-none with the caller's
    /// mutations. Returns each workspace identity, in order.
    pub async fn prepare_create_many(
        &self,
        creations: &[CreateSpec<'_>],
        commit: &mut FilesystemCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<Vec<crate::WorkspaceId>>, WorkspaceError> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        // All-or-none, even when this future is dropped mid-way: the guard
        // removes every entry, contribution and hold this call added unless
        // the whole batch succeeded.
        let mut guard = commit.guard();
        let result = async {
            let accounting = &mut accounting;
            let mut created = Vec::with_capacity(creations.len());
            for creation in creations {
                created.push(
                    self.contribute_creation(creation, guard.commit(), accounting, cancellation)
                        .await?,
                );
            }
            Ok(created)
        }
        .await;
        if result.is_ok() {
            guard.disarm();
        }
        measured(result, &accounting)
    }

    async fn contribute_creation(
        &self,
        spec: &CreateSpec<'_>,
        commit: &mut FilesystemCommit,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::WorkspaceId, WorkspaceError> {
        accounting.check()?;
        let id = self.workspace_id_for_name(&spec.name, accounting, cancellation)?;
        let volume_id = id.volume_id();
        let initialization = spec.initialization;
        let config = initialization.config;
        validate_volume_capabilities(config, self.inner.capabilities)?;
        let proven_at = self.inner.objects.collection_sweeps();
        let generation_root = self
            .initial_generation_root(
                volume_id,
                initialization,
                Some(spec.creation),
                accounting,
                cancellation,
            )
            .await?;
        let proof = self
            .publication_proof(
                generation_root,
                proven_at,
                volume_id,
                config,
                accounting,
                cancellation,
            )
            .await?;
        let prepared = self
            .prepare_creation_commit(
                VolumeCreated {
                    volume_id,
                    config,
                    initial_generation_root: generation_root,
                },
                Some(spec.creation.operation_id),
                &proof.value.objects,
                proven_at,
                accounting,
                cancellation,
            )
            .await;
        proof.retire(accounting)?;
        let prepared = prepared?;
        accounting.check()?;
        let mut authority = authority_work(accounting, cancellation)?;
        let result = self
            .inner
            .authority
            .contribute_first_record(
                commit,
                volume_authority_id(volume_id),
                &prepared.commit,
                &mut authority,
            )
            .await;
        let recorded = record_authority_work(accounting, &authority);
        Self::retire_creation_commit(prepared.changes, prepared.capacity, accounting)?;
        recorded?;
        commit.hold(prepared.hold);
        result.map_err(|error| WorkspaceError::from(FsError::from(error)))?;
        Ok(id)
    }

    /// Fences the active policy cut and freezes a caller-submitted commit.
    /// Call this before sending its request through an external provider.
    /// Repeated sealing preserves the exact request and key.
    pub fn seal_commit(
        &self,
        commit: &mut FilesystemCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = (|| {
            let mut authority = authority_work(&accounting, cancellation)?;
            let result = self.inner.authority.seal(commit, &mut authority);
            record_authority_work(&mut accounting, &authority)?;
            result.map_err(FsError::from)
        })();
        measured(result, &accounting)
    }

    /// Submits a composed commit once. On `Unknown`, resubmit the returned
    /// commit unchanged; its key replays the original receipt.
    pub async fn submit_commit(
        &self,
        commit: FilesystemCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FilesystemCommitOutcome> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let mut authority = authority_work(&accounting, cancellation)?;
            let result = self.inner.authority.submit(commit, &mut authority).await;
            record_authority_work(&mut accounting, &authority)?;
            result.map_err(FsError::from)
        }
        .await;
        measured(result, &accounting)
    }

    /// Settles a composed commit the caller submitted through its own
    /// provider, from the exact outcome of [`FilesystemCommit::request`].
    pub async fn settle_commit(
        &self,
        commit: FilesystemCommit,
        outcome: acyclic_stream::CommitOutcome,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FilesystemCommitOutcome> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        if let Err(error) = commit.ensure_sealed() {
            return measured(Err(FsError::from(error)), &accounting);
        }
        let settled = match outcome {
            // The outcome is decided before any check this call makes: a
            // committed request keeps its receipt even when cancellation or
            // the budget stops the best-effort cache refresh.
            acyclic_stream::CommitOutcome::Committed(envelope) => {
                if let Ok(mut authority) = authority_work(&accounting, cancellation) {
                    self.inner
                        .authority
                        .refresh_committed(commit, envelope.commit_id, &mut authority)
                        .await;
                    drop(record_authority_work(&mut accounting, &authority));
                }
                FilesystemCommitOutcome::Committed(envelope)
            }
            acyclic_stream::CommitOutcome::Conflict(conflicts) => {
                FilesystemCommitOutcome::Rejected(conflicts)
            }
        };
        measured(Ok(settled), &accounting)
    }
}
