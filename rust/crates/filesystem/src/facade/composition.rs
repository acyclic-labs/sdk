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

/// One exact-head generation restoration in a composed commit.
pub struct RestoreSpec<'g, A: 'static, O: 'static> {
    /// The expected current generation. Concurrent publication refuses the
    /// complete transaction, including every caller mutation.
    pub expected: &'g crate::Generation<A, O>,
    /// Retained source state. A source from another workspace shares its file
    /// table under a generation root belonging to the destination workspace.
    pub source: &'g crate::Generation<A, O>,
    /// Stable operation identity for this restoration.
    pub operation: OperationId,
    /// Authorization already admitted by the shared filesystem owner.
    pub permit: PublicationPermit,
}

/// Immutable generation created by a prepared workspace fork. It becomes
/// readable through its workspace when the containing transaction commits.
#[derive(Clone, Copy, Debug)]
pub struct ForkedGeneration {
    /// Destination workspace identity.
    pub workspace: crate::WorkspaceId,
    /// Destination generation, sharing its source's immutable file table.
    pub generation: GenerationId,
}

impl<P: ?Sized, O> Fs<StreamAuthorityStore<P>, O> {
    /// The Stream provider holding this filesystem's authority, so callers can
    /// compose their own records into the same consistency domain.
    pub fn stream_provider(&self) -> std::sync::Arc<P> {
        self.inner.authority.provider()
    }
}

impl<P, O> Fs<StreamAuthorityStore<P>, O>
where
    P: crate::distributed::AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Prepares authenticated restores with the caller's own records in one
    /// conditional transaction. No volume head changes during preparation.
    /// The commit retains all proofs and collection/projection guards through
    /// an unknown outcome; resubmit that exact commit after transport failure.
    pub async fn prepare_restore_many(
        &self,
        restores: &[RestoreSpec<'_, StreamAuthorityStore<P>, O>],
        commit: &mut FilesystemCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<
        OperationReceipt<Vec<crate::Generation<StreamAuthorityStore<P>, O>>>,
        WorkspaceError,
    > {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let mut guard = commit.guard();
        let result = async {
            let mut generations = Vec::with_capacity(restores.len());
            for restore in restores {
                let volume = &restore.expected.workspace.volume;
                if !self.same_deployment(&volume.fs)
                    || !self.same_deployment(&restore.source.workspace.volume.fs)
                {
                    return Err(WorkspaceError::ForeignGeneration);
                }
                let (root, head) = volume
                    .resolve_head_generation(&mut accounting, cancellation)
                    .await
                    .map_err(WorkspaceError::engine)?;
                if GenerationId::from_root(root) != restore.expected.id() {
                    return Err(WorkspaceError::StaleGeneration);
                }
                let copied = if restore.source.workspace_id() == restore.expected.workspace_id() {
                    None
                } else {
                    let paths = Owned::new(None, accounting.claim_owned(0, 0)?, 0);
                    Some(
                        self.materialize_forked_generation_root(
                            restore.source,
                            restore.expected.workspace_id(),
                            paths,
                            &mut accounting,
                            cancellation,
                        )
                        .await?,
                    )
                };
                let generation_root = copied
                    .as_ref()
                    .map_or(restore.source.id().object_id(), |(root, _, _, _)| {
                        root.value.generation_root
                    });
                let prepared = crate::kernel::prepare_publication(
                    self,
                    PublishGenerationRequest {
                        authority_id: volume_authority_id(volume.id),
                        volume_id: volume.id,
                        epoch: head.epoch,
                        expected: head,
                        operation_id: restore.operation,
                        generation_root,
                    },
                    restore.permit,
                    closure_limits(volume.config),
                    &mut accounting,
                    cancellation,
                )
                .await
                .map_err(WorkspaceError::engine)?;
                let mut authority = authority_work(&mut accounting, cancellation)?;
                let contributed = self
                    .inner
                    .authority
                    .contribute_publication(guard.commit(), prepared, &mut authority)
                    .await;
                record_authority_work(&mut accounting, &authority)?;
                if !contributed.map_err(|error| WorkspaceError::from(FsError::from(error)))? {
                    return Err(WorkspaceError::StaleGeneration);
                }
                if let Some((root, _, _, _)) = copied {
                    root.retire(&mut accounting)?;
                }
                generations.push(crate::Generation {
                    workspace: restore.expected.workspace.clone(),
                    id: GenerationId::from_root(generation_root),
                    publication_closure: None,
                });
            }
            Ok(generations)
        }
        .await;
        if result.is_ok() {
            guard.disarm();
        }
        measured(result, &accounting)
    }

    /// Adds every fork to `commit`, all-or-none with the caller's mutations.
    ///
    /// Returns each destination's workspace and generation, in order. Nothing is
    /// visible until the commit is submitted and committed.
    pub async fn prepare_fork_many(
        &self,
        forks: &[ForkSpec<'_, StreamAuthorityStore<P>, O>],
        commit: &mut FilesystemCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<Vec<ForkedGeneration>>, WorkspaceError> {
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
    ) -> Result<ForkedGeneration, WorkspaceError> {
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
        let (fork_root, source_root, pending, filtered) = self
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
                    filtered,
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
        commit.hold(prepared.hold.with_proof(pending));
        result.map_err(|error| WorkspaceError::from(FsError::from(error)))?;
        let generation = GenerationId::from_root(fork_root.value.generation_root);
        fork_root.retire(accounting)?;
        Ok(ForkedGeneration {
            workspace: destination_id,
            generation,
        })
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

    /// Conditions `commit` on `source` still being its workspace's head when
    /// the commit applies. Composing this for every source a commit forks
    /// makes the sources one consistent cut: if any advanced after it was
    /// read, the whole commit is rejected and nothing is published.
    pub async fn require_head(
        &self,
        source: &crate::Generation<StreamAuthorityStore<P>, O>,
        commit: &mut FilesystemCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<()>, WorkspaceError> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            if !self.same_deployment(&source.workspace.volume.fs) {
                return Err(WorkspaceError::ForeignGeneration);
            }
            let mut authority = authority_work(&accounting, cancellation)?;
            let result = self
                .inner
                .authority
                .contribute_head(
                    commit,
                    crate::kernel::volume_authority_id(source.workspace.volume.id),
                    source.id,
                    &mut authority,
                )
                .await;
            record_authority_work(&mut accounting, &authority)?;
            result.map_err(|error| WorkspaceError::from(FsError::from(error)))
        }
        .await;
        measured(result, &accounting)
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
            let mut authority = match authority_work(&accounting, cancellation) {
                Ok(authority) => authority,
                Err(error) => {
                    return Ok(FilesystemCommitOutcome::Unknown {
                        commit,
                        error: crate::async_storage::authority_error(error),
                    });
                }
            };
            let result = self.inner.authority.submit(commit, &mut authority).await;
            // Completion accounting cannot discard an unresolved owner or
            // replace a decided result after the provider has returned it.
            drop(record_authority_work(&mut accounting, &authority));
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
        let accounting = KernelAccounting::standalone(budget, cancellation);
        let result = self
            .inner
            .authority
            .settle_recorded(commit, outcome, |commit, commit_id| {
                self.inner.authority.refresh_committed(commit, commit_id);
            });
        measured(result.map_err(FsError::from), &accounting)
    }
}
