//! Volume creations and forks contributed to one caller-built Streams commit.
//!
//! Every operation accepts the caller's work and cancellation. Batch preparation
//! admits its ordered output backing before contributing any volume. With native
//! work, the returned allocation belongs to that original ledger and must outlive
//! the returned vector. Preparation errors roll back only this call's additions;
//! an unresolved submitted commit retains the original proofs and holds.
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

/// One private workspace transaction published with a caller's other effects.
pub struct TransactionSpec<'t, A: 'static, O: 'static> {
    /// The staged candidate and its original operation identity. Preparation
    /// retains its proof without changing the candidate or any public head.
    pub transaction: &'t crate::Transaction<A, O>,
    /// Authorization already admitted by the shared filesystem owner.
    pub permit: PublicationPermit,
}

/// One dirty workspace's immutable staged generation in a coupled commit.
pub struct PublishSpec<'g, A: 'static, O: 'static> {
    /// Original generation read by the caller. The complete commit is refused
    /// if any workspace has advanced from its expected generation.
    pub expected: &'g crate::Generation<A, O>,
    /// Staged checkpoint belonging to the same workspace, without publication.
    pub generation: GenerationId,
    /// Stable identity of this workspace's publication in the caller's commit.
    pub operation: OperationId,
    /// Original filesystem publication authority, checked by the shared core.
    pub permit: PublicationPermit,
}

/// Immutable generation contributed by workspace creation or a fork. It becomes
/// readable through its workspace when the containing transaction commits.
#[derive(Clone, Copy, Debug)]
pub struct PreparedGeneration {
    /// Destination workspace identity.
    pub workspace: crate::WorkspaceId,
    /// Exact initial generation published by the containing transaction.
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
    /// Contributes private candidates to the caller's original Streams commit.
    ///
    /// Each candidate's exact base must still be its workspace head, including
    /// at submission. A stale base refuses the entire composition; it is never
    /// silently rebased across effects prepared against the original cut.
    /// Preparation errors remove only this call's additions. Unknown outcomes
    /// retain the same commit, proofs and original native custody for replay.
    pub async fn prepare_transaction_many<W: FsWork>(
        &self,
        transactions: &[TransactionSpec<'_, StreamAuthorityStore<P>, O>],
        commit: &mut FilesystemCommit,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::OwnedOutput<Vec<crate::Generation<StreamAuthorityStore<P>, O>>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let mut guard = commit.guard();
        let result = async {
            let mut generations = Owned::<Vec<_>>::allocate(transactions.len(), &mut accounting)?;
            for spec in transactions {
                let transaction = spec.transaction;
                if transaction.requires_rebase {
                    return Err(WorkspaceError::StaleGeneration);
                }
                let volume = &transaction.workspace.volume;
                if !self.same_deployment(&volume.fs) {
                    return Err(WorkspaceError::ForeignGeneration);
                }
                let (root, head) = volume
                    .resolve_head_generation(&mut accounting, cancellation)
                    .await
                    .map_err(WorkspaceError::engine)?;
                if GenerationId::from_root(root) != transaction.checkout.generation_id() {
                    return Err(WorkspaceError::StaleGeneration);
                }
                let candidate = transaction
                    .checkout
                    .checkpoint_root_with_accounting(&mut accounting, cancellation)
                    .await?;
                self.contribute_generation(
                    PublishGenerationRequest {
                        authority_id: volume_authority_id(volume.id),
                        volume_id: volume.id,
                        epoch: head.epoch,
                        expected: head,
                        operation_id: transaction.idempotency_key.operation_id(),
                        generation_root: candidate,
                    },
                    spec.permit,
                    volume.config,
                    guard.commit(),
                    &mut accounting,
                    cancellation,
                )
                .await?;

                generations.push(
                    crate::Generation {
                        workspace: transaction.workspace.clone(),
                        id: GenerationId::from_root(candidate),
                        publication_closure: None,
                    },
                    &mut accounting,
                )?;
            }
            Ok((generations.value, generations.allocation))
        }
        .await;
        if result.is_ok() {
            guard.disarm();
        }
        work.finish_owned::<_, WorkspaceError>(result, &accounting)
    }

    async fn contribute_generation(
        &self,
        request: PublishGenerationRequest,
        permit: PublicationPermit,
        config: VolumeConfig,
        commit: &mut FilesystemCommit,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let prepared = crate::kernel::prepare_publication(
            self,
            request,
            permit,
            closure_limits(config),
            accounting,
            cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;
        let mut authority = crate::async_storage::AuthorityFrame::borrow(accounting)?;
        let contributed = self
            .inner
            .authority
            .contribute_publication(commit, prepared, &mut authority)
            .await;
        finish_authority_work(authority)?;
        if !contributed.map_err(|error| WorkspaceError::from(FsError::from(error)))? {
            return Err(WorkspaceError::StaleGeneration);
        }
        Ok(())
    }
    /// Contributes every dirty workspace head to the caller's one Streams
    /// transaction, together with git/merge-state CAS contributions. Preparation
    /// publishes nothing; all heads, state and caller records have one hard
    /// commit decision. Cost is O(dirty workspaces + changed state entries).
    ///
    /// Immutable closures are authenticated, flushed and held by the shared
    /// publication core. Dropping this future or returning an error removes only
    /// this call's contributions. After an unknown submission outcome, retain
    /// and resubmit the exact sealed commit, including its original key.
    pub async fn prepare_publish_many<W: FsWork>(
        &self,
        publications: &[PublishSpec<'_, StreamAuthorityStore<P>, O>],
        state_changes: &[crate::GitCompatCas<'_>],
        commit: &mut FilesystemCommit,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let mut guard = commit.guard();
        let result = async {
            accounting.check()?;
            if !state_changes.is_empty() {
                let mut authority = crate::async_storage::AuthorityFrame::borrow(&mut accounting)?;
                let preserved = guard.preserve_conditions(&mut authority);
                finish_authority_work(authority)?;
                preserved.map_err(|error| WorkspaceError::from(FsError::from(error)))?;
            }
            for publication in publications {
                let volume = &publication.expected.workspace.volume;
                if !self.same_deployment(&volume.fs) {
                    return Err(WorkspaceError::ForeignGeneration);
                }
                // The head read buys exact-generation CAS; a concurrent edit
                // after it is fenced by the contributed Streams condition.
                let (root, head) = volume
                    .resolve_head_generation(&mut accounting, cancellation)
                    .await
                    .map_err(WorkspaceError::engine)?;
                if GenerationId::from_root(root) != publication.expected.id() {
                    return Err(WorkspaceError::StaleGeneration);
                }
                self.contribute_generation(
                    PublishGenerationRequest {
                        authority_id: volume_authority_id(volume.id),
                        volume_id: volume.id,
                        epoch: head.epoch,
                        expected: head,
                        operation_id: publication.operation,
                        generation_root: publication.generation.object_id(),
                    },
                    publication.permit,
                    volume.config,
                    guard.commit(),
                    &mut accounting,
                    cancellation,
                )
                .await?;
            }
            let store = crate::StreamGitCompatStore::new(self.inner.authority.clone());
            for change in state_changes {
                let mut authority = crate::async_storage::AuthorityFrame::borrow(&mut accounting)?;
                let contributed = store
                    .contribute_cas(change, guard.commit(), &mut authority)
                    .await;
                finish_authority_work(authority)?;
                if !contributed.map_err(|error| WorkspaceError::GitCompat(Box::new(error)))? {
                    return Err(WorkspaceError::StaleGeneration);
                }
            }
            Ok(())
        }
        .await;
        if result.is_ok() {
            guard.disarm();
        }
        work.finish(result, &accounting)
    }

    /// Prepares authenticated restores with the caller's own records in one
    /// conditional transaction. No volume head changes during preparation.
    /// The commit retains all proofs and collection/projection guards through
    /// an unknown outcome; resubmit that exact commit after transport failure.
    pub async fn prepare_restore_many<W: FsWork>(
        &self,
        restores: &[RestoreSpec<'_, StreamAuthorityStore<P>, O>],
        commit: &mut FilesystemCommit,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::OwnedOutput<Vec<crate::Generation<StreamAuthorityStore<P>, O>>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let mut guard = commit.guard();
        let result = async {
            let mut generations = Owned::<Vec<_>>::allocate(restores.len(), &mut accounting)?;
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
                        root.generation_root
                    });
                self.contribute_generation(
                    PublishGenerationRequest {
                        authority_id: volume_authority_id(volume.id),
                        volume_id: volume.id,
                        epoch: head.epoch,
                        expected: head,
                        operation_id: restore.operation,
                        generation_root,
                    },
                    restore.permit,
                    volume.config,
                    guard.commit(),
                    &mut accounting,
                    cancellation,
                )
                .await?;

                if let Some((root, _, _, _)) = copied {
                    drop(root);
                }
                generations.push(
                    crate::Generation {
                        workspace: restore.expected.workspace.clone(),
                        id: GenerationId::from_root(generation_root),
                        publication_closure: None,
                    },
                    &mut accounting,
                )?;
            }
            Ok((generations.value, generations.allocation))
        }
        .await;
        if result.is_ok() {
            guard.disarm();
        }
        work.finish_owned::<_, WorkspaceError>(result, &accounting)
    }

    /// Adds every fork to `commit`, all-or-none with the caller's mutations.
    ///
    /// Returns each destination's workspace and generation, in order. Nothing is
    /// visible until the commit is submitted and committed.
    pub async fn prepare_fork_many<W: FsWork>(
        &self,
        forks: &[ForkSpec<'_, StreamAuthorityStore<P>, O>],
        commit: &mut FilesystemCommit,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::OwnedOutput<Vec<PreparedGeneration>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        // All-or-none, even when this future is dropped mid-way: the guard
        // removes every entry, contribution and hold this call added unless
        // the whole batch succeeded.
        let mut guard = commit.guard();
        let result = async {
            let accounting = &mut accounting;
            let mut created = Owned::<Vec<_>>::allocate(forks.len(), accounting)?;
            for fork in forks {
                let value = self
                    .contribute_fork(fork, guard.commit(), accounting, cancellation)
                    .await?;
                created.push(value, accounting)?;
            }
            Ok((created.value, created.allocation))
        }
        .await;
        if result.is_ok() {
            guard.disarm();
        }
        work.finish_owned::<_, WorkspaceError>(result, &accounting)
    }

    async fn contribute_fork(
        &self,
        fork: &ForkSpec<'_, StreamAuthorityStore<P>, O>,
        commit: &mut FilesystemCommit,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<PreparedGeneration, WorkspaceError> {
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
        let destination_id = self.workspace_id_for_name(&fork.destination, accounting)?;
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
                    fork_root: &fork_root,
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
        let mut authority = crate::async_storage::AuthorityFrame::borrow(accounting)?;
        let result = self
            .inner
            .authority
            .contribute_fork(commit, &prepared.fork, &mut authority)
            .await;
        let recorded = finish_authority_work(authority);
        if let Some(changes) = prepared.changes {
            changes.retire(accounting)?;
        }
        recorded?;
        commit.hold(prepared.hold.with_proof(pending));
        result.map_err(|error| WorkspaceError::from(FsError::from(error)))?;
        let generation = GenerationId::from_root(fork_root.generation_root);
        drop(fork_root);
        Ok(PreparedGeneration {
            workspace: destination_id,
            generation,
        })
    }

    /// Adds every fresh workspace to `commit`, all-or-none with the caller's
    /// mutations. Returns each workspace and its exact initial generation in order.
    pub async fn prepare_create_many<W: FsWork>(
        &self,
        creations: &[CreateSpec<'_>],
        commit: &mut FilesystemCommit,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::OwnedOutput<Vec<PreparedGeneration>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        // All-or-none, even when this future is dropped mid-way: the guard
        // removes every entry, contribution and hold this call added unless
        // the whole batch succeeded.
        let mut guard = commit.guard();
        let result = async {
            let accounting = &mut accounting;
            let mut created = Owned::<Vec<_>>::allocate(creations.len(), accounting)?;
            for creation in creations {
                let value = self
                    .contribute_creation(creation, guard.commit(), accounting, cancellation)
                    .await?;
                created.push(value, accounting)?;
            }
            Ok((created.value, created.allocation))
        }
        .await;
        if result.is_ok() {
            guard.disarm();
        }
        work.finish_owned::<_, WorkspaceError>(result, &accounting)
    }

    async fn contribute_creation(
        &self,
        spec: &CreateSpec<'_>,
        commit: &mut FilesystemCommit,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<PreparedGeneration, WorkspaceError> {
        accounting.check()?;
        let id = self.workspace_id_for_name(&spec.name, accounting)?;
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
            .publication_proof(generation_root, proven_at, volume_id, config, accounting)
            .await?;
        let prepared = self
            .prepare_creation_commit(
                VolumeCreated {
                    volume_id,
                    config,
                    initial_generation_root: generation_root,
                },
                Some(spec.creation.operation_id),
                &proof.objects,
                proven_at,
                accounting,
                cancellation,
            )
            .await;
        drop(proof);
        let prepared = prepared?;
        accounting.check()?;
        let mut authority = crate::async_storage::AuthorityFrame::borrow(accounting)?;
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
        let recorded = finish_authority_work(authority);
        if let Some(changes) = prepared.changes {
            changes.retire(accounting)?;
        }
        recorded?;
        commit.hold(prepared.hold);
        result.map_err(|error| WorkspaceError::from(FsError::from(error)))?;
        Ok(PreparedGeneration {
            workspace: id,
            generation: GenerationId::from_root(generation_root),
        })
    }

    /// Conditions `commit` on `source` still being its workspace's head when
    /// the commit applies. Composing this for every source a commit forks
    /// makes the sources one consistent cut: if any advanced after it was
    /// read, the whole commit is rejected and nothing is published.
    pub async fn require_head<W: FsWork>(
        &self,
        source: &crate::Generation<StreamAuthorityStore<P>, O>,
        commit: &mut FilesystemCommit,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.require_heads(&[source], commit, work, cancellation)
            .await
    }

    /// Conditions the same caller commit on every original source head, using
    /// one supplied accounting frame. Native preparation retains the original
    /// invocation and allocation owner; standalone callers spend one budget.
    /// This neither submits nor seals the commit. On refusal its caller still
    /// owns the same partially conditioned commit and original retry identity.
    pub async fn require_heads<W: FsWork>(
        &self,
        sources: &[&crate::Generation<StreamAuthorityStore<P>, O>],
        commit: &mut FilesystemCommit,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            accounting.check()?;
            for source in sources {
                if !self.same_deployment(&source.workspace.volume.fs) {
                    return Err(WorkspaceError::ForeignGeneration);
                }
                let mut authority = crate::async_storage::AuthorityFrame::borrow(&mut accounting)?;
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
                finish_authority_work(authority)?;
                result.map_err(|error| WorkspaceError::from(FsError::from(error)))?;
            }
            Ok(())
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Fences the active policy cut and freezes a caller-submitted commit.
    /// Call this before sending its request through an external provider.
    /// Repeated sealing preserves the exact request and key.
    pub fn seal_commit<W: FsWork>(
        &self,
        commit: &mut FilesystemCommit,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), FsError> {
        let mut accounting = work.accounting(cancellation);
        let result = (|| {
            let mut authority = crate::async_storage::AuthorityFrame::borrow(&mut accounting)?;
            let result = self.inner.authority.seal(commit, &mut authority);
            finish_authority_work(authority)?;
            result.map_err(FsError::from)
        })();
        work.finish(result, &accounting)
    }

    /// Submits a composed commit once. On `Unknown`, resubmit the returned
    /// commit unchanged; its key replays the original receipt.
    pub async fn submit_commit<W: FsWork>(
        &self,
        commit: FilesystemCommit,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<FilesystemCommitOutcome, FsError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let mut authority = match crate::async_storage::AuthorityFrame::borrow(&mut accounting)
            {
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
            drop(finish_authority_work(authority));
            result.map_err(FsError::from)
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Settles a composed commit the caller submitted through its own
    /// provider, from the exact outcome of [`FilesystemCommit::request`].
    pub async fn settle_commit<W: FsWork>(
        &self,
        commit: FilesystemCommit,
        outcome: acyclic_stream::CommitOutcome,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<FilesystemCommitOutcome, FsError> {
        let accounting = work.accounting(cancellation);
        let result = self
            .inner
            .authority
            .settle_recorded(commit, outcome, |commit, commit_id| {
                self.inner.authority.refresh_committed(commit, commit_id);
            });
        work.finish(result.map_err(FsError::from), &accounting)
    }
}
