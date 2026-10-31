//! Volume creations and forks contributed to one caller-built Streams commit.
use super::*;
use crate::distributed::{FilesystemCommit, FilesystemCommitOutcome, StreamAuthorityStore};

/// Preparation adds a proposed generation or returns the original authority
/// refusal. A proposal becomes visible only when the caller's commit succeeds.
#[must_use]
pub enum PublicationContribution {
    /// The authenticated generation contributed to the caller's transaction.
    Prepared {
        /// Proposed authenticated generation.
        generation: GenerationId,
        /// The sole unsent commit, including original publication custody.
        commit: FilesystemCommit,
    },
    /// Nothing was added, including on an already committed operation.
    Rejected {
        /// The original authority preparation refusal.
        outcome: crate::AppendGroupOutcome,
        /// The unchanged caller batch; no publication was submitted.
        commit: FilesystemCommit,
    },
}

pub(crate) struct PreparedCheckoutPublication {
    publication: crate::kernel::PublicationContributionCustody,
    _read_fence: super::proof::PublicationFreeze,
    // Last: borrowed store custody retires before the original lifecycle owner.
    _owner: Arc<dyn acyclic_stream::ProviderPlatform>,
}

impl PreparedCheckoutPublication {
    pub(crate) fn recorded(&mut self) {
        self.publication.recorded();
    }
}

impl<A, O> Checkout<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Consumes an authored checkout into a caller's all-or-none commit.
    ///
    /// Uses the original publication preparation, including closure
    /// authentication, object durability and policy/projection fences. The
    /// original dependency freeze and every proof/collection hold stay in
    /// `commit`, including when submission returns Unknown. Preparation submits
    /// nothing. A failed or dropped preparation drops the entire unsent batch,
    /// so a partial contribution cannot escape through the caller. A semantic
    /// refusal returns the unchanged batch. The consumed checkout needs no
    /// post-publication root install.
    pub async fn contribute_publication<W: FsWork>(
        self,
        operation_id: OperationId,
        mut intent: PublicationIntent,
        mut commit: FilesystemCommit,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<PublicationContribution, FsError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            cancellation.check()?;
            accounting.check()?;
            if self.mode.access != AccessMode::ReadWrite
                || self.mode.mutations != MutationMode::PrivateOverlay
            {
                return Err(FsError::MutationNotAllowed);
            }
            use crate::async_storage::AuthorityComposition as _;
            let composition = self.volume.fs.inner.authority.composition().ok_or(
                crate::AuthorityStoreError::RejectedStatic(
                    "authority backend does not support atomic composition",
                ),
            )?;
            let expected = self
                .authority_head
                .ok_or(FsError::WritableCheckoutRequiresHead)?;
            let (read_fence, preconditions) = self.publication_dependencies()?;
            intent.preconditions = preconditions;
            let root = self
                .checkpoint_root_with_accounting(&mut accounting, cancellation)
                .await?;
            let prepared = crate::kernel::prepare_generation_contribution(
                &self.volume.fs,
                self.publication_request(operation_id, expected, root),
                intent,
                closure_limits(self.volume.config),
                Some(&self.volume.fs.inner.publication_proof.inner),
                &mut accounting,
                cancellation,
            )
            .await?;
            let generation = prepared.generation();
            let mut authority = authority_work(&accounting, cancellation)?;
            let contributed = async {
                commit.reserve_publication(&mut authority)?;
                let contribution = composition
                    .contribute(
                        &mut commit,
                        std::slice::from_ref(prepared.append()),
                        &[],
                        &mut authority,
                    )
                    .await?;
                // The shared confirmation borrows the append. The original
                // consumed generation custody below owns authoritative settlement.
                Ok::<_, crate::AuthorityStoreError>(match contribution {
                    crate::FilesystemAppendPreparation::Ready(_) => None,
                    crate::FilesystemAppendPreparation::Rejected(refused) => Some(refused),
                })
            }
            .await;
            record_authority_work(&mut accounting, &authority)?;
            if let Some(refused) = contributed? {
                return Ok(PublicationContribution::Rejected {
                    outcome: refused,
                    commit,
                });
            }
            commit.hold_publication(PreparedCheckoutPublication {
                publication: prepared,
                _read_fence: read_fence,
                _owner: self.volume.fs.inner.clone(),
            });
            Ok(PublicationContribution::Prepared { generation, commit })
        }
        .await;
        work.finish(result, &accounting)
    }
}

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
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Fs<A, O> {
    /// Whether the backend can compose publications with caller-owned records.
    /// Query before staging files; unsupported backends never publish separately.
    pub fn supports_composition(&self) -> bool {
        self.inner.authority.composition().is_some()
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
            use crate::async_storage::AuthorityComposition as _;
            let composition = self.inner.authority.composition().ok_or(
                crate::AuthorityStoreError::RejectedStatic(
                    "authority backend does not support atomic composition",
                ),
            )?;
            let result = composition.seal(commit, &mut authority);
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
            use crate::async_storage::AuthorityComposition as _;
            let Some(composition) = self.inner.authority.composition() else {
                return Ok(FilesystemCommitOutcome::Unknown {
                    commit,
                    error: crate::AuthorityStoreError::RejectedStatic(
                        "authority backend does not support atomic composition",
                    ),
                });
            };
            let result = composition.submit(commit, &mut authority).await;
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
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        use crate::async_storage::AuthorityComposition as _;
        let Some(composition) = self.inner.authority.composition() else {
            return measured(
                Ok(FilesystemCommitOutcome::Unknown {
                    commit,
                    error: crate::AuthorityStoreError::RejectedStatic(
                        "authority backend does not support atomic composition",
                    ),
                }),
                &accounting,
            );
        };
        let mut authority = authority_work(&accounting, cancellation).ok();
        let result = composition
            .settle(commit, outcome, authority.as_mut())
            .await;
        if let Some(authority) = authority {
            drop(record_authority_work(&mut accounting, &authority));
        }
        measured(result.map_err(FsError::from), &accounting)
    }
}
