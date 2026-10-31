use super::*;

pub(super) struct PreparedForkCommit {
    pub(super) fork: crate::WorkspaceForkCommit,
    pub(super) changes: Option<crate::PublicationChanges>,
    pub(super) hold: crate::PublicationHold,
    pub(super) payload_bytes: u64,
}

pub(super) struct ForkPublication<'a, A: 'static, O: 'static> {
    pub(super) source: &'a crate::Generation<A, O>,
    pub(super) destination_id: crate::WorkspaceId,
    pub(super) fork_root: &'a VerifiedPublication<A, O>,
    pub(super) source_root: ObjectId,
    pub(super) idempotency_key: crate::IdempotencyKey,
    pub(super) permit: PublicationPermit,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(super) async fn materialize_forked_generation_root(
        &self,
        source: &crate::Generation<A, O>,
        destination_id: crate::WorkspaceId,
        mut paths: Owned<Option<Vec<String>>>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<
        (
            Owned<VerifiedPublication<A, O>>,
            ObjectId,
            Option<crate::kernel::closure::PendingProof>,
        ),
        WorkspaceError,
    > {
        let config = source.workspace.volume.config;
        accounting.check()?;
        let proven_at = self.inner.objects.collection_sweeps();
        let source_object = source.id.object_id();
        let (source_root, allocation) = read_generation_root_with_accounting(
            &self.inner.objects,
            source_object,
            config,
            accounting,
            cancellation,
        )
        .await?;
        let source_parent_bytes = root_parent_bytes(&source_root)?;
        let source_root = Owned::new(source_root, allocation, source_parent_bytes);
        if source_root.value.volume_id != source.workspace.volume.id {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let file_table = if let Some(selected) = paths.value.take() {
            let selected = Owned::new(selected, paths.allocation, paths.bytes);
            self.select_fork_paths(
                source,
                &source_root.value,
                selected,
                accounting,
                cancellation,
            )
            .await?
        } else {
            paths.retire(accounting)?;
            source_root.value.file_table
        };
        let mut parents = Owned::<Vec<GenerationId>>::allocate(1, accounting)?;
        fork_selection::selection_push(&mut parents, source.id, 1, accounting)?;
        let required_features = source_root.value.continuation_features()
            | if file_table == source_root.value.file_table {
                0
            } else {
                GenerationRoot::FILTERED_FORK
            };
        let fork_root = GenerationRoot {
            volume_id: destination_id.volume_id(),
            root_file_id: source_root.value.root_file_id,
            file_table,
            parents: parents.value,
            required_features,
        };
        let fork_root = Owned::new(fork_root, parents.allocation, parents.bytes);
        source_root.retire(accounting)?;
        let encoded =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_generation_root(&fork_root.value, observe, allocation_error)
            })?;
        let generation_root = self
            .put_encoded_with_accounting(
                ObjectKind::GenerationRoot,
                encoded,
                accounting,
                cancellation,
            )
            .await?;
        fork_root.retire(accounting)?;
        let (proof, pending) = self
            .proof_reusing(
                generation_root,
                proven_at,
                destination_id.volume_id(),
                Some(source.workspace.volume.id),
                config,
                accounting,
                cancellation,
            )
            .await?;
        Ok((proof, source_object, pending))
    }
    pub(super) async fn publication_proof(
        &self,
        generation_root: ObjectId,
        proven_at: u64,
        volume_id: VolumeId,
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Owned<VerifiedPublication<A, O>>, FsError> {
        self.proof_reusing(
            generation_root,
            proven_at,
            volume_id,
            None,
            config,
            accounting,
            cancellation,
        )
        .await
        .map(|(proof, _)| proof)
    }

    /// A fork's proof extends `fork_of`'s published proof of its source; the
    /// returned proof becomes reusable once the fork's volume is created.
    async fn proof_reusing(
        &self,
        generation_root: ObjectId,
        proven_at: u64,
        volume_id: VolumeId,
        fork_of: Option<VolumeId>,
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<
        (
            Owned<VerifiedPublication<A, O>>,
            Option<crate::kernel::closure::PendingProof>,
        ),
        FsError,
    > {
        use crate::kernel::closure::{ProofContext, ProofMeter, closure_error};
        let limits = closure_limits(config);
        let cache = Some(&self.inner.publication_proof.inner);
        let (proof, allocation, pending) = match (accounting, fork_of) {
            #[cfg(not(target_arch = "wasm32"))]
            (KernelAccounting::NativeOriginal { work: original, .. }, fork_of) => {
                let mut context = ProofContext::new(
                    &self.inner.objects,
                    limits,
                    ProofMeter::Native {
                        original,
                        cancellation,
                    },
                );
                let (proof, mut allocation, pending) = match fork_of {
                    Some(source) => context.prove_fork(generation_root, cache, source).await?,
                    None => {
                        let (proof, allocation) = context.prove(generation_root).await?;
                        (proof, allocation, None)
                    }
                };
                (
                    proof,
                    KernelOwnedAllocation::NativeOriginal(allocation.take_native()?),
                    pending,
                )
            }
            (accounting, Some(source)) => {
                let closure = |error| FsError::Closure(closure_error(error));
                let meter = ProofMeter::standalone(accounting.remaining()?, cancellation)
                    .map_err(closure)?;
                let result = ProofContext::new(&self.inner.objects, limits, meter.clone())
                    .prove_fork(generation_root, cache, source)
                    .await;
                merge_creation_backend_work(accounting, meter.work())?;
                let (proof, guard, pending) = result.map_err(closure)?;
                drop(guard);
                let bytes = usize_to_u64(proof.objects.capacity() * size_of::<ObjectId>());
                let allocation = accounting.claim_owned(bytes, 0)?;
                (proof, allocation, pending)
            }
            (accounting, None) => {
                let result = prove_generation_closure(
                    &self.inner.objects,
                    generation_root,
                    limits,
                    accounting.remaining()?,
                    cancellation,
                )
                .await;
                let work = match &result {
                    Ok(proof) => proof.work,
                    Err(failure) => *failure.work,
                };
                merge_creation_backend_work(accounting, work)?;
                let proof = result.map_err(|failure| FsError::from(failure.error))?;
                let bytes = usize_to_u64(proof.objects.capacity() * size_of::<ObjectId>());
                let allocation = accounting.claim_owned(bytes, 0)?;
                (proof, allocation, None)
            }
        };
        #[cfg(not(target_arch = "wasm32"))]
        let mut allocation = allocation;
        if proof.root.volume_id != volume_id {
            return Err(FsError::VolumeMismatch);
        }
        let root_bytes = root_parent_bytes(&proof.root)?;
        let _ = root_bytes;
        let bytes = usize_to_u64(proof.objects.capacity() * size_of::<ObjectId>());
        drop(proof.root);
        #[cfg(not(target_arch = "wasm32"))]
        if let KernelOwnedAllocation::NativeOriginal(guard) = &mut allocation {
            guard.release(root_bytes)?;
        }
        Ok((
            Owned::new(
                VerifiedPublication {
                    filesystem: self.clone(),
                    generation_root,
                    volume_id,
                    limits,
                    objects: proof.objects,
                    proven_at,
                    _allocation: KernelOwnedAllocation::Standalone,
                },
                allocation,
                bytes,
            ),
            pending,
        ))
    }
    /// Forks one immutable generation under its publication guard and work context.
    pub(crate) async fn fork_workspace<W, G, E>(
        &self,
        destination: crate::WorkspaceName,
        source: &crate::Generation<A, O>,
        idempotency_key: crate::IdempotencyKey,
        paths: Option<Vec<String>>,
        permit: PublicationPermit,
        budget: W,
        cancellation: &CancellationToken,
        guard: Option<&mut G>,
    ) -> W::Output<crate::Workspace<A, O>, E>
    where
        W: FsWork,
        G: crate::AuthorityGuard,
        G::Error: Into<E>,
        E: From<FsError> + From<WorkspaceError> + Send,
    {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let accounting = &mut accounting;
            let name_bytes = if readonly_standalone_live(accounting).is_some() {
                destination.retained_bytes()
            } else {
                0
            };
            let (_name_allocation, admitted) = accounting.record_actual_owned(name_bytes)?;
            admitted?;
            let path_bytes = paths.as_ref().map_or(Ok(0), |paths| {
                crate::workspace::workspace_owned_strings_bytes(paths)
            })?;
            let (path_allocation, admitted) = accounting.record_actual_owned(path_bytes)?;
            let paths = Owned::new(paths, path_allocation, path_bytes);
            admitted?;
            if !self.same_deployment(&source.workspace.volume.fs) {
                return Err(WorkspaceError::ForeignGeneration.into());
            }
            let destination_id =
                self.workspace_id_for_name(&destination, accounting, cancellation)?;
            let volume_id = destination_id.volume_id();
            let config = source.workspace.volume.config;
            let (fork_root, source_root, pending) = self
                .materialize_forked_generation_root(
                    source,
                    destination_id,
                    paths,
                    accounting,
                    cancellation,
                )
                .await?;
            let original_root = fork_root.value.generation_root;
            let (created, operation) = match self
                .read_creation(volume_id, accounting, cancellation)
                .await
                .map_err(WorkspaceError::from)
            {
                Ok(created) => created,
                Err(WorkspaceError::Filesystem(FsError::Authority(
                    AuthorityStoreError::Missing,
                ))) => {
                    self.publish_fork(
                        ForkPublication {
                            source,
                            destination_id,
                            fork_root: &fork_root.value,
                            source_root,
                            idempotency_key,
                            permit,
                        },
                        accounting,
                        cancellation,
                        guard,
                    )
                    .await?;
                    self.read_creation(volume_id, accounting, cancellation)
                        .await
                        .map_err(WorkspaceError::from)?
                }
                Err(error) => return Err(error.into()),
            };
            fork_root.retire(accounting)?;
            if operation != idempotency_key.operation_id()
                || created.volume_id != volume_id
                || created.config != config
                || created.initial_generation_root != original_root
            {
                return Err(WorkspaceError::from(FsError::CreationRejected).into());
            }
            if let Some(pending) = pending {
                pending.created();
            }
            Ok(crate::Workspace {
                name: destination,
                id: destination_id,
                volume: Volume {
                    fs: self.clone(),
                    id: volume_id,
                    config,
                },
                #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
                source: None,
                access: None,
            })
        }
        .await;
        budget.finish(result, &accounting)
    }

    async fn publish_fork<G, E>(
        &self,
        publication: ForkPublication<'_, A, O>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
        guard: Option<&mut G>,
    ) -> Result<(), E>
    where
        G: crate::AuthorityGuard,
        G::Error: Into<E>,
        E: From<FsError> + From<WorkspaceError> + Send,
    {
        let permit = publication.permit;
        let prepared = self
            .prepare_fork_commit(&publication, accounting, cancellation)
            .await?;
        accounting.check()?;
        let mut authority = authority_work(accounting, cancellation)?;
        let result = self
            .inner
            .authority
            .fork_workspace(prepared.fork, permit, &mut authority, guard)
            .await;
        let recorded = record_authority_work(accounting, &authority);
        Self::retire_fork_commit(prepared.changes, prepared.payload_bytes, accounting)?;
        recorded?;
        drop(prepared.hold);
        let outcome = result.map_err(|error| match error {
            crate::AuthorityError::Authority(error) => E::from(FsError::from(error)),
            crate::AuthorityError::Owner(error) => error.into(),
        })?;
        match outcome {
            crate::WorkspaceForkOutcome::Committed => {}
            crate::WorkspaceForkOutcome::RetentionConflict => {
                return Err(WorkspaceError::RetentionConflict.into());
            }
            crate::WorkspaceForkOutcome::CreationRejected => {
                return Err(WorkspaceError::from(FsError::CreationRejected).into());
            }
        }
        Ok(())
    }

    /// Builds one fork's authority records and holds its closure live until
    /// the commit that carries them is durable.
    pub(super) async fn prepare_fork_commit(
        &self,
        publication: &ForkPublication<'_, A, O>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<PreparedForkCommit, WorkspaceError> {
        let source = publication.source;
        let destination_id = publication.destination_id;
        let fork_root = publication.fork_root;
        let source_root = publication.source_root;
        let operation_id = publication.idempotency_key.operation_id();
        let source_volume = &source.workspace.volume;
        let volume_id = destination_id.volume_id();
        let lineage = self
            .fork_lineage_with_accounting(source, accounting, cancellation)
            .await?;
        let (retention, retained, mut creation, payload_bytes) = fork_records_with_accounting(
            source_volume,
            source_root,
            destination_id,
            fork_root.generation_root,
            operation_id,
            accounting,
        )?;
        creation.publication = Some(
            crate::PublicationChanges::initial(
                &self.inner.objects,
                VolumeCreated {
                    volume_id,
                    config: source_volume.config,
                    initial_generation_root: fork_root.generation_root,
                },
                accounting,
                cancellation,
            )
            .await?,
        );
        let changes = creation.publication.clone();
        let fork = crate::WorkspaceForkCommit {
            lineage,
            destination: volume_authority_id(volume_id),
            creation,
            retention,
            retained,
        };
        let hold = self
            .flush_publication(
                crate::PublicationScope::Closure {
                    objects: &fork_root.objects,
                    proven_at: fork_root.proven_at,
                },
                accounting,
                cancellation,
            )
            .await?;
        Ok(PreparedForkCommit {
            fork,
            changes,
            hold,
            payload_bytes,
        })
    }

    pub(super) fn retire_fork_commit(
        changes: Option<crate::PublicationChanges>,
        payload_bytes: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        if let Some(changes) = changes {
            changes.retire(accounting)?;
        }
        if readonly_standalone_live(accounting).is_some() {
            accounting.release_owned(&mut KernelOwnedAllocation::Standalone, payload_bytes)?;
        }
        Ok(())
    }

    pub(super) async fn fork_lineage_with_accounting(
        &self,
        source: &crate::Generation<A, O>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<crate::GenerationForkSource>, WorkspaceError> {
        if !self.inner.authority.supports_generation_lineage_prefix() {
            return Ok(None);
        }
        let (root, _) = source
            .workspace
            .volume
            .resolve_head_generation(accounting, cancellation)
            .await?;
        self.initial_generation(source.workspace.clone(), root, accounting, cancellation)
            .await?;
        Ok(Some(crate::GenerationForkSource {
            authority: volume_authority_id(source.workspace.volume.id),
            generation: source.id,
            lineage: if root == source.id.object_id() {
                crate::GenerationFork::PublishedPrefix
            } else {
                crate::GenerationFork::Independent
            },
        }))
    }
}

pub(super) fn fork_records_with_accounting<A, O>(
    source: &Volume<A, O>,
    source_root: ObjectId,
    destination: crate::WorkspaceId,
    fork_root: ObjectId,
    operation_id: OperationId,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<(crate::AuthorityId, ProposedCommit, ProposedCommit, u64), FsError> {
    let mut label = crate::kernel::Owned::<Vec<u8>>::allocate(32, accounting)?;
    let encoding = WorkCounters {
        bytes_encoded: 32,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(encoding)?;
    label.value.resize(32, 0);
    hex::encode_to_slice(destination.into_bytes(), &mut label.value)
        .map_err(|_| FsError::InvalidDiff)?;
    accounting.record_completed(encoding)?;
    let text = String::from_utf8(label.value).map_err(|_| FsError::InvalidDiff)?;
    let label = crate::kernel::Owned::new(text, label.allocation, label.bytes);
    let hashing = WorkCounters {
        bytes_hashed: (b"acyclic-fs-retention-authority-v1\0".len()
            + 16
            + 1
            + 8
            + label.value.len()) as u64,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(hashing)?;
    let retention = retention_authority_id(source.id, RetentionKind::ForkBase, &label.value);
    accounting.record_completed(hashing)?;
    let fact = crate::kernel::Owned::new(
        RetentionCreated {
            volume_id: source.id,
            kind: RetentionKind::ForkBase,
            label: label.value,
            generation_root: source_root,
            config: source.config,
        },
        label.allocation,
        label.bytes,
    );
    let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
        crate::kernel::encode_retention_created(&fact.value, observe, allocation_error)
    })?;
    let retained_bytes = encoded.0.capacity() as u64;
    let retained = creation_commit(
        OperationId::from_bytes(retention.into_bytes()),
        encoded,
        accounting,
    )?;
    fact.retire(accounting)?;
    let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
        crate::kernel::encode_volume_created(
            VolumeCreated {
                volume_id: destination.volume_id(),
                config: source.config,
                initial_generation_root: fork_root,
            },
            observe,
            allocation_error,
        )
    })?;
    let creation_bytes = encoded.0.capacity() as u64;
    let creation = creation_commit(operation_id, encoded, accounting)?;
    let payload_bytes = retained_bytes
        .checked_add(creation_bytes)
        .ok_or(WorkError::Overflow)?;
    Ok((retention, retained, creation, payload_bytes))
}
