use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(super) async fn materialize_forked_generation_root(
        &self,
        source: &crate::Generation<A, O>,
        destination_id: crate::WorkspaceId,
        config: VolumeConfig,
        paths: Option<Vec<String>>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Owned<VerifiedPublication<A, O>>, ObjectId), WorkspaceError> {
        let path_bytes = paths.as_ref().map_or(Ok(0), |paths| {
            crate::workspace::workspace_owned_strings_bytes(paths)
        })?;
        let (path_allocation, admitted) = accounting.record_actual_owned(path_bytes)?;
        let mut paths = (paths, path_allocation);
        admitted?;
        accounting.check()?;
        let proven_at = self.inner.objects.collection_sweeps();
        let source_object = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: source.id.digest(),
        };
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
        let file_table = if let Some(selected) = paths.0.take() {
            accounting.release_owned(&mut paths.1, path_bytes)?;
            self.select_fork_paths_with_accounting(
                source,
                &source_root.value,
                selected,
                accounting,
                cancellation,
            )
            .await?
        } else {
            source_root.value.file_table
        };
        let mut parents = Owned::<Vec<GenerationId>>::allocate(1, accounting)?;
        let copied = WorkCounters {
            bytes_copied: size_of::<GenerationId>() as u64,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(copied)?;
        parents.value.push(source.id);
        accounting.record_completed(copied)?;
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
        let proof = self
            .publication_proof(
                generation_root,
                proven_at,
                destination_id.volume_id(),
                config,
                accounting,
                cancellation,
            )
            .await?;
        Ok((proof, source_object))
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
        let limits = closure_limits(config);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let (proof, mut allocation) = prove_generation_closure_with_native_work(
                &self.inner.objects,
                generation_root,
                limits,
                original,
                cancellation,
            )
            .await?;
            if proof.root.volume_id != volume_id {
                return Err(FsError::VolumeMismatch);
            }
            let root_bytes = root_parent_bytes(&proof.root)?;
            let bytes = usize_to_u64(proof.objects.capacity() * size_of::<ObjectId>());
            drop(proof.root);
            allocation.release(root_bytes)?;
            return Ok(Owned::new(
                VerifiedPublication {
                    filesystem: self.clone(),
                    generation_root,
                    volume_id,
                    limits,
                    objects: proof.objects,
                    proven_at,
                    _allocation: KernelOwnedAllocation::Standalone,
                },
                KernelOwnedAllocation::NativeOriginal(allocation),
                bytes,
            ));
        }
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
        if proof.root.volume_id != volume_id {
            return Err(FsError::VolumeMismatch);
        }
        drop(proof.root);
        let bytes = usize_to_u64(proof.objects.capacity() * size_of::<ObjectId>());
        let allocation = accounting.claim_owned(bytes, 0)?;
        Ok(Owned::new(
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
            let mut paths = (paths, path_allocation);
            admitted?;
            if !self.same_deployment(&source.workspace.volume.fs) {
                return Err(WorkspaceError::ForeignGeneration.into());
            }
            accounting.check()?;
            let destination_id = self.derive_fork_workspace_id_with_accounting(
                &destination,
                accounting,
                cancellation,
            )?;
            let volume_id = destination_id.volume_id();
            let source_volume = &source.workspace.volume;
            let config = source_volume.config;
            let selected = paths.0.take();
            accounting.release_owned(&mut paths.1, path_bytes)?;
            let (fork_root, source_root) = self
                .materialize_forked_generation_root(
                    source,
                    destination_id,
                    config,
                    selected,
                    accounting,
                    cancellation,
                )
                .await?;
            match self
                .read_fork_creation_with_accounting(volume_id, accounting, cancellation)
                .await
            {
                Ok((created, operation)) => {
                    if operation != idempotency_key.operation_id()
                        || created.volume_id != volume_id
                        || created.config != config
                        || created.initial_generation_root != fork_root.value.generation_root
                    {
                        return Err(WorkspaceError::from(FsError::CreationRejected).into());
                    }
                    fork_root.retire(accounting)?;
                    return Ok(crate::Workspace {
                        name: destination,
                        id: destination_id,
                        volume: Volume {
                            fs: self.clone(),
                            id: volume_id,
                            config,
                        },
                        #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
                        source: None,
                    });
                }
                Err(WorkspaceError::Filesystem(FsError::Authority(
                    AuthorityStoreError::Missing,
                ))) => accounting.check()?,
                Err(error) => return Err(error.into()),
            }
            let lineage = self
                .fork_lineage_with_accounting(source, accounting, cancellation)
                .await?;
            let (retention, retained, creation, payload_bytes) = fork_records_with_accounting(
                source_volume,
                source_root,
                destination_id,
                fork_root.value.generation_root,
                idempotency_key.operation_id(),
                accounting,
                cancellation,
            )?;
            let fork = crate::WorkspaceForkCommit {
                lineage,
                destination: volume_authority_id(volume_id),
                creation,
                retention,
                retained,
            };
            let original_root = fork_root.value.generation_root;
            let hold = self
                .flush_publication(
                    crate::PublicationScope::Closure {
                        objects: &fork_root.value.objects,
                        proven_at: fork_root.value.proven_at,
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
                .fork_workspace(fork, permit, &mut authority, guard)
                .await;
            record_authority_work(accounting, &authority)?;
            if readonly_standalone_live(accounting).is_some() {
                accounting.release_owned(&mut KernelOwnedAllocation::Standalone, payload_bytes)?;
            }
            drop(hold);
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
            fork_root.retire(accounting)?;
            let (created, operation) = self
                .read_fork_creation_with_accounting(volume_id, accounting, cancellation)
                .await?;
            if operation != idempotency_key.operation_id()
                || created.config != config
                || created.initial_generation_root != original_root
            {
                return Err(WorkspaceError::from(FsError::CreationRejected).into());
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
            })
        }
        .await;
        budget.finish(result, &accounting)
    }

    pub(super) fn derive_fork_workspace_id_with_accounting(
        &self,
        destination: &crate::WorkspaceName,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::WorkspaceId, WorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            return crate::WorkspaceId::derive(
                self.inner.workspace_namespace,
                destination,
                original,
                cancellation,
            );
        }
        let derived = crate::WorkspaceId::derive(
            self.inner.workspace_namespace,
            destination,
            accounting.remaining()?,
            cancellation,
        );
        let work = match &derived {
            Ok(receipt) => receipt.work,
            Err(failure) => *failure.work,
        };
        merge_fork_backend_work(accounting, work)?;
        derived
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
    }

    pub(super) async fn read_fork_creation_with_accounting(
        &self,
        volume_id: VolumeId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(VolumeCreated, OperationId), WorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            return self
                .read_creation_record(volume_id, original, cancellation)
                .await
                .map_err(WorkspaceError::from);
        }
        let read = self
            .read_creation_record(volume_id, accounting.remaining()?, cancellation)
            .await;
        let work = match &read {
            Ok(receipt) => receipt.work,
            Err(failure) => *failure.work,
        };
        merge_fork_backend_work(accounting, work)?;
        read.map(|receipt| receipt.value)
            .map_err(|failure| WorkspaceError::from(failure.error))
    }

    /// The source lineage a fork's authority starts from: the published
    pub(super) async fn fork_lineage_with_accounting(
        &self,
        source: &crate::Generation<A, O>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<crate::GenerationForkSource>, WorkspaceError> {
        if !self.inner.authority.supports_generation_lineage_prefix() {
            return Ok(None);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let head = source
                .workspace
                .volume
                .checkout(
                    GenerationSelector::Head,
                    CheckoutMode::read_only_pinned(),
                    original,
                    cancellation,
                )
                .await?;
            return Ok(Some(crate::GenerationForkSource {
                authority: volume_authority_id(source.workspace.volume.id),
                generation: source.id,
                lineage: if head.generation_id() == source.id {
                    crate::GenerationFork::PublishedPrefix
                } else {
                    crate::GenerationFork::Independent
                },
            }));
        }
        let head = source
            .workspace
            .head(accounting.remaining()?, cancellation)
            .await;
        let work = match &head {
            Ok(receipt) => receipt.work,
            Err(failure) => *failure.work,
        };
        merge_fork_backend_work(accounting, work)?;
        let head = head.map_err(|failure| failure.error)?;
        Ok(Some(crate::GenerationForkSource {
            authority: volume_authority_id(source.workspace.volume.id),
            generation: source.id,
            lineage: if head.value.id == source.id {
                crate::GenerationFork::PublishedPrefix
            } else {
                crate::GenerationFork::Independent
            },
        }))
    }
}

/// A fork's two first records: the retention of its source generation, under
pub(super) fn fork_records_with_accounting<A, O>(
    source: &Volume<A, O>,
    source_root: ObjectId,
    destination: crate::WorkspaceId,
    fork_root: ObjectId,
    operation_id: OperationId,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(crate::AuthorityId, ProposedCommit, ProposedCommit, u64), FsError> {
    accounting.check()?;
    cancellation.check()?;
    let mut label = crate::kernel::Owned::<Vec<u8>>::allocate(32, accounting)?;
    let encoding = WorkCounters {
        bytes_encoded: 32,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(encoding)?;
    label.value.resize(32, 0);
    hex::encode_to_slice(destination.into_bytes(), &mut label.value)
        .expect("sixteen-byte identity encodes to thirty-two hex bytes");
    accounting.record_completed(encoding)?;
    let text = String::from_utf8(label.value).expect("hex encoder produces ASCII");
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
    let retained = creation_commit_with_accounting(
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
    let creation = creation_commit_with_accounting(operation_id, encoded, accounting)?;
    let payload_bytes = retained_bytes
        .checked_add(creation_bytes)
        .ok_or(WorkError::Overflow)?;
    Ok((retention, retained, creation, payload_bytes))
}

pub(super) fn merge_fork_backend_work(
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    backend: WorkCounters,
) -> Result<(), crate::workspace::WorkspaceError> {
    let (allocations, work, budget, _) = accounting
        .standalone_parts()
        .ok_or(FsError::HandleAllocationFailed)?;
    let prior = *work;
    *work = prior
        .with_backend(backend, allocations.live_bytes())
        .map_err(|error| crate::workspace::WorkspaceError::WorkMerge {
            prior,
            backend,
            error,
        })?;
    work.verify(budget)?;
    Ok(())
}
