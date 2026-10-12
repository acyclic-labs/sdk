use super::*;

pub(super) const ROOT_FILE_DOMAIN: &[u8] = b"acyclic-fs-root-file-v1\0";
pub(super) const CREATION_OPERATION_DOMAIN: &[u8] = b"acyclic-fs-create-operation-v1\0";
pub(super) const CREATION_FINGERPRINT_DOMAIN: &[u8] = b"acyclic-fs-create-fingerprint-v1\0";

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    /// Creates a fresh volume and its authenticated empty generation.
    pub async fn create_volume(
        &self,
        config: VolumeConfig,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        self.create_volume_with_id(VolumeId::new(), config, budget, cancellation)
            .await
    }
    /// Idempotently creates one caller-selected volume identity.
    pub async fn create_volume_with_id(
        &self,
        volume_id: VolumeId,
        config: VolumeConfig,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = self
            .create_volume_operation(
                volume_id,
                config,
                None,
                &mut accounting,
                cancellation,
                None::<&mut ()>,
            )
            .await
            .map(|(volume, _)| volume);
        measured(result, &accounting)
    }
    /// Opens one volume using its authenticated immutable configuration.
    pub async fn open_volume(
        &self,
        volume_id: VolumeId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        cancellation
            .check()
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        let observed = self
            .read_creation_record(volume_id, budget, cancellation)
            .await?;
        let config = observed.value.0.config;
        validate_volume_capabilities(config, self.inner.capabilities)
            .map_err(|error| OperationFailure::new(error, observed.work))?;
        Ok(OperationReceipt {
            value: Volume {
                fs: self.clone(),
                id: volume_id,
                config,
            },
            work: observed.work,
        })
    }
    /// Restores a volume authority from a fully imported immutable generation.
    pub async fn restore_volume(
        &self,
        manifest: &GenerationExportManifest,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        cancellation
            .check()
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        validate_volume_capabilities(manifest.config, self.inner.capabilities)
            .map_err(OperationFailure::before_work)?;
        let proven_at = self.inner.objects.collection_sweeps();
        let proof = authenticate_generation_export_manifest_async(
            &self.inner.objects,
            manifest,
            closure_limits(manifest.config),
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| {
            OperationFailure::new(map_transfer_error(failure.error), *failure.work)
        })?;
        let mut work = proof.work;
        drop(proof);
        let mut allocations = AllocationLedger::default();
        let mut accounting = KernelAccounting::borrowed_standalone(
            &mut allocations,
            &mut work,
            budget,
            cancellation,
        );
        let result = self
            .publish_creation(
                VolumeCreated {
                    volume_id: manifest.volume_id,
                    config: manifest.config,
                    initial_generation_root: manifest.generation_root,
                },
                Some(operation_id),
                &manifest.objects,
                proven_at,
                &mut accounting,
                cancellation,
                None::<&mut ()>,
            )
            .await;
        measured(result, &accounting)
    }
    pub(super) async fn create_volume_operation<E, G>(
        &self,
        volume_id: VolumeId,
        config: VolumeConfig,
        operation_id: Option<OperationId>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
        guard: Option<&mut G>,
    ) -> Result<(Volume<A, O>, ObjectId), E>
    where
        E: From<FsError> + Send,
        G: crate::AuthorityGuard,
        G::Error: Into<E>,
    {
        accounting.check()?;
        validate_volume_capabilities(config, self.inner.capabilities)?;
        let proven_at = self.inner.objects.collection_sweeps();
        let produced = self
            .build_empty_generation(volume_id, config, accounting, cancellation)
            .await?;
        let generation_root = produced[3];
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
        let result = self
            .publish_creation(
                VolumeCreated {
                    volume_id,
                    config,
                    initial_generation_root: generation_root,
                },
                operation_id,
                &proof.value.objects,
                proven_at,
                accounting,
                cancellation,
                guard,
            )
            .await;
        proof.retire(accounting)?;
        result.map(|volume| (volume, generation_root))
    }
    pub(super) async fn publish_creation<E, G>(
        &self,
        creation: VolumeCreated,
        operation_id: Option<OperationId>,
        closure: &[ObjectId],
        proven_at: u64,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
        guard: Option<&mut G>,
    ) -> Result<Volume<A, O>, E>
    where
        E: From<FsError> + Send,
        G: crate::AuthorityGuard,
        G::Error: Into<E>,
    {
        let VolumeCreated {
            volume_id, config, ..
        } = creation;
        accounting.check()?;
        let operation_id = match operation_id {
            Some(id) => id,
            None => {
                accounting.charge(identity_hash_work(CREATION_OPERATION_DOMAIN))?;
                OperationId::from_bytes(
                    derived_digest(CREATION_OPERATION_DOMAIN, volume_id).as_bytes()[..16]
                        .try_into()
                        .expect("sixteen digest bytes"),
                )
            }
        };
        let encoded =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_volume_created(creation, observe, allocation_error)
            })?;
        let capacity = encoded.0.capacity() as u64;
        let commit = creation_commit(operation_id, encoded, accounting)?;
        let hold = self
            .flush_publication(
                crate::PublicationScope::Closure {
                    objects: closure,
                    proven_at,
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
            .create_first_record(
                volume_authority_id(volume_id),
                commit,
                &mut authority,
                guard,
            )
            .await;
        record_authority_work(accounting, &authority)?;
        if readonly_standalone_live(accounting).is_some() {
            accounting.release_owned(&mut KernelOwnedAllocation::Standalone, capacity)?;
        }
        drop(hold);
        let created = result.map_err(|error| match error {
            crate::AuthorityError::Authority(error) => E::from(FsError::from(error)),
            crate::AuthorityError::Owner(error) => error.into(),
        })?;
        if !created {
            return Err(FsError::CreationRejected.into());
        }
        Ok(Volume {
            fs: self.clone(),
            id: volume_id,
            config,
        })
    }
    pub(crate) async fn read_creation_record<W: FsWork>(
        &self,
        volume_id: VolumeId,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(VolumeCreated, OperationId), FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            accounting.check()?;
            let mut work = authority_work(&accounting, cancellation)?;
            let result = self
                .inner
                .authority
                .replay(
                    volume_authority_id(volume_id),
                    Sequence::GENESIS,
                    ReplayLimit {
                        records: 1,
                        payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES,
                    },
                    &mut work,
                )
                .await;
            record_authority_work(&mut accounting, &work)?;
            let (records, _allocation) = result?;
            decode_first_creation_record(volume_id, &records, &mut accounting)
        }
        .await;
        budget.finish(result, &accounting)
    }
    pub(super) async fn build_empty_generation(
        &self,
        volume_id: VolumeId,
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<[ObjectId; 4], FsError> {
        let encoded =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_file_metadata(
                    FileMetadata::default(),
                    observe,
                    allocation_error,
                )
            })?;
        let metadata = self
            .put_encoded_with_accounting(ObjectKind::Metadata, encoded, accounting, cancellation)
            .await?;
        let encoded =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_tree_page(
                    &TreePage::Leaf(Vec::new()),
                    config.limits.maximum_directory_page_entries,
                    observe,
                    allocation_error,
                )
            })?;
        let tree = self
            .put_encoded_with_accounting(ObjectKind::TreePage, encoded, accounting, cancellation)
            .await?;
        accounting.charge(identity_hash_work(ROOT_FILE_DOMAIN))?;
        let root_file_id = FileId::from_bytes(
            derived_digest(ROOT_FILE_DOMAIN, volume_id).as_bytes()[..16]
                .try_into()
                .expect("sixteen digest bytes"),
        );
        let record = FileRecord {
            file_id: root_file_id,
            kind: FileKind::Directory,
            link_count: 1,
            metadata,
            payload: FilePayload::Directory { entries: tree },
        };
        let encoded =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_file_table_leaf_records(
                    std::slice::from_ref(&record),
                    config.limits.maximum_directory_page_entries,
                    None,
                    observe,
                    allocation_error,
                )
            })?;
        let file_table = self
            .put_encoded_with_accounting(
                ObjectKind::FileTablePage,
                encoded,
                accounting,
                cancellation,
            )
            .await?;
        let root = GenerationRoot {
            volume_id,
            root_file_id,
            file_table,
            parents: Vec::new(),
            required_features: 0,
        };
        let encoded =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_generation_root(&root, observe, allocation_error)
            })?;
        let generation_root = self
            .put_encoded_with_accounting(
                ObjectKind::GenerationRoot,
                encoded,
                accounting,
                cancellation,
            )
            .await?;
        Ok([metadata, tree, file_table, generation_root])
    }
}

pub(super) fn derived_digest(domain: &[u8], volume_id: VolumeId) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(&volume_id.into_bytes());
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

pub(super) fn identity_hash_work(domain: &[u8]) -> WorkCounters {
    WorkCounters {
        bytes_hashed: u64::try_from(domain.len()).unwrap_or(u64::MAX) + 16,
        bytes_copied: 16,
        ..WorkCounters::default()
    }
}

pub(super) fn creation_fingerprint(payload: &[u8]) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CREATION_FINGERPRINT_DOMAIN);
    hasher.update(payload);
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

pub(super) fn creation_commit(
    operation_id: OperationId,
    encoded: (Vec<u8>, crate::kernel::allocation::KernelOwnedAllocation),
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<ProposedCommit, FsError> {
    accounting.check()?;
    let hashing = WorkCounters {
        bytes_hashed: (CREATION_FINGERPRINT_DOMAIN.len() as u64)
            .checked_add(encoded.0.len() as u64)
            .ok_or(WorkError::Overflow)?,
        ..WorkCounters::default()
    };
    accounting.admit(hashing)?;
    let fingerprint = creation_fingerprint(&encoded.0);
    accounting.record_completed(hashing)?;
    #[cfg(not(target_arch = "wasm32"))]
    if let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) = &encoded.1
    {
        if allocation.bytes() != encoded.0.capacity() as u64
            || !accounting
                .native_work()
                .is_some_and(|original| allocation.work().shares_ledger(original))
        {
            return Err(FsError::HandleAllocationFailed);
        }
        let control_bytes = crate::kernel::allocation::bytes_owner_control_bytes::<
            crate::kernel::Owned<Vec<u8>>,
        >()?;
        let mut control = allocation.work().claim(
            control_bytes,
            u64::from((control_bytes) != 0),
            crate::WorkCounters::UNCHARGED,
        )?;
        let bytes = encoded.0.capacity() as u64;
        let mut payload = crate::kernel::Owned::new(encoded.0, encoded.1, bytes);
        let KernelOwnedAllocation::NativeOriginal(allocation) = &mut payload.allocation else {
            unreachable!("native encoded allocation was matched before moving");
        };
        allocation.absorb(&mut control)?;
        payload.bytes = allocation.bytes();
        let payload = Bytes::from_owner(payload);
        return Ok(ProposedCommit {
            operation_id,
            fingerprint,
            payload,
        });
    }
    let owner = WorkCounters {
        allocation_operations: u64::from(encoded.0.capacity() != encoded.0.len()),
        ..WorkCounters::default()
    };
    accounting.admit(owner)?;
    let payload = Bytes::from(encoded.0);
    accounting.record_completed(owner)?;
    Ok(ProposedCommit {
        operation_id,
        fingerprint,
        payload,
    })
}

pub(super) fn merge_creation_backend_work(
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    spent: WorkCounters,
) -> Result<(), FsError> {
    let (allocations, work, budget, _) = accounting
        .standalone_parts()
        .ok_or(FsError::HandleAllocationFailed)?;
    *work = work.with_backend(spent, allocations.live_bytes())?;
    work.verify(budget)?;
    Ok(())
}
