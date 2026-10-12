use super::*;

pub(super) const ROOT_FILE_DOMAIN: &[u8] = b"acyclic-fs-root-file-v1\0";

pub(super) const CREATION_OPERATION_DOMAIN: &[u8] = b"acyclic-fs-create-operation-v1\0";

pub(super) const CREATION_FINGERPRINT_DOMAIN: &[u8] = b"acyclic-fs-create-fingerprint-v1\0";

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {

    /// Creates a fresh volume identity and its authenticated empty generation.
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
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.create_volume",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty,
            volume_id = crate::obs::hex(&volume_id.into_bytes()),
        );
        let result = crate::obs::in_span(
            &span,
            self.create_volume_with_id_operation(volume_id, config, None, budget, cancellation),
        )
        .await;
        result.observe_on(&span, "create_volume")
    }

    pub(super) async fn create_volume_with_id_operation(
        &self,
        volume_id: VolumeId,
        config: VolumeConfig,
        operation_id: Option<OperationId>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = self.create_volume_with_id_operation_with_accounting(
            volume_id, config, operation_id, &mut accounting, cancellation, |id, commit, budget| async move {
                    let created = ({ let __authority_args = (id, commit, budget, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = self.inner.authority.create_first_record(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) });
                    let work = match &created { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
                    (created.map(|receipt| receipt.value).map_err(|failure| FsError::from(failure.error)), work)
                },
        ).await;
        let work = accounting.spent();
        result.map(|(value, _)| FsReceipt { value, work }).map_err(|error| OperationFailure::new(error, work))
    }

    pub(super) async fn create_volume_with_id_operation_with_accounting<E, P, Fut>(
        &self, volume_id: VolumeId, config: VolumeConfig, operation_id: Option<OperationId>,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken, publication: P,
    ) -> Result<(Volume<A, O>, ObjectId), E>
    where
        E: From<FsError> + Send,
        P: FnOnce(crate::AuthorityId, ProposedCommit, WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = (Result<bool, E>, WorkCounters)> + ProviderTask,
    {
        accounting.check()?;
        validate_volume_capabilities(config, self.inner.capabilities)?;
        let proven_at = self.inner.objects.collection_sweeps();
        let produced = self.build_empty_generation_with_accounting(volume_id, config, accounting, cancellation).await?;
        let generation_root = produced[3];
        let creation = VolumeCreated { volume_id, config, initial_generation_root: generation_root };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let proof = prove_generation_closure_with_native_work(
                &self.inner.objects, generation_root, closure_limits(config), original, cancellation,
            ).await.map_err(FsError::from)?;
            let volume = self.publish_volume_creation_with_accounting(
                creation, operation_id, &proof.0.objects, proven_at, accounting, cancellation, publication,
            ).await?;
            return Ok((volume, generation_root));
        }
        let proof = prove_generation_closure(
            &self.inner.objects, generation_root, closure_limits(config), accounting.remaining()?, cancellation,
        ).await;
        let spent = match &proof { Ok(proof) => proof.work, Err(failure) => *failure.work, };
        merge_creation_backend_work(accounting, spent)?;
        let proof = proof.map_err(|failure| FsError::from(failure.error))?;
        let closure_bytes = (proof.objects.capacity() as u64)
            .checked_mul(size_of::<ObjectId>() as u64).ok_or(FsError::from(WorkError::Overflow))?;
        let root_bytes = root_parent_bytes(&proof.root)?;
        let owned_bytes = closure_bytes.checked_add(root_bytes).ok_or(FsError::from(WorkError::Overflow))?;
        let mut allocation = accounting.claim_owned(owned_bytes, 0)?;
        let result = self.publish_volume_creation_with_accounting(
            creation, operation_id, &proof.objects, proven_at, accounting, cancellation, publication,
        ).await;
        drop(proof);
        accounting.release_owned(&mut allocation, owned_bytes)?;
        result.map(|volume| (volume, generation_root))
    }

    /// Opens one volume by reconstructing its immutable configuration from the
    pub async fn open_volume(
        &self,
        volume_id: VolumeId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.open_volume",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty,
            volume_id = crate::obs::hex(&volume_id.into_bytes()),
        );
        let result = crate::obs::in_span(&span, async move {
            cancellation
                .check()
                .map_err(|error| OperationFailure::before_work(error.into()))?;
            let observed = self.read_creation_record(volume_id, budget, cancellation).await?;
            let creation = observed.value.0;
            let work = observed.work;
            validate_volume_capabilities(creation.config, self.inner.capabilities)
                .map_err(|error| OperationFailure::new(error, work))?;
            Ok(FsReceipt {
                value: Volume {
                    fs: self.clone(),
                    id: volume_id,
                    config: creation.config,
                },
                work,
            })
        })
        .await;
        result.observe_on(&span, "open_volume")
    }

    /// Restores a volume authority from a fully imported immutable generation.
    pub async fn restore_volume(
        &self,
        manifest: &GenerationExportManifest,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.restore_volume",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty,
        );
        let result = crate::obs::in_span(&span, async move {
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
            let work = proof.work;
            drop(proof);
            self.publish_volume_creation(
                VolumeCreated {
                    volume_id: manifest.volume_id, config: manifest.config,
                    initial_generation_root: manifest.generation_root,
                }, Some(operation_id),
                &manifest.objects,
                0,
                proven_at,
                work,
                budget,
                cancellation,
            )
            .await
        })
        .await;
        result.observe_on(&span, "restore_volume")
    }

    /// Durably creates a volume's authority with its creation record, once
    pub(super) async fn publish_volume_creation(
        &self,
        creation: VolumeCreated, operation_id: Option<OperationId>,
        closure: &[ObjectId],
        closure_owned_bytes: u64,
        proven_at: u64,
        mut work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        let mut allocations = AllocationLedger::default();
        allocations.claim_bytes(closure_owned_bytes, 0, &mut work, budget)
            .map_err(|error| OperationFailure::new(crate::collection::handle_allocation_error(error), work))?;
        let result = {
            let mut accounting = KernelAccounting::borrowed_standalone(
                &mut allocations, &mut work, budget, cancellation,
            );
            self.publish_volume_creation_with_accounting(
                creation, operation_id, closure, proven_at, &mut accounting, cancellation, |id, commit, budget| async move {
                    let created = ({ let __authority_args = (id, commit, budget, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = self.inner.authority.create_first_record(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) });
                    let work = match &created { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
                    (created.map(|receipt| receipt.value).map_err(|failure| FsError::from(failure.error)), work)
                },
            ).await
        };
        result.map(|value| FsReceipt { value, work }).map_err(|error| OperationFailure::new(error, work))
    }

    pub(super) async fn publish_volume_creation_with_accounting<E, P, Fut>(
        &self, creation: VolumeCreated, operation_id: Option<OperationId>, closure: &[ObjectId], proven_at: u64, accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken, publication: P,
    ) -> Result<Volume<A, O>, E>
    where
        E: From<FsError> + Send,
        P: FnOnce(crate::AuthorityId, ProposedCommit, WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = (Result<bool, E>, WorkCounters)> + ProviderTask,
    {
        let VolumeCreated { volume_id, config, .. } = creation;
        accounting.check()?;
        let operation_id = match operation_id {
            Some(operation_id) => operation_id, None => {
                accounting.charge(identity_hash_work(CREATION_OPERATION_DOMAIN))?;
                OperationId::from_bytes(derived_digest(CREATION_OPERATION_DOMAIN, volume_id).as_bytes()[..16].try_into().expect("sixteen digest bytes"))
            }
        };
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_volume_created(creation, observe, allocation_error))?;
        let capacity = encoded.0.capacity() as u64;
        let commit = creation_commit_with_accounting(operation_id, encoded, accounting)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let drained = ({ let __object_args = (crate::PublicationScope::Closure { objects: closure, proven_at }, original, cancellation,); let mut __object_work = crate::AuthorityWork::native(__object_args.1, __object_args.2); let __object_result = self.inner.objects.flush_before_publish(__object_args.0, &mut __object_work).await; __object_result }).map_err(FsError::from)?;
            accounting.check()?;
            let (created, _) = publication(volume_authority_id(volume_id), commit, accounting.remaining()?).await;
            let created = created?;
            if !created { return Err(FsError::CreationRejected.into()); }
            drop(drained);
            return Ok(Volume { fs: self.clone(), id: volume_id, config });
        }
        let drained = ({ let __object_args = (crate::PublicationScope::Closure { objects: closure, proven_at }, accounting.remaining()?, cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2); let __object_result = self.inner.objects.flush_before_publish(__object_args.0, &mut __object_work).await; __object_work.receipt(__object_result) });
        let spent = match &drained { Ok(receipt) => receipt.work, Err(failure) => *failure.work, };
        merge_creation_backend_work(accounting, spent)?;
        let drained = drained.map_err(|failure| FsError::from(failure.error))?;
        accounting.check()?;
        let (created, spent) = publication(volume_authority_id(volume_id), commit, accounting.remaining()?).await;
        let merged = merge_creation_backend_work(accounting, spent);
        let mut allocation = KernelOwnedAllocation::Standalone;
        accounting.release_owned(&mut allocation, capacity)?;
        merged?;
        let created = created?;
        if !created { return Err(FsError::CreationRejected.into()); }
        drop(drained);
        Ok(Volume { fs: self.clone(), id: volume_id, config })
    }

    pub(super) async fn read_creation_record<W: FsWork>(
        &self, volume_id: VolumeId, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<(VolumeCreated, OperationId), FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                accounting.check()?;
                let records = ({ let __authority_args = (volume_authority_id(volume_id), Sequence::GENESIS, ReplayLimit { records: 1, payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES }, original, cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.3, __authority_args.4); let __authority_result = self.inner.authority.replay(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, allocation)| (value, allocation.expect("native authority allocation"))); __authority_result })?;
                return decode_first_creation_record(volume_id, &records.0, &mut accounting);
            }
            let allowance = accounting.standalone_parts().expect("standalone creation read").2;
            let records = ({ let __authority_args = (volume_authority_id(volume_id), Sequence::GENESIS, ReplayLimit { records: 1, payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES }, allowance, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = self.inner.authority.replay(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) });
            let completed = match &records { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
            *accounting.standalone_parts().expect("standalone creation read").1 = completed;
            let records = records.map_err(|failure| FsError::from(failure.error))?;
            decode_first_creation_record(volume_id, &records.value, &mut accounting)
        }.await;
        budget.finish(result, &accounting)
    }

    pub(super) async fn build_empty_generation_with_accounting(
        &self,
        volume_id: VolumeId,
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<[ObjectId; 4], FsError> {
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_file_metadata(FileMetadata::default(), observe, allocation_error))?;
        let metadata = self.put_encoded_with_accounting(ObjectKind::Metadata, encoded, accounting, cancellation).await?;
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_tree_page(&TreePage::Leaf(Vec::new()), config.limits.maximum_directory_page_entries, observe, allocation_error))?;
        let tree = self.put_encoded_with_accounting(ObjectKind::TreePage, encoded, accounting, cancellation).await?;
        accounting.charge(identity_hash_work(ROOT_FILE_DOMAIN))?;
        let root_file_id = FileId::from_bytes(derived_digest(ROOT_FILE_DOMAIN, volume_id).as_bytes()[..16].try_into().expect("sixteen digest bytes"));
        let record = FileRecord {
            file_id: root_file_id,
            kind: FileKind::Directory,
            link_count: 1,
            metadata, payload: FilePayload::Directory { entries: tree },
        };
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_file_table_leaf_records(std::slice::from_ref(&record), config.limits.maximum_directory_page_entries, None, observe, allocation_error))?;
        let file_table = self.put_encoded_with_accounting(
            ObjectKind::FileTablePage, encoded, accounting, cancellation,
        ).await?;
        let root = GenerationRoot {
            volume_id, root_file_id, file_table, parents: Vec::new(), required_features: 0,
        };
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_generation_root(&root, observe, allocation_error))?;
        let generation_root = self.put_encoded_with_accounting(
            ObjectKind::GenerationRoot, encoded, accounting, cancellation,
        ).await?;
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

/// Transfers an already admitted canonical buffer without recharging its allocation.
pub(super) fn creation_commit(
    operation_id: OperationId,
    encoded: OperationReceipt<Vec<u8>>,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> crate::MeasuredResult<OperationReceipt<(ProposedCommit, u64)>, FsError> {
    let capacity = encoded.value.capacity() as u64;
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    let mut work = encoded.work;
    let result = {
        let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
            &mut allocations, &mut work, budget, cancellation,
        );
        creation_commit_with_accounting(
            operation_id, (encoded.value, crate::kernel::allocation::KernelOwnedAllocation::Standalone),
            &mut accounting,
        )
    };
    result.map(|value| OperationReceipt { value: (value, capacity), work })
        .map_err(|error| OperationFailure::new(error, work))
}

pub(super) fn creation_fingerprint(payload: &[u8]) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CREATION_FINGERPRINT_DOMAIN);
    hasher.update(payload);
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

pub(super) fn creation_commit_with_accounting(
    operation_id: OperationId,
    encoded: (Vec<u8>, crate::kernel::allocation::KernelOwnedAllocation),
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<ProposedCommit, FsError> {
    accounting.check()?;
    let hashing = WorkCounters {
        bytes_hashed: (CREATION_FINGERPRINT_DOMAIN.len() as u64).checked_add(encoded.0.len() as u64)
            .ok_or(WorkError::Overflow)?,
        ..WorkCounters::default()
    };
    accounting.admit(hashing)?;
    let fingerprint = creation_fingerprint(&encoded.0);
    accounting.record_completed(hashing)?;
    #[cfg(not(target_arch = "wasm32"))]
    if let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) = &encoded.1 {
        if allocation.bytes() != encoded.0.capacity() as u64
            || !accounting.native_work().is_some_and(|original| allocation.work().shares_ledger(original))
        {
            return Err(FsError::HandleAllocationFailed);
        }
        let control_bytes = crate::kernel::allocation::bytes_owner_control_bytes::<crate::kernel::Owned<Vec<u8>>>()?;
        let mut control = allocation.work().claim(control_bytes, u64::from((control_bytes) != 0), crate::WorkCounters::UNCHARGED)?;
        let bytes = encoded.0.capacity() as u64;
        let mut payload = crate::kernel::Owned::new(encoded.0, encoded.1, bytes);
        let KernelOwnedAllocation::NativeOriginal(allocation) = &mut payload.allocation else {
            unreachable!("native encoded allocation was matched before moving");
        };
        allocation.absorb(&mut control)?;
        payload.bytes = allocation.bytes();
        let payload = Bytes::from_owner(payload);
        return Ok(ProposedCommit { operation_id, fingerprint, payload });
    }
    // Audited bytes 1.12: only a spare-capacity Vec allocates the private owner.
    let owner = WorkCounters {
        allocation_operations: u64::from(encoded.0.capacity() != encoded.0.len()),
        ..WorkCounters::default()
    };
    accounting.admit(owner)?;
    let payload = Bytes::from(encoded.0);
    accounting.record_completed(owner)?;
    Ok(ProposedCommit { operation_id, fingerprint, payload })
}

pub(super) fn merge_creation_backend_work(
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    spent: WorkCounters,
) -> Result<(), FsError> {
    let (allocations, work, budget, _) = accounting.standalone_parts()
        .ok_or(FsError::HandleAllocationFailed)?;
    *work = work.with_backend(spent, allocations.live_bytes())?;
    work.verify(budget)?;
    Ok(())
}

