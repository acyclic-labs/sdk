use super::*;

pub(super) const MAXIMUM_VOLUME_EVENT_BYTES: u64 = 4 * 1024;

/// One independently configured volume handle reconstructed from authority.
pub struct Volume<A, O> {
    pub(crate) fs: Fs<A, O>,
    pub(crate) id: VolumeId,
    pub(crate) config: VolumeConfig,
}

impl<A, O> Clone for Volume<A, O> {
    fn clone(&self) -> Self {
        Self {
            fs: self.fs.clone(),
            id: self.id,
            config: self.config,
        }
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {

    pub(crate) async fn observe_volume_operation(
        &self,
        volume_id: VolumeId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<DurableCommit>> {
        let receipt = ({ let __authority_args = (volume_authority_id(volume_id), operation_id, budget, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = self
            .inner
            .authority.find_operation(__authority_args.0, __authority_args.1, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })
            .map_err(|failure| {
                OperationFailure::new(FsError::Authority(failure.error), *failure.work)
            })?;
        Ok(FsReceipt {
            value: receipt.value,
            work: receipt.work,
        })
    }


    pub(crate) async fn generation_parents<W: FsWork>(
        &self, volume: &Volume<A, O>, generation: GenerationId, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<(Vec<GenerationId>, KernelOwnedAllocation), WorkspaceError> {
        let mut accounting = budget.accounting(cancellation);
        let standalone = readonly_standalone_live(&accounting).is_some();
        let result = async {
            let decoded = read_generation_root_with_accounting(
                &self.inner.objects, ObjectId { kind: ObjectKind::GenerationRoot, digest: generation.digest() },
                volume.config, &mut accounting, cancellation,
            ).await.map_err(|error| if standalone { WorkspaceError::engine(error) } else { error.into() })?;
            if decoded.0.volume_id != volume.id { return Err(WorkspaceError::ForeignGeneration); }
            Ok((decoded.0.parents, decoded.1))
        }.await;
        budget.finish(result, &accounting)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Volume<A, O> {

    /// Stable volume identity.
    #[must_use]
    pub const fn id(&self) -> VolumeId {
        self.id
    }


    /// Immutable volume configuration reconstructed from authority.
    #[must_use]
    pub const fn config(&self) -> VolumeConfig {
        self.config
    }


    /// Resolves the exact generation root object and, for a writable
    pub(super) async fn resolve_checkout_generation(
        &self,
        selector: GenerationSelector,
        mode: CheckoutMode,
        work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(ObjectId, Option<Head>, WorkCounters), OperationFailure<FsError>> {
        match selector {
            GenerationSelector::Exact(generation_id) => {
                let mut work = work;
                let authority_head = if mode.access == AccessMode::ReadWrite {
                    if mode.consistency != ConsistencyMode::TrackingSafe
                        || mode.mutations != MutationMode::PrivateOverlay
                    {
                        return Err(OperationFailure::before_work(
                            FsError::WritableCheckoutRequiresHead,
                        ));
                    }
                    let resolved = self
                        .resolve_head_generation(remaining(work, budget)?, cancellation)
                        .await
                        .map_err(|failure| {
                            failure.map_with_prior_work(work, std::convert::identity)
                        })?;
                    work = add(work, resolved.2)?;
                    Some(resolved.1)
                } else {
                    None
                };
                Ok((
                    ObjectId {
                        kind: ObjectKind::GenerationRoot,
                        digest: generation_id.digest(),
                    },
                    authority_head,
                    work,
                ))
            }
            GenerationSelector::Head => {
                let resolved = self
                    .resolve_head_generation(remaining(work, budget)?, cancellation)
                    .await
                    .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
                let work = add(work, resolved.2)?;
                Ok((resolved.0, Some(resolved.1), work))
            }
        }
    }


    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn resolve_checkout_generation_with_native_work(
        &self, selector: GenerationSelector, mode: CheckoutMode, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(ObjectId, Option<Head>), FsError> {
        match selector {
            GenerationSelector::Exact(generation_id) => {
                let authority_head = if mode.access == AccessMode::ReadWrite {
                    if mode.consistency != ConsistencyMode::TrackingSafe
                        || mode.mutations != MutationMode::PrivateOverlay {
                        return Err(FsError::WritableCheckoutRequiresHead);
                    }
                    Some(self.resolve_head_generation_with_native_work(work, cancellation).await?.1)
                } else { None };
                Ok((ObjectId { kind: ObjectKind::GenerationRoot, digest: generation_id.digest(), }, authority_head))
            }
            GenerationSelector::Head => {
                let (root, head) = self.resolve_head_generation_with_native_work(work, cancellation).await?;
                Ok((root, Some(head)))
            }
        }
    }


    pub(super) async fn resolve_head_generation(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(ObjectId, Head, WorkCounters), OperationFailure<FsError>> {
        async {
            let authority_id = volume_authority_id(self.id);
            let head = ({ let __authority_args = (authority_id, budget, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = self
                .fs
                .inner
                .authority.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })
                .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
            let mut work = head.work;
            if head.value.sequence == Sequence::GENESIS {
                return Err(OperationFailure::new(FsError::EmptyAuthority, work));
            }
            let after = Sequence::new(head.value.sequence.get().saturating_sub(1));
            let records = ({ let __authority_args = (authority_id, after, ReplayLimit {
                        records: 1,
                        payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES,
                    }, remaining(work, budget)?, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = self
                .fs
                .inner
                .authority.replay(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })
                .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
            work = add(work, records.work)?;
            let latest = records
                .value
                .first()
                .ok_or_else(|| OperationFailure::new(FsError::InvalidAuthorityHistory, work))?;
            let generation_root = generation_from_head_record_value(latest, self.id, &mut KernelAccounting::borrowed_standalone(&mut crate::kernel::allocation::AllocationLedger::default(), &mut work, budget, cancellation)).map_err(|error| OperationFailure::new(error, work))?;
            Ok((generation_root, head.value, work))
        }
        .await
    }


    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn resolve_head_generation_with_native_work(
        &self, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(ObjectId, Head), FsError> {
        cancellation.check()?;
        let authority_id = volume_authority_id(self.id);
        let head = ({ let __authority_args = (authority_id, work, cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result = self.fs.inner.authority.head(__authority_args.0, &mut __authority_work).await; __authority_result })?;
        if head.sequence == Sequence::GENESIS { return Err(FsError::EmptyAuthority); }
        let records = ({ let __authority_args = (authority_id, Sequence::new(head.sequence.get().saturating_sub(1)), ReplayLimit { records: 1, payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES }, work, cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.3, __authority_args.4); let __authority_result = self.fs.inner.authority.replay(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, allocation)| (value, allocation.expect("native authority allocation"))); __authority_result })?;
        let latest = records.0.first().ok_or(FsError::InvalidAuthorityHistory)?;
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        Ok((generation_from_head_record_value(latest, self.id, &mut KernelAccounting::native(work, cancellation))?, head))
    }
}

pub(super) fn generation_from_head_record_value(record: &DurableCommit, volume_id: VolumeId, accounting: &mut KernelAccounting<'_>) -> Result<ObjectId, FsError> {
    if let Ok(deleted) = decode_workspace_deleted(&record.payload, MAXIMUM_VOLUME_EVENT_BYTES) {
        if deleted != volume_id { return Err(FsError::VolumeMismatch); }
        return Err(FsError::WorkspaceDeleted);
    }
    generation_from_record(record, volume_id, accounting)
}

pub(super) fn record_logical_bytes(record: FileRecord) -> u64 {
    match record.payload {
        FilePayload::InlineRegular(bytes) => {
            u64::try_from(bytes.as_bytes().len()).unwrap_or(u64::MAX)
        }
        FilePayload::Regular { logical_bytes, .. } => logical_bytes,
        FilePayload::SymbolicLink { target_bytes, .. } => target_bytes,
        _ => 0,
    }
}

pub(crate) async fn read_generation_root<O: AsyncObjectStore>(
    objects: &O,
    generation_root: ObjectId,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(GenerationRoot, WorkCounters), OperationFailure<FsError>> {
    let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
    let result = crate::kernel::read_generation_root_with_accounting(
        objects, generation_root, config, &mut accounting, cancellation,
    ).await;
    let work = accounting.spent();
    result.map(|(root, ownership)| {
        match ownership {
            crate::kernel::allocation::KernelOwnedAllocation::Standalone => (),
            #[cfg(not(target_arch = "wasm32"))]
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(_) => unreachable!(
                "standalone generation reading cannot create native custody",
            ),
        }
        (root, work)
    }).map_err(|error| OperationFailure::new(error, work))
}

pub(super) fn generation_from_record(
    record: &DurableCommit,
    volume_id: VolumeId,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<ObjectId, FsError> {
    let mut observer = crate::kernel::OriginalReadAccounting::new(accounting)?;
    let item = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
    observer.admit(item)?;
    let initial = record.sequence == Sequence::new(1);
    observer.record_completed(item)?;
    let (record_volume, generation_root) = {
        let mut observe = |step| observer.observe(step);
        if initial {
            let creation = crate::kernel::decode_volume_created(&record.payload, MAXIMUM_VOLUME_EVENT_BYTES, &mut observe)?;
            (creation.volume_id, creation.initial_generation_root)
        } else {
            let publication = crate::kernel::decode_published_generation(
                &record.payload, MAXIMUM_VOLUME_EVENT_BYTES, &mut observe,
            )?;
            (publication.volume_id, publication.generation_root)
        }
    };
    drop(observer.into_allocation());
    if record_volume != volume_id {
        return Err(FsError::VolumeMismatch);
    }
    Ok(generation_root)
}

