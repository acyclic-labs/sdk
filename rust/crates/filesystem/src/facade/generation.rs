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
    let mut accounting =
        crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
    let result = crate::kernel::read_generation_root_with_accounting(
        objects,
        generation_root,
        config,
        &mut accounting,
        cancellation,
    )
    .await;
    let work = accounting.spent();
    result
        .map(|(root, _allocation)| (root, work))
        .map_err(|error| OperationFailure::new(error, work))
}

pub(super) fn generation_from_record(
    record: &DurableCommit,
    volume_id: VolumeId,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<ObjectId, FsError> {
    let mut observer = crate::kernel::OriginalReadAccounting::new(accounting)?;
    let (record_volume, root) = if record.sequence == Sequence::new(1) {
        let created =
            decode_volume_created(&record.payload, MAXIMUM_VOLUME_EVENT_BYTES, &mut |step| {
                observer.observe(step)
            })?;
        (created.volume_id, created.initial_generation_root)
    } else {
        let published = decode_published_generation(
            &record.payload,
            MAXIMUM_VOLUME_EVENT_BYTES,
            &mut |step| observer.observe(step),
        )?;
        (published.volume_id, published.generation_root)
    };
    drop(observer.into_allocation());
    if record_volume != volume_id {
        return Err(FsError::VolumeMismatch);
    }
    Ok(root)
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn observe_volume_operation(
        &self,
        volume_id: VolumeId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<DurableCommit>> {
        let mut work = crate::AuthorityWork::new(budget, cancellation);
        let result = self
            .inner
            .authority
            .find_operation(volume_authority_id(volume_id), operation_id, &mut work)
            .await
            .map_err(FsError::Authority);
        work.receipt(result)
    }
    pub(crate) async fn generation_parents<W: FsWork>(
        &self,
        volume: &Volume<A, O>,
        generation: GenerationId,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<
        (
            Vec<GenerationId>,
            crate::kernel::allocation::KernelOwnedAllocation,
        ),
        crate::workspace::WorkspaceError,
    > {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let (root, allocation) = crate::kernel::read_generation_root_with_accounting(
                &self.inner.objects,
                ObjectId {
                    kind: ObjectKind::GenerationRoot,
                    digest: generation.digest(),
                },
                volume.config,
                &mut accounting,
                cancellation,
            )
            .await
            .map_err(crate::workspace::WorkspaceError::Filesystem)?;
            let bytes = crate::foundation::usize_to_u64(
                root.parents.capacity() * size_of::<GenerationId>(),
            );
            let root = crate::kernel::Owned::new(root, allocation, bytes);
            if root.value.volume_id != volume.id {
                return Err(crate::workspace::WorkspaceError::ForeignGeneration);
            }
            Ok((root.value.parents, root.allocation))
        }
        .await;
        work.finish(result, &accounting)
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
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(ObjectId, Option<Head>), FsError> {
        if mode.access == AccessMode::ReadWrite
            && selector != GenerationSelector::Head
            && (mode.consistency != ConsistencyMode::TrackingSafe
                || mode.mutations != MutationMode::PrivateOverlay)
        {
            return Err(FsError::WritableCheckoutRequiresHead);
        }
        let head = if mode.access == AccessMode::ReadWrite || selector == GenerationSelector::Head {
            Some(
                self.resolve_head_generation(accounting, cancellation)
                    .await?,
            )
        } else {
            None
        };
        let root = match selector {
            GenerationSelector::Exact(id) => ObjectId {
                kind: ObjectKind::GenerationRoot,
                digest: id.digest(),
            },
            GenerationSelector::Head => head.as_ref().expect("head selector resolved authority").0,
        };
        Ok((root, head.map(|(_, head)| head)))
    }
    pub(super) async fn resolve_head_generation(
        &self,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(ObjectId, Head), FsError> {
        let authority_id = volume_authority_id(self.id);
        let mut authority = authority_work(accounting, cancellation)?;
        let result = self
            .fs
            .inner
            .authority
            .head(authority_id, &mut authority)
            .await;
        record_authority_work(accounting, &authority)?;
        let head = result?;
        if head.sequence == Sequence::GENESIS {
            return Err(FsError::EmptyAuthority);
        }
        let mut authority = authority_work(accounting, cancellation)?;
        let result = self
            .fs
            .inner
            .authority
            .replay(
                authority_id,
                Sequence::new(head.sequence.get().saturating_sub(1)),
                ReplayLimit {
                    records: 1,
                    payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES,
                },
                &mut authority,
            )
            .await;
        record_authority_work(accounting, &authority)?;
        let (records, _allocation) = result?;
        let latest = records.first().ok_or(FsError::InvalidAuthorityHistory)?;
        Ok((
            generation_from_head_record_value(latest, self.id, accounting)?,
            head,
        ))
    }
}

pub(super) fn generation_from_head_record_value(
    record: &DurableCommit,
    volume_id: VolumeId,
    accounting: &mut KernelAccounting<'_>,
) -> Result<ObjectId, FsError> {
    if let Ok(deleted) = decode_workspace_deleted(&record.payload, MAXIMUM_VOLUME_EVENT_BYTES) {
        if deleted != volume_id {
            return Err(FsError::VolumeMismatch);
        }
        return Err(FsError::WorkspaceDeleted);
    }
    generation_from_record(record, volume_id, accounting)
}
