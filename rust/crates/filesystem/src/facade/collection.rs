#[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
use super::*;

/// Bounded reclamation result for the canonical local Stream/Objects composition.
#[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LocalGarbageCollection {
    /// Immutable filesystem object versions examined.
    pub examined: u64,
    /// Unreachable immutable filesystem object versions removed.
    pub removed: u64,
}

/// Limits that admit every page either admits.
#[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
pub(super) fn widest_limits(left: DecodeLimits, right: DecodeLimits) -> DecodeLimits {
    DecodeLimits {
        maximum_object_bytes: left.maximum_object_bytes.max(right.maximum_object_bytes),
        maximum_name_bytes: left.maximum_name_bytes.max(right.maximum_name_bytes),
        maximum_page_items: left.maximum_page_items.max(right.maximum_page_items),
        maximum_page_bytes: left.maximum_page_bytes.max(right.maximum_page_bytes),
        maximum_page_height: left.maximum_page_height.max(right.maximum_page_height),
        maximum_visited_pages: left.maximum_visited_pages.max(right.maximum_visited_pages),
    }
}

#[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
pub(super) fn mark_error(error: crate::kernel::MarkError) -> FsError {
    match error {
        crate::kernel::MarkError::Storage(error) => FsError::Object(error),
        crate::kernel::MarkError::Decode(reason) => FsError::Object(ObjectStoreError::Rejected(
            format!("a live object does not decode, so nothing was collected: {reason}"),
        )),
    }
}

#[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
impl Fs<LocalAuthorityBackend, LocalObjectBackend> {
    /// Reclaims every stored object nothing live reaches, while the root
    pub async fn collect_local_garbage(
        &self,
        core_state: Option<&crate::LocalCoreStateStore>,
        cancellation: &CancellationToken,
    ) -> Result<LocalGarbageCollection, FsError> {
        (async move {
            let collection = Arc::clone(self.inner.objects.collection().ok_or(FsError::Object(
                ObjectStoreError::Rejected("the local object store does not collect".to_owned()),
            ))?);
            let collecting = collection.begin().await;
            let candidates = self.local_object_candidates(cancellation).await?;
            let mut marker = crate::kernel::Marker::new(&self.inner.objects, cancellation);
            let shadow_limits = self
                .mark_local_authorities(&mut marker, cancellation)
                .await?;
            #[cfg(feature = "s3-http")]
            {
                let multipart = crate::active_s3_multipart_objects(
                    self.inner.authority.provider().as_ref(),
                    &self.inner.objects,
                    crate::FilesystemS3Limits::default(),
                    crate::S3MultipartRetentionLimits::default(),
                    WorkBudget::UNBOUNDED,
                    cancellation,
                )
                .await
                .map_err(|_| {
                    FsError::Object(ObjectStoreError::Rejected(
                        "active S3 multipart retention scan failed".to_owned(),
                    ))
                })?;
                marker.keep(multipart.value);
            }
            if let Some(core_state) = core_state {
                marker.set_limits(shadow_limits);
                for record in core_state.lazy_shadow_records().await.map_err(|error| {
                    FsError::Object(ObjectStoreError::Rejected(error.to_string()))
                })? {
                    marker.record(&record).await.map_err(mark_error)?;
                }
            }
            for held in collection.held() {
                match held {
                    crate::collection::Held::Checkout {
                        base,
                        working,
                        config,
                        ..
                    } => {
                        marker.set_limits(DecodeLimits::for_volume(config));
                        marker
                            .generation(GenerationId::new(base.digest))
                            .await
                            .map_err(mark_error)?;
                        marker.working(&working).await.map_err(mark_error)?;
                    }
                    crate::collection::Held::Record { record, config } => {
                        marker.set_limits(DecodeLimits::for_volume(config));
                        marker.record(&record).await.map_err(mark_error)?;
                    }
                    crate::collection::Held::Generation { root, config } => {
                        marker.set_limits(DecodeLimits::for_volume(config));
                        marker
                            .generation(GenerationId::new(root.digest))
                            .await
                            .map_err(mark_error)?;
                    }
                }
            }
            marker.set_limits(shadow_limits);
            marker.lineage_roots().await.map_err(mark_error)?;
            let marked = marker
                .marked()
                .iter()
                .map(|object| object.digest)
                .collect::<std::collections::HashSet<_>>();
            let unmarked = candidates
                .into_iter()
                .filter(|digest| !marked.contains(digest))
                .collect::<Vec<_>>();
            drop(marker);
            self.sweep_local_objects(&collecting, unmarked, cancellation)
                .await
        })
        .await
    }
    /// Every stored blob digest.
    pub(super) async fn local_object_candidates(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<Vec<crate::Digest>, FsError> {
        let provider = self.inner.objects.inner().inner().provider();
        let mut after = None;
        let mut digests = Vec::new();
        loop {
            cancellation.check()?;
            let page = provider
                .list(after, 1024)
                .await
                .map_err(|error| FsError::Object(ObjectStoreError::LogicalObjects(error)))?;
            digests.extend(
                page.digests
                    .iter()
                    .map(|digest| crate::Digest::from_bytes(*digest.as_bytes())),
            );
            match page.next {
                Some(next) => after = Some(next),
                None => return Ok(digests),
            }
        }
    }
    /// Deletes `unmarked` in batches, each under the collection gate.
    pub(super) async fn sweep_local_objects(
        &self,
        collecting: &crate::collection::Collecting,
        unmarked: Vec<crate::Digest>,
        cancellation: &CancellationToken,
    ) -> Result<LocalGarbageCollection, FsError> {
        const BATCH: usize = 256;
        let provider = self.inner.objects.inner().inner().provider();
        let examined = u64::try_from(unmarked.len()).unwrap_or(u64::MAX);
        let mut removed = 0_u64;
        for batch in unmarked.chunks(BATCH) {
            cancellation.check()?;
            let (_gate, sweepable) = collecting.sweepable(batch.to_vec()).await;
            for digest in sweepable {
                collecting.sweeping(digest);
                let deleted = provider
                    .delete(acyclic_objects::Digest::from_bytes(digest.into_bytes()))
                    .await
                    .map_err(|error| FsError::Object(ObjectStoreError::LogicalObjects(error)));
                self.inner.objects.inner().clear()?;
                if deleted? == acyclic_objects::Delete::Deleted {
                    removed = removed.saturating_add(1);
                }
            }
            // A cached copy of a swept object must not stand in for it.
            self.inner.objects.inner().clear()?;
        }
        Ok(LocalGarbageCollection { examined, removed })
    }
    /// Marks every live generation of every authority, releasing the
    pub(super) async fn mark_local_authorities(
        &self,
        marker: &mut crate::kernel::Marker<'_, LocalObjectBackend>,
        cancellation: &CancellationToken,
    ) -> Result<DecodeLimits, FsError> {
        let mut widest = DecodeLimits::for_volume(VolumeConfig::portable(Lifecycle::Durable));
        let authorities = self
            .inner
            .authority
            .authorities(u32::MAX - 1)
            .await
            .map_err(FsError::from)?;
        for authority_id in authorities {
            match self
                .mark_local_authority(marker, authority_id, &mut widest, cancellation)
                .await
            {
                Ok(()) | Err(FsError::WorkspaceDeleted) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(widest)
    }
    /// Marks one authority's live generations; see
    pub(super) async fn mark_local_authority(
        &self,
        marker: &mut crate::kernel::Marker<'_, LocalObjectBackend>,
        authority_id: crate::foundation::AuthorityId,
        widest: &mut DecodeLimits,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        cancellation.check()?;
        let head = match {
            let sdk_args = (authority_id, WorkBudget::UNBOUNDED, cancellation);
            let sdk_store = &self.inner.authority;
            async move {
                let mut sdk_work = crate::AuthorityWork::new(sdk_args.1, sdk_args.2);
                let result = sdk_store.head(sdk_args.0, &mut sdk_work).await;
                sdk_work.receipt(result)
            }
        }
        .await
        {
            Ok(head) => head.value,
            Err(failure) if matches!(failure.error, AuthorityStoreError::Retired) => return Ok(()),
            Err(failure) => return Err(failure.error.into()),
        };
        if head.sequence == Sequence::GENESIS {
            return Ok(());
        }
        let first = self
            .local_records(authority_id, Sequence::GENESIS, 1, cancellation)
            .await?
            .into_iter()
            .next()
            .ok_or(FsError::InvalidAuthorityHistory)?;
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED,
            cancellation,
        );
        if let Ok((created, _allocation)) =
            crate::kernel::decode_owned(&mut accounting, |observe| {
                decode_volume_created(&first.payload, MAXIMUM_VOLUME_EVENT_BYTES, &mut |step| {
                    observe(step)
                })
            })
        {
            if volume_authority_id(created.volume_id) != authority_id {
                return Err(FsError::VolumeMismatch);
            }
            let limits = DecodeLimits::for_volume(created.config);
            *widest = widest_limits(*widest, limits);
            marker.set_limits(limits);
            self.mark_local_volume(marker, authority_id, created.volume_id, head, cancellation)
                .await?;
            return Ok(());
        }
        #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
        if let Ok(source_volume) = decode_source_volume(&first.payload, MAXIMUM_VOLUME_EVENT_BYTES)
        {
            if source_authority_id(source_volume) != authority_id {
                return Err(FsError::VolumeMismatch);
            }
            return Ok(());
        }
        let retained = decode_retention_created(&first.payload, MAXIMUM_VOLUME_EVENT_BYTES)?;
        let owner = match retained.kind {
            RetentionKind::ForkBase => hex::decode(&retained.label)
                .ok()
                .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
                .map(VolumeId::from_bytes)
                .ok_or(FsError::InvalidAuthorityHistory)?,
            RetentionKind::Checkpoint | RetentionKind::Pin => retained.volume_id,
        };
        if self.local_volume_ended(owner, cancellation).await? {
            self.retire_local_authority(authority_id, cancellation)
                .await?;
            return Ok(());
        }
        let limits = DecodeLimits::for_volume(retained.config);
        *widest = widest_limits(*widest, limits);
        marker.set_limits(limits);
        marker
            .generation(GenerationId::new(retained.generation_root.digest))
            .await
            .map_err(mark_error)?;
        Ok(())
    }
    /// Marks every generation in one volume's history, or releases the
    pub(super) async fn mark_local_volume(
        &self,
        marker: &mut crate::kernel::Marker<'_, LocalObjectBackend>,
        authority_id: crate::foundation::AuthorityId,
        volume_id: VolumeId,
        head: Head,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        const PAGE: u32 = 256;
        let latest = self
            .local_records(
                authority_id,
                Sequence::new(head.sequence.get().saturating_sub(1)),
                1,
                cancellation,
            )
            .await?;
        if let Some(latest) = latest.first()
            && let Ok(deleted) =
                decode_workspace_deleted(&latest.payload, MAXIMUM_VOLUME_EVENT_BYTES)
        {
            if deleted != volume_id {
                return Err(FsError::VolumeMismatch);
            }
            return self
                .retire_local_authority(authority_id, cancellation)
                .await;
        }
        let mut after = Sequence::GENESIS;
        while after < head.sequence {
            let records = self
                .local_records(authority_id, after, PAGE, cancellation)
                .await?;
            let Some(last) = records.last() else {
                return Err(FsError::InvalidAuthorityHistory);
            };
            after = last.sequence;
            for record in records
                .iter()
                .take_while(|record| record.sequence <= head.sequence)
            {
                let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
                    WorkBudget::UNBOUNDED,
                    cancellation,
                );
                let generation = generation_from_record(record, volume_id, &mut accounting)?;
                marker
                    .generation(GenerationId::new(generation.digest))
                    .await
                    .map_err(mark_error)?;
            }
        }
        Ok(())
    }
    /// Up to `count` records of one authority after `after`.
    pub(super) async fn local_records(
        &self,
        authority_id: crate::foundation::AuthorityId,
        after: Sequence,
        count: u32,
        cancellation: &CancellationToken,
    ) -> Result<Vec<crate::foundation::DurableCommit>, FsError> {
        Ok({
            let sdk_args = (
                authority_id,
                after,
                ReplayLimit {
                    records: count,
                    payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES.saturating_mul(u64::from(count)),
                },
                WorkBudget::UNBOUNDED,
                cancellation,
            );
            let sdk_store = &self.inner.authority;
            async move {
                let mut sdk_work = crate::AuthorityWork::new(sdk_args.3, sdk_args.4);
                let result = sdk_store
                    .replay(sdk_args.0, sdk_args.1, sdk_args.2, &mut sdk_work)
                    .await
                    .map(|(value, _allocation)| value);
                sdk_work.receipt(result)
            }
        }
        .await
        .map_err(|failure| FsError::from(failure.error))?
        .value)
    }

    /// Whether the workspace `volume_id` ended: deleted, or never created.
    pub(super) async fn local_volume_ended(
        &self,
        volume_id: VolumeId,
        cancellation: &CancellationToken,
    ) -> Result<bool, FsError> {
        let authority_id = volume_authority_id(volume_id);
        let head = match {
            let sdk_args = (authority_id, WorkBudget::UNBOUNDED, cancellation);
            let sdk_store = &self.inner.authority;
            async move {
                let mut sdk_work = crate::AuthorityWork::new(sdk_args.1, sdk_args.2);
                let result = sdk_store.head(sdk_args.0, &mut sdk_work).await;
                sdk_work.receipt(result)
            }
        }
        .await
        {
            Ok(head) => head.value,
            Err(failure)
                if matches!(
                    failure.error,
                    AuthorityStoreError::Retired | AuthorityStoreError::Missing
                ) =>
            {
                return Ok(true);
            }
            Err(failure) => return Err(failure.error.into()),
        };
        if head.sequence == Sequence::GENESIS {
            return Ok(false);
        }
        let latest = self
            .local_records(
                authority_id,
                Sequence::new(head.sequence.get().saturating_sub(1)),
                1,
                cancellation,
            )
            .await?;
        Ok(latest.first().is_some_and(|record| {
            decode_workspace_deleted(&record.payload, MAXIMUM_VOLUME_EVENT_BYTES)
                .is_ok_and(|deleted| deleted == volume_id)
        }))
    }
    pub(super) async fn retire_local_authority(
        &self,
        authority_id: crate::foundation::AuthorityId,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        self.inner
            .authority
            .retire_authority(
                authority_id,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, cancellation),
            )
            .await
            .map_err(FsError::from)?;
        Ok(())
    }
}
