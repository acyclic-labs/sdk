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
        maximum_page_items: left.maximum_page_items.max(right.maximum_page_items),
        maximum_page_bytes: left.maximum_page_bytes.max(right.maximum_page_bytes),
        maximum_page_height: left.maximum_page_height.max(right.maximum_page_height),
    }
}

/// Stops a collection after `step`, as a crash there would.
#[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
fn crash(_epoch: &crate::logical_objects::Epoch, _step: Step) -> Result<(), FsError> {
    #[cfg(test)]
    if _epoch.hooks.crashes(_step) {
        return Err(FsError::Object(ObjectStoreError::Rejected(
            "crashed".to_owned(),
        )));
    }
    Ok(())
}

#[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
use crate::logical_objects::epoch::Step;

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
    /// stays open for publications.
    ///
    /// A first pass sweeps what no epoch retains. Only when that finds
    /// unreachable objects an epoch still retains does a second pass open the
    /// next epoch: a quiet account's collection writes nothing.
    pub async fn collect_local_garbage(
        &self,
        core_state: Option<&crate::LocalCoreStateStore>,
        cancellation: &CancellationToken,
    ) -> Result<LocalGarbageCollection, FsError> {
        let (first, retained) = self
            .collect_local_pass(core_state, false, cancellation)
            .await?;
        if retained == 0 {
            return Ok(first);
        }
        let (second, _) = self
            .collect_local_pass(core_state, true, cancellation)
            .await?;
        Ok(LocalGarbageCollection {
            examined: second.examined,
            removed: first.removed.saturating_add(second.removed),
        })
    }

    /// One mark and sweep. With `advance`, it opens the next epoch once no
    /// publication is in flight, retains everything marked under it and
    /// releases the earlier epochs before sweeping. Returns how many
    /// unreachable objects some root still retains.
    ///
    /// Opening an epoch costs one retained index key per live object; a
    /// collection pays it only when there is retained garbage to reclaim.
    pub(crate) async fn collect_local_pass(
        &self,
        core_state: Option<&crate::LocalCoreStateStore>,
        advance: bool,
        cancellation: &CancellationToken,
    ) -> Result<(LocalGarbageCollection, u64), FsError> {
        (async move {
            let collection = Arc::clone(self.inner.objects.collection().ok_or(FsError::Object(
                ObjectStoreError::Rejected("the local object store does not collect".to_owned()),
            ))?);
            // Before the epoch is read: a concurrent collection never
            // persists or switches an epoch computed from a stale one.
            let mut running = collection.run().await;
            let logical = self.inner.objects.inner().inner();
            let epoch = Arc::clone(logical.epoch());
            let previous = epoch.current();
            let next = previous
                .checked_add(1)
                .ok_or(FsError::Work(WorkError::Overflow))?;
            if advance {
                // A restart resumes `next` before anything retains under it.
                running = epoch
                    .persist(next, running, self.clone())
                    .await
                    .map_err(FsError::LocalRoot)?;
                crash(&epoch, Step::Persisted)?;
            }
            let collecting = collection
                .begin_then(running, || {
                    if advance {
                        epoch.switch(next);
                    }
                })
                .await;
            if advance {
                crash(&epoch, Step::Switched)?;
            }
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
                            .generation(GenerationId::from_root(base))
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
                            .generation(GenerationId::from_root(root))
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
            // A marked object that is not stored is still private to its
            // holder; the hold protects it until its publication retains it.
            let (live, unmarked) = candidates
                .into_iter()
                .partition::<Vec<_>, _>(|digest| marked.contains(digest));
            drop(marker);
            if advance {
                self.retain_marked(logical.provider(), next, live, cancellation)
                    .await?;
                crash(&epoch, Step::Retained)?;
                // Every epoch an interrupted collection left behind, then `previous`.
                for released in epoch.unreleased()..next {
                    cancellation.check()?;
                    logical
                        .provider()
                        .release(&crate::logical_objects::Epoch::root(released))
                        .await
                        .map_err(|error| {
                            FsError::Object(ObjectStoreError::LogicalObjects(error))
                        })?;
                }
                epoch.released_before(next);
                #[cfg(test)]
                epoch.hooks.released.notify_one();
                crash(&epoch, Step::Released)?;
            }
            self.sweep_local_objects(&collecting, unmarked, cancellation)
                .await
        })
        .await
    }

    /// The epoch this root's publications retain under.
    #[cfg(test)]
    pub(crate) fn local_epoch(&self) -> Arc<crate::logical_objects::Epoch> {
        Arc::clone(self.inner.objects.inner().inner().epoch())
    }

    /// The account every volume of this root stores its blobs in.
    #[cfg(test)]
    pub(crate) fn local_account(&self) -> Arc<acyclic_objects::LocalObjects> {
        Arc::clone(self.inner.objects.inner().inner().provider())
    }

    /// This root's collection state.
    #[cfg(test)]
    pub(crate) fn local_collection(&self) -> Arc<crate::collection::Collection> {
        Arc::clone(
            self.inner
                .objects
                .collection()
                .expect("a local root collects"),
        )
    }

    /// Retains the stored live objects under epoch `next`, one bounded batch at a time.
    async fn retain_marked(
        &self,
        provider: &Arc<acyclic_objects::LocalObjects>,
        next: u64,
        live: Vec<crate::Digest>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        const BATCH: usize = 4096;
        let root = crate::logical_objects::Epoch::root(next);
        let live = live
            .into_iter()
            .map(|digest| acyclic_objects::Digest::from_bytes(digest.into_bytes()))
            .collect::<Vec<_>>();
        for batch in live.chunks(BATCH) {
            cancellation.check()?;
            provider
                .retain(&root, batch.to_vec())
                .await
                .map_err(|error| FsError::Object(ObjectStoreError::LogicalObjects(error)))?;
        }
        Ok(())
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
    /// Deletes `unmarked` in batches, each under the collection gate, in
    /// the order [`crate::collection::Worklist`] sets: a dead graph of any
    /// depth goes in one pass. Returns how many objects a root still retains.
    pub(super) async fn sweep_local_objects(
        &self,
        collecting: &crate::collection::Collecting,
        unmarked: Vec<crate::Digest>,
        cancellation: &CancellationToken,
    ) -> Result<(LocalGarbageCollection, u64), FsError> {
        const BATCH: usize = 256;
        let provider = self.inner.objects.inner().inner().provider();
        let object_error = |error| FsError::Object(ObjectStoreError::LogicalObjects(error));
        let examined = u64::try_from(unmarked.len()).unwrap_or(u64::MAX);

        let mut removed = 0_u64;
        let mut work = crate::collection::Worklist::new(unmarked);
        loop {
            let batch = work.next(BATCH);
            if batch.is_empty() {
                let retained = u64::try_from(work.retained()).unwrap_or(u64::MAX);
                return Ok((LocalGarbageCollection { examined, removed }, retained));
            }
            cancellation.check()?;
            let (_gate, sweepable) = collecting.sweepable(batch).await;
            for digest in sweepable {
                let object = acyclic_objects::Digest::from_bytes(digest.into_bytes());
                // The references a deletion releases, read while it still exists.
                let children = match provider.head(object).await {
                    Ok(head) => head.references,
                    Err(error) if error.code == acyclic_objects::wire::ErrorCode::NotFound => {
                        continue;
                    }
                    Err(error) => return Err(object_error(error)),
                };
                collecting.sweeping();
                let deleted = provider.delete(object).await.map_err(object_error);
                self.inner.objects.inner().clear()?;
                match deleted?.outcome {
                    acyclic_objects::Delete::Deleted => {
                        removed = removed.saturating_add(1);
                        work.deleted(
                            children
                                .iter()
                                .map(|child| crate::Digest::from_bytes(*child.as_bytes())),
                        );
                    }
                    acyclic_objects::Delete::Referenced => work.refused(digest),
                    acyclic_objects::Delete::Absent => {}
                }
            }
        }
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
                decode_volume_created(&first.payload, first.payload.len() as u64, &mut |step| {
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
        if let Ok(source_volume) = decode_source_volume(&first.payload, first.payload.len() as u64)
        {
            if source_authority_id(source_volume) != authority_id {
                return Err(FsError::VolumeMismatch);
            }
            return Ok(());
        }
        let retained = decode_retention_created(&first.payload, first.payload.len() as u64)?;
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
            .generation(GenerationId::from_root(retained.generation_root))
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
                decode_workspace_deleted(&latest.payload, latest.payload.len() as u64)
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
                    .generation(GenerationId::from_root(generation))
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
        let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, cancellation);
        let maximum_payload_bytes = work.accounting().remaining()?.authority_bytes_read;
        let result = self
            .inner
            .authority
            .replay(
                authority_id,
                after,
                ReplayLimit {
                    records: count,
                    payload_bytes: maximum_payload_bytes,
                },
                &mut work,
            )
            .await
            .map(|(records, _allocation)| records);
        Ok(work
            .receipt(result)
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
            decode_workspace_deleted(&record.payload, record.payload.len() as u64)
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
