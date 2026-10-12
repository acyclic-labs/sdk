use super::*;

/// Bounded reclamation result for the canonical local Stream/Objects composition.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LocalGarbageCollection {
    /// Immutable filesystem object versions examined.
    pub examined: u64,
    /// Unreachable immutable filesystem object versions removed.
    pub removed: u64,
    /// Physical body segments removed after logical reclamation.
    pub segments_removed: u64,
    /// Crash-left temporary body publications removed.
    pub temporary_files_removed: u64,
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl Fs<LocalAuthorityBackend, LocalObjectBackend> {

    /// Reclaims every stored object nothing live reaches, while the root
    pub async fn collect_local_garbage(
        &self,
        core_state: Option<&crate::LocalCoreStateStore>,
        cancellation: &CancellationToken,
    ) -> Result<LocalGarbageCollection, FsError> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.collect_local_garbage",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty
        );
        let result = crate::obs::in_span(&span, async move {
            if let Some(core_state) = core_state {
                let store_error = |error: crate::LocalCoreStateStoreError| {
                    crate::FsError::Object(crate::ObjectStoreError::Rejected(error.to_string()))
                };
                let records = core_state.workspace_records().await.map_err(store_error)?;
                let mut deleted = std::collections::BTreeSet::new();
                for workspace in records.keys() {
                    cancellation.check()?;
                    if self
                        .local_volume_deleted(workspace.volume_id(), cancellation)
                        .await?
                    {
                        deleted.insert(*workspace);
                    }
                }
                // Live descendants retain their deleted ancestors' lineage.
                let mut ancestors = std::collections::BTreeSet::new();
                for workspace in records.keys().filter(|id| !deleted.contains(id)) {
                    let mut pending = records.get(workspace).cloned().unwrap_or_default();
                    while let Some(parent) = pending.pop() {
                        if ancestors.insert(parent) {
                            pending.extend(records.get(&parent).cloned().unwrap_or_default());
                        }
                    }
                }
                for workspace in deleted.difference(&ancestors) {
                    core_state
                        .forget_workspace(*workspace)
                        .await
                        .map_err(store_error)?;
                }
                core_state.collect_lazy_nodes().await.map_err(store_error)?;
            }
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
                for (generation, config) in core_state.lazy_retained_generations().await.map_err(|error| {
                    FsError::Object(ObjectStoreError::Rejected(error.to_string()))
                })? {
                    cancellation.check()?;
                    marker.set_limits(DecodeLimits::for_volume(config));
                    marker.generation(generation).await.map_err(mark_error)?;
                }
                marker.set_limits(shadow_limits);
                for record in core_state.lazy_shadow_records().await.map_err(|error| {
                    FsError::Object(ObjectStoreError::Rejected(error.to_string()))
                })? {
                    marker.record(&record).await.map_err(mark_error)?;
                }
            }
            for held in collection.held() {
                match held {
                    crate::collection::Held::Checkout(held) => {
                        marker.set_limits(DecodeLimits::for_volume(held.config));
                        marker
                            .generation(GenerationId::new(held.base.digest))
                            .await
                            .map_err(mark_error)?;
                        marker.working(&held.working).await.map_err(mark_error)?;
                    }
                    crate::collection::Held::Record { record, config } => {
                        marker.set_limits(DecodeLimits::for_volume(config));
                        marker.record(&record).await.map_err(mark_error)?;
                    }
                }
            }
            marker.set_limits(shadow_limits);
            marker.lineage_roots().await.map_err(mark_error)?;
            let unmarked = candidates
                .into_iter()
                .filter(|(object, _)| !marker.marked().contains(object))
                .collect::<Vec<_>>();
            drop(marker);
            self.sweep_local_objects(&collecting, unmarked, cancellation)
                .await
        })
        .await;
        crate::obs::outcome_on(&span, result)
    }

    /// Every current filesystem key and its conditional-deletion validator.
    pub(super) async fn local_object_candidates(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<Vec<(ObjectId, String)>, FsError> {
        let provider = self.inner.objects.inner().inner().provider();
        let bucket = self.inner.objects.inner().inner().bucket().clone();
        let mut continuation = String::new();
        let mut versions = Vec::new();
        loop {
            cancellation.check()?;
            let page = provider
                .list(acyclic_objects::v1::wire::ListObjectsRequest {
                    bucket: Some(bucket.clone()),
                    prefix: "fs/v1/".to_owned(),
                    page_size: 256,
                    continuation_token: continuation,
                    ..Default::default()
                })
                .await
                .map_err(FsError::LocalObjectsBucket)?;
            for entry in page.entries {
                let version = entry
                    .object
                    .ok_or(FsError::Object(ObjectStoreError::Corrupt))?;
                let object = crate::distributed::object_id_from_key(&entry.object_key)
                    .ok_or(FsError::Object(ObjectStoreError::Corrupt))?;
                versions.push((object, version.etag));
            }
            continuation = page.continuation_token;
            if continuation.is_empty() {
                return Ok(versions);
            }
        }
    }

    /// Deletes `unmarked` in batches, each under the collection gate, then
    pub(super) async fn sweep_local_objects(
        &self,
        collecting: &crate::collection::Collecting,
        unmarked: Vec<(ObjectId, String)>,
        cancellation: &CancellationToken,
    ) -> Result<LocalGarbageCollection, FsError> {
        const BATCH: usize = 256;
        let provider = self.inner.objects.inner().inner().provider();
        let bucket = self.inner.objects.inner().inner().bucket().clone();
        let examined = u64::try_from(unmarked.len()).unwrap_or(u64::MAX);
        let mut candidates = unmarked
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        let mut removed = 0_u64;
        let objects = candidates.keys().copied().collect::<Vec<_>>();
        for batch in objects.chunks(BATCH) {
            cancellation.check()?;
            let (_gate, sweepable) = collecting.sweepable(batch.to_vec()).await;
            for object in sweepable {
                let Some(etag) = candidates.remove(&object) else {
                    continue;
                };
                collecting.sweeping(object);
                let deleted = provider
                    .delete(acyclic_objects::v1::wire::DeleteObjectRequest {
                        bucket: Some(bucket.clone()),
                        object_key: crate::distributed::object_key(object),
                        preconditions: Some(acyclic_objects::v1::wire::Preconditions {
                            condition: Some(
                                acyclic_objects::v1::wire::preconditions::Condition::IfMatch(etag),
                            ),
                        }),
                        mutation: None,
                    })
                    .await
                    .map_err(FsError::LocalObjectsBucket);
                // Also clear after an uncertain/partially completed sweep.
                self.inner.objects.inner().clear()?;
                if deleted?.existed {
                    removed = removed.saturating_add(1);
                }
            }
            // A cached copy of a swept object must not stand in for it.
            self.inner.objects.inner().clear()?;
        }
        let physical = provider
            .collect_garbage(u64::MAX)
            .await
            .map_err(FsError::LocalObjects)?;
        Ok(LocalGarbageCollection {
            examined,
            removed,
            segments_removed: physical.segments_removed,
            temporary_files_removed: physical.temporary_files_removed,
        })
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
                // Deleted and released since it was listed: nothing of it is
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
        let head = match ({ let __authority_args = (authority_id, WorkBudget::UNBOUNDED, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = self
            .inner
            .authority.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })
        {
            Ok(head) => head.value,
            // Released since it was listed.
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
        if let Ok(created) = crate::kernel::decode_volume_created(&first.payload, MAXIMUM_VOLUME_EVENT_BYTES, &mut |_| Ok::<_, FsError>(())) {
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
            // Its facts name generations of its volume's own history.
            return Ok(());
        }
        let retained = decode_retention_created(&first.payload, MAXIMUM_VOLUME_EVENT_BYTES)?;
        // A retention lasts as long as the workspace it serves: a fork
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
            // A delete interrupted before it released the authority.
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
            // Exactly the history as of the head read: a record appended
            for record in records
                .iter()
                .take_while(|record| record.sequence <= head.sequence)
            {
                let generation = generation_from_record(record, volume_id, &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, cancellation))?;
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
        Ok(({ let __authority_args = (authority_id, after, ReplayLimit {
                    records: count,
                    payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES.saturating_mul(u64::from(count)),
                }, WorkBudget::UNBOUNDED, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = self
            .inner
            .authority.replay(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })
            .map_err(|failure| FsError::from(failure.error))?
            .value)
    }

    /// Whether the workspace `volume_id` was deleted; one never created was
    pub(crate) async fn local_volume_deleted(
        &self,
        volume_id: VolumeId,
        cancellation: &CancellationToken,
    ) -> Result<bool, FsError> {
        match ({ let __authority_args = (volume_authority_id(volume_id), WorkBudget::UNBOUNDED, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = self
            .inner
            .authority.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })
        {
            Err(failure) if matches!(failure.error, AuthorityStoreError::Missing) => Ok(false),
            _ => self.local_volume_ended(volume_id, cancellation).await,
        }
    }

    /// Whether the workspace `volume_id` ended: deleted, or never created.
    pub(super) async fn local_volume_ended(
        &self,
        volume_id: VolumeId,
        cancellation: &CancellationToken,
    ) -> Result<bool, FsError> {
        let authority_id = volume_authority_id(volume_id);
        let head = match ({ let __authority_args = (authority_id, WorkBudget::UNBOUNDED, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = self
            .inner
            .authority.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })
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
        ({ let __authority_args = (authority_id, WorkBudget::UNBOUNDED, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = self.inner
            .authority.retire_authority(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })
            .map_err(|failure| FsError::from(failure.error))?;
        Ok(())
    }
}

/// Limits that admit every page either admits.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
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

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(super) fn mark_error(error: crate::kernel::MarkError) -> FsError {
    match error {
        crate::kernel::MarkError::Storage(error) => FsError::Object(error),
        crate::kernel::MarkError::Decode(reason) => FsError::Object(ObjectStoreError::Rejected(
            format!("a live object does not decode, so nothing was collected: {reason}"),
        )),
    }
}

