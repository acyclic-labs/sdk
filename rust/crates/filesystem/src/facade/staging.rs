use super::*;

/// Authenticated content staged from a bounded stream without materializing the
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StagedContent {
    /// Root of the immutable chunk/index closure.
    pub(super) root: ObjectId,
    /// Exact logical source length.
    pub(super) logical_bytes: u64,
}

impl StagedContent {

    #[cfg(feature = "s3-http")]
    pub(crate) const fn from_canonical_parts(root: ObjectId, logical_bytes: u64) -> Self {
        Self {
            root,
            logical_bytes,
        }
    }


    /// Authenticated immutable blob-root identity created by this SDK.
    #[must_use]
    pub const fn root(&self) -> ObjectId {
        self.root
    }


    /// Exact staged logical byte length authenticated by the blob build.
    #[must_use]
    pub const fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {


    pub(crate) async fn make_records_durable(
        &self,
        config: VolumeConfig,
        records: &[FileRecord],
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<crate::PublicationHold>, FsError> {
        let proven_at = self.inner.objects.collection_sweeps();
        let (closure, work) = prove_record_closure_async(
            &self.inner.objects, records, closure_limits(config), budget, cancellation,
        ).await.map_err(|failure| failure.map_with_prior_work(WorkCounters::default(), FsError::from))?;
        let flushed = self.inner.objects.flush_before_publish(
            crate::PublicationScope::Closure { objects: &closure, proven_at }, remaining(work, budget)?, cancellation,
        ).await.map_err(|failure| failure.map_with_prior_work(work, FsError::from))?;
        let total = work.checked_add(flushed.work).map_err(|error| OperationFailure::new(FsError::from(error), work))?;
        Ok(FsReceipt { value: flushed.value, work: total })
    }


    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn make_records_durable_with_native_work(
        &self, config: VolumeConfig, records: &[FileRecord], original: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::PublicationHold, FsError> {
        original.admit(WorkCounters::UNCHARGED)?;
        cancellation.check()?;
        let proven_at = self.inner.objects.collection_sweeps();
        let (value, allocation) = prove_record_closure_with_native_work(
            &self.inner.objects, records, closure_limits(config), original, cancellation,
        ).await?;
        let closure = (value, allocation);
        self.inner.objects.flush_before_publish_with_native_work(
            crate::PublicationScope::Closure { objects: &closure.0, proven_at }, original, cancellation,
        ).await.map_err(FsError::from)
    }


    /// Holds the actual generation closure across an external durable publication.
    pub(crate) async fn make_generation_durable(
        &self, volume: &Volume<A, O>, generation: GenerationId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<crate::PublicationHold>, WorkspaceError> {

        let generation_root = ObjectId { kind: ObjectKind::GenerationRoot, digest: generation.digest() };
        let proven_at = self.inner.objects.collection_sweeps();
        let proof = prove_generation_closure(
            &self.inner.objects, generation_root, closure_limits(volume.config), budget, cancellation,
        ).await.map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::engine))?;
        let work = proof.work;
        if proof.root.volume_id != volume.id {
            return Err(OperationFailure::new(WorkspaceError::ForeignGeneration, work));
        }
        drop(proof.root);
        let live = (proof.objects.capacity() as u64).checked_mul(size_of::<ObjectId>() as u64)
            .ok_or_else(|| OperationFailure::new(WorkspaceError::Work(WorkError::Overflow), work))?;
        let flushed = self.inner.objects.flush_before_publish(
            crate::PublicationScope::Closure { objects: &proof.objects, proven_at },
            remaining_owned(work, budget, live).map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?,
            cancellation,
        ).await.map_err(|failure| merge_simultaneous_failure(work, *failure.work, live, FsError::Object(failure.error))
            .map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        let work = merge_simultaneous_work(work, flushed.work, live, budget)
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        Ok(FsReceipt { value: flushed.value, work })
    }


    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn make_generation_durable_with_native_work(
        &self, volume: &Volume<A, O>, generation: GenerationId, original: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<crate::PublicationHold, (WorkspaceError, Option<NativeAllocation>)> {
        let result = async {
            let mut accounting = KernelAccounting::native(original, cancellation);
            accounting.check()?;
            if !self.same_deployment(&volume.fs) { return Err(WorkspaceError::ForeignGeneration); }
            let publication = self.retention_publication_with_accounting(
                volume, ObjectId { kind: ObjectKind::GenerationRoot, digest: generation.digest() }, &mut accounting, cancellation,
            ).await?;
            let held = self.inner.objects.flush_before_publish_with_native_work(
                crate::PublicationScope::Closure {
                    objects: &publication.value.objects, proven_at: publication.value.proven_at,
                }, original, cancellation,
            ).await.map_err(FsError::from)?;
            publication.retire(&mut accounting)?;
            Ok(held)
        }.await;
        result.map_err(|error| (error, None))
    }


    pub(crate) async fn restore_workspace_generation(
        &self,
        workspace: &Volume<A, O>,
        generation: GenerationId,
        expected: GenerationId,
        operation_id: OperationId,
        permit: PublicationPermit,
    ) -> Result<crate::WorkspaceRestore<GenerationId>, WorkspaceError> {
        let cancellation = CancellationToken::new();
        if let Some(recorded) = self
            .workspace_operation_generation(workspace, operation_id,
                &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation), &cancellation)
            .await?.0.map(|(generation, record)| { drop(record); generation })
        {
            return Ok(if recorded == generation {
                crate::WorkspaceRestore::AlreadyRestored(recorded)
            } else {
                crate::WorkspaceRestore::IdempotencyConflict
            });
        }
        let (current, authority_head, _) = workspace
            .resolve_head_generation(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(WorkspaceError::engine)?;
        let current = GenerationId::new(current.digest);
        if current != expected {
            return Ok(crate::WorkspaceRestore::Stale(current));
        }
        if current == generation {
            return Ok(crate::WorkspaceRestore::Current(current));
        }
        let generation_root = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: generation.digest(),
        };
        let (root, _) = read_generation_root(
            &self.inner.objects,
            generation_root,
            workspace.config,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;
        if root.volume_id != workspace.id {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let publication = publish_generation_async_with_permit(
            &self.inner.objects,
            &self.inner.authority,
            PublishGenerationRequest {
                authority_id: volume_authority_id(workspace.id),
                volume_id: workspace.id,
                epoch: authority_head.epoch,
                expected: authority_head,
                operation_id,
                generation_root,
            },
            PublicationIntent::guarded(None, permit),
            closure_limits(workspace.config),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;
        Ok(match publication.outcome {
            AppendOutcome::Committed(_) => {
                crate::WorkspaceRestore::Restored(publication.proof.generation_id)
            }
            AppendOutcome::AlreadyCommitted(_) => {
                crate::WorkspaceRestore::AlreadyRestored(
                    publication.proof.generation_id,
                )
            }
            AppendOutcome::Conflict { .. } => {
                let (actual, _, _) = workspace
                    .resolve_head_generation(WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(WorkspaceError::engine)?;
                crate::WorkspaceRestore::Stale(GenerationId::new(actual.digest))
            }
            AppendOutcome::Fenced { .. } => crate::WorkspaceRestore::Fenced,
            AppendOutcome::IdempotencyConflict { .. } => {
                crate::WorkspaceRestore::IdempotencyConflict
            }
        })
    }


    /// Executes one active local-residency prediction through this filesystem's
    pub async fn execute_residency(
        &self,
        speculator: &crate::ResidencySpeculator,
        permit: crate::ResidencyPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<u64> {
        crate::execute_residency(
            speculator,
            &self.inner.objects,
            permit,
            budget,
            cancellation,
        )
        .await
    }


    pub(super) async fn put_encoded_with_accounting(
        &self, kind: ObjectKind, mut encoded: (Vec<u8>, KernelOwnedAllocation), accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let allocation = match &mut encoded.1 {
                KernelOwnedAllocation::NativeOriginal(allocation) => allocation, KernelOwnedAllocation::Standalone =>
                    return Err(FsError::HandleAllocationFailed),
            };
            let hashed = HashedObject::from_native_vec(kind, &mut encoded.0, allocation, original, cancellation)?;
            let object = hashed.object_id();
            self.inner.objects.put_hashed_with_native_work(hashed, original, cancellation).await?;
            return Ok(object);
        }
        let capacity = encoded.0.capacity() as u64;
        let hashing = WorkCounters {
            bytes_hashed: (encoded.0.len() as u64).checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
                .ok_or(WorkError::Overflow)?,..WorkCounters::UNCHARGED
        };
        let owner = WorkCounters {
            allocation_operations: u64::from(encoded.0.capacity() != encoded.0.len()),..WorkCounters::UNCHARGED
        };
        accounting.admit(owner)?;
        accounting.admit(hashing)?;
        let (payload, allocation) = encoded;
        let bytes = acyclic_stream::OwnedBytes::from(payload);
        if let Err(error) = accounting.record_completed(owner) { drop(bytes); drop(allocation); return Err(error); }
        drop(allocation);
        let hashed = HashedObject::new(kind, bytes);
        accounting.record_completed(hashing)?;
        let object = hashed.object_id();
        let receipt = self.inner.objects.put_hashed(hashed, accounting.remaining()?, cancellation).await;
        let spent = match &receipt { Ok(receipt) => receipt.work, Err(failure) => *failure.work, };
        let (allocations, work, budget, _) = accounting.standalone_parts().ok_or(FsError::HandleAllocationFailed)?;
        *work = work.with_backend(spent, allocations.live_bytes())?;
        allocations.release(capacity).map_err(|error| error.into_work_or(FsError::PendingMutationAllocationFailed))?;
        work.verify(budget)?;
        receipt.map_err(|failure| FsError::from(failure.error))?;
        Ok(object)
    }


    /// Includes retained caller buffers across both canonical encoding and storage.
    pub(super) async fn put_generation_root(
        &self, root: &GenerationRoot, mut work: WorkCounters, budget: WorkBudget, cancellation: &CancellationToken, live_owned_bytes: u64,
    ) -> Result<(ObjectId, WorkCounters), OperationFailure<FsError>> {
        cancellation.check().map_err(|error| OperationFailure::new(error.into(), work))?;
        let mut allocations = AllocationLedger::default();
        allocations.claim_bytes(live_owned_bytes, 0, &mut work, budget)
            .map_err(|error| OperationFailure::new(error.into_work_or(FsError::PendingMutationAllocationFailed), work))?;
        let child_budget = remaining_owned(work, budget, live_owned_bytes)?;
        let encoded = crate::kernel::encode::<FsError>(child_budget, Some(cancellation), 0, |observe| crate::kernel::encode_generation_root(root, observe, FsError::EncodedAllocationFailed))
            .map_err(|failure| merge_simultaneous_failure(work, *failure.work, live_owned_bytes, failure.error))?;
        let (object, child_work) = self.put_encoded(
            ObjectKind::GenerationRoot, encoded, WorkCounters::default(), child_budget, cancellation,
        ).await.map_err(|failure| merge_simultaneous_failure(work, *failure.work, live_owned_bytes, failure.error))?;
        Ok((object, merge_simultaneous_work(work, child_work, live_owned_bytes, budget)?))
    }


    pub(super) async fn put_encoded(
        &self,
        kind: ObjectKind,
        encoded: FsReceipt<Vec<u8>>,
        work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(ObjectId, WorkCounters), OperationFailure<FsError>> {
        let work = add(work, encoded.work)?;
        cancellation.check().map_err(|error| OperationFailure::new(error.into(), work))?;
        let live_bytes = usize_to_u64(encoded.value.capacity());
        let length = usize_to_u64(encoded.value.len());
        let hashed_bytes = length.checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
            .ok_or_else(|| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
        let hashing = WorkCounters {
            bytes_hashed: hashed_bytes,
            // Bytes 1.12 allocates its owner header exactly for spare-capacity
            allocation_operations: u64::from(encoded.value.capacity() != encoded.value.len()),
            ..WorkCounters::default()
        };
        let mut prospective = work;
        prospective.charge(&hashing, &budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let hashed = HashedObject::new(kind, encoded.value.into());
        let object = hashed.object_id();
        let backend_budget = remaining_owned(prospective, budget, live_bytes)?;
        let receipt = self
            .inner
            .objects
            .put_hashed(hashed, backend_budget, cancellation)
            .await
            .map_err(|failure| {
                merge_simultaneous_failure(
                    prospective,
                    *failure.work,
                    live_bytes,
                    failure.error.into(),
                )
            })?;
        Ok((
            object,
            merge_simultaneous_work(prospective, receipt.work, live_bytes, budget)?,
        ))
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {

    /// Streams one bounded source into immutable authenticated chunks.
    pub async fn stage<R: AsyncBlobSource, W: FsWork>(
        &self, source: &mut R, maximum_source_bytes: u64, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<StagedContent, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let standalone = matches!(accounting, KernelAccounting::Standalone { .. } | KernelAccounting::BorrowedStandalone { .. });
        let read = stage_content_for_volume_with_accounting(
            &self.volume, source, maximum_source_bytes, &mut accounting, cancellation,
        );
        let result = if standalone { in_heap(move || async move { read.await }).await } else { read.await };
        budget.finish(result, &accounting)
    }


    pub(super) async fn stage_authored_blob_with_accounting(
        &self,
        content: acyclic_stream::OwnedBytes,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<StagedContent, FsError> {
        if content.len() as u64 > self.volume.config.limits.maximum_read_bytes {
            return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
        }
        let mut source = std::io::Cursor::new(content.as_ref());
        let result = self.stage_content_with_accounting(
            &mut source, (content.len() as u64).max(1), accounting, cancellation,
        ).await;
        drop(source);
        drop(content);
        result
    }


    /// Streams caller content into immutable authenticated chunks without
    pub async fn stage_content<R: AsyncBlobSource>(
        &self,
        source: &mut R,
        maximum_source_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<StagedContent> {
        in_heap(move || async move {
            let mut accounting = KernelAccounting::standalone(budget, cancellation);
            let result = self.stage_content_with_accounting(
                source, maximum_source_bytes, &mut accounting, cancellation,
            ).await;
            measured(result, &accounting)
        }).await
    }


    pub(crate) async fn stage_content_with_accounting<R: AsyncBlobSource>(
        &self,
        source: &mut R,
        maximum_source_bytes: u64,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<StagedContent, FsError> {
        stage_content_for_volume_with_accounting(
            &self.volume, source, maximum_source_bytes, accounting, cancellation,
        ).await
    }
}

pub(crate) async fn stage_content_for_volume_with_accounting<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    R: AsyncBlobSource,
>(
    volume: &Volume<A, O>,
    source: &mut R,
    maximum_source_bytes: u64,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<StagedContent, FsError> {
    if maximum_source_bytes == 0
        || maximum_source_bytes > volume.config.limits.maximum_generation_bytes
    {
        return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
    }
    accounting.check()?;
    cancellation.check()?;
    let chunk_bytes = u32::try_from(volume.config.limits.maximum_object_bytes.min(1024 * 1024))
        .unwrap_or(u32::MAX)
        .max(1);
    let page_bytes = u32::try_from(volume.config.limits.maximum_object_bytes)
        .unwrap_or(u32::MAX)
        .max(1);
    let blob = crate::kernel::build_blob_with_accounting(
        &volume.fs.inner.objects,
        source,
        BlobBuildOptions {
            chunk_bytes,
            page_items: volume.config.limits.maximum_directory_page_entries,
            page_bytes,
            maximum_blob_bytes: maximum_source_bytes,
        },
        accounting,
        cancellation,
    ).await?;
    Ok(StagedContent { root: blob.root, logical_bytes: blob.logical_bytes })
}

#[cfg(not(target_arch = "wasm32"))]
impl AsRef<[u8]> for crate::kernel::Owned<Vec<u8>> {
    fn as_ref(&self) -> &[u8] { &self.value }
}

