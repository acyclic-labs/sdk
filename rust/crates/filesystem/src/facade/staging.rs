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
    pub(super) async fn flush_publication(
        &self,
        scope: crate::PublicationScope<'_>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::PublicationHold, FsError> {
        accounting.check()?;
        let mut work = authority_work(accounting, cancellation)?;
        let result = self
            .inner
            .objects
            .flush_before_publish(scope, &mut work)
            .await;
        record_authority_work(accounting, &work)?;
        result.map_err(FsError::from)
    }
    pub(crate) async fn make_records_durable<W: FsWork>(
        &self,
        config: VolumeConfig,
        records: &[FileRecord],
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::PublicationHold, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            accounting.check()?;
            let proven_at = self.inner.objects.collection_sweeps();
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                let (closure, _allocation) = prove_record_closure_with_native_work(
                    &self.inner.objects,
                    records,
                    closure_limits(config),
                    original,
                    cancellation,
                )
                .await?;
                return self
                    .flush_publication(
                        crate::PublicationScope::Closure {
                            objects: &closure,
                            proven_at,
                        },
                        &mut accounting,
                        cancellation,
                    )
                    .await;
            }
            let proof = prove_record_closure_async(
                &self.inner.objects,
                records,
                closure_limits(config),
                accounting.remaining()?,
                cancellation,
            )
            .await;
            let spent = match &proof {
                Ok((_, work)) => *work,
                Err(failure) => *failure.work,
            };
            merge_creation_backend_work(&mut accounting, spent)?;
            let (closure, _) = proof.map_err(|failure| FsError::from(failure.error))?;
            self.flush_publication(
                crate::PublicationScope::Closure {
                    objects: &closure,
                    proven_at,
                },
                &mut accounting,
                cancellation,
            )
            .await
        }
        .await;
        budget.finish(result, &accounting)
    }
    /// Holds the authenticated closure across an external durable publication.
    pub(crate) async fn make_generation_durable<W: FsWork>(
        &self,
        volume: &Volume<A, O>,
        generation: GenerationId,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::PublicationHold, WorkspaceError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            accounting.check()?;
            if !self.same_deployment(&volume.fs) {
                return Err(WorkspaceError::ForeignGeneration);
            }
            let publication = self
                .retention_publication_with_accounting(
                    volume,
                    ObjectId {
                        kind: ObjectKind::GenerationRoot,
                        digest: generation.digest(),
                    },
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let result = self
                .flush_publication(
                    crate::PublicationScope::Closure {
                        objects: &publication.value.objects,
                        proven_at: publication.value.proven_at,
                    },
                    &mut accounting,
                    cancellation,
                )
                .await;
            publication.retire(&mut accounting)?;
            result.map_err(Into::into)
        }
        .await;
        budget.finish(result, &accounting)
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
            .workspace_operation_generation(
                workspace,
                operation_id,
                &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation),
                &cancellation,
            )
            .await?
            .0
            .map(|(generation, record)| {
                drop(record);
                generation
            })
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
            .map(|receipt| receipt.value)
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
                crate::WorkspaceRestore::AlreadyRestored(publication.proof.generation_id)
            }
            AppendOutcome::Conflict { .. } => {
                let (actual, _, _) = workspace
                    .resolve_head_generation(WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map(|receipt| receipt.value)
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
        &self,
        kind: ObjectKind,
        mut encoded: (Vec<u8>, KernelOwnedAllocation),
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let allocation = match &mut encoded.1 {
                KernelOwnedAllocation::NativeOriginal(allocation) => allocation,
                KernelOwnedAllocation::Standalone => return Err(FsError::HandleAllocationFailed),
            };
            let hashed = HashedObject::from_native_vec(
                kind,
                &mut encoded.0,
                allocation,
                original,
                cancellation,
            )?;
            let object = hashed.object_id();
            let mut work = authority_work(accounting, cancellation)?;
            self.inner.objects.put_hashed(hashed, &mut work).await?;
            return Ok(object);
        }
        let capacity = encoded.0.capacity() as u64;
        let hashing = WorkCounters {
            bytes_hashed: (encoded.0.len() as u64)
                .checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
                .ok_or(WorkError::Overflow)?,
            ..WorkCounters::UNCHARGED
        };
        let owner = WorkCounters {
            allocation_operations: u64::from(encoded.0.capacity() != encoded.0.len()),
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(owner)?;
        accounting.admit(hashing)?;
        let (payload, allocation) = encoded;
        let bytes = acyclic_stream::OwnedBytes::from(payload);
        if let Err(error) = accounting.record_completed(owner) {
            drop(bytes);
            drop(allocation);
            return Err(error);
        }
        drop(allocation);
        let hashed = HashedObject::new(kind, bytes);
        accounting.record_completed(hashing)?;
        let object = hashed.object_id();
        let mut object_work = authority_work(accounting, cancellation)?;
        let result = self
            .inner
            .objects
            .put_hashed(hashed, &mut object_work)
            .await;
        let spent = object_work.spent();
        let (allocations, work, budget, _) = accounting
            .standalone_parts()
            .ok_or(FsError::HandleAllocationFailed)?;
        *work = work.with_backend(spent, allocations.live_bytes())?;
        allocations
            .release(capacity)
            .map_err(|error| error.into_work_or(FsError::PendingMutationAllocationFailed))?;
        work.verify(budget)?;
        result.map_err(FsError::from)?;
        Ok(object)
    }
    pub(super) async fn put_generation_root(
        &self,
        root: &GenerationRoot,
        mut work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        live_bytes: u64,
    ) -> Result<(ObjectId, WorkCounters), OperationFailure<FsError>> {
        let mut allocations = AllocationLedger::default();
        let result = async {
            let mut accounting = KernelAccounting::borrowed_standalone(
                &mut allocations,
                &mut work,
                budget,
                cancellation,
            );
            accounting.claim_owned(live_bytes, 0)?;
            let encoded = crate::kernel::codec::encode_owned(&mut accounting, |observe, error| {
                crate::kernel::encode_generation_root(root, observe, error)
            })?;
            self.put_encoded_with_accounting(
                ObjectKind::GenerationRoot,
                encoded,
                &mut accounting,
                cancellation,
            )
            .await
        }
        .await;
        result
            .map(|object| (object, work))
            .map_err(|error| OperationFailure::new(error, work))
    }
    pub(super) async fn put_encoded(
        &self,
        kind: ObjectKind,
        encoded: OperationReceipt<Vec<u8>>,
        prior: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(ObjectId, WorkCounters), OperationFailure<FsError>> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            accounting
                .record_completed(add(prior, encoded.work).map_err(|failure| failure.error)?)?;
            let allocation = accounting.claim_owned(encoded.value.capacity() as u64, 0)?;
            self.put_encoded_with_accounting(
                kind,
                (encoded.value, allocation),
                &mut accounting,
                cancellation,
            )
            .await
        }
        .await;
        let work = accounting.spent();
        result
            .map(|object| (object, work))
            .map_err(|error| OperationFailure::new(error, work))
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    /// Streams one bounded source into immutable authenticated chunks.
    pub async fn stage<R: AsyncBlobSource, W: FsWork>(
        &self,
        source: &mut R,
        maximum_source_bytes: u64,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<StagedContent, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let standalone = matches!(
            accounting,
            KernelAccounting::Standalone { .. } | KernelAccounting::BorrowedStandalone { .. }
        );
        let read = stage_content_for_volume_with_accounting(
            &self.volume,
            source,
            maximum_source_bytes,
            &mut accounting,
            cancellation,
        );
        let result = if standalone {
            in_heap(move || async move { read.await }).await
        } else {
            read.await
        };
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
        let result = self
            .stage_content_with_accounting(
                &mut source,
                (content.len() as u64).max(1),
                accounting,
                cancellation,
            )
            .await;
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
            let result = self
                .stage_content_with_accounting(
                    source,
                    maximum_source_bytes,
                    &mut accounting,
                    cancellation,
                )
                .await;
            measured(result, &accounting)
        })
        .await
    }

    pub(crate) async fn stage_content_with_accounting<R: AsyncBlobSource>(
        &self,
        source: &mut R,
        maximum_source_bytes: u64,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<StagedContent, FsError> {
        stage_content_for_volume_with_accounting(
            &self.volume,
            source,
            maximum_source_bytes,
            accounting,
            cancellation,
        )
        .await
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
    )
    .await?;
    Ok(StagedContent {
        root: blob.root,
        logical_bytes: blob.logical_bytes,
    })
}

#[cfg(not(target_arch = "wasm32"))]
impl AsRef<[u8]> for crate::kernel::Owned<Vec<u8>> {
    fn as_ref(&self) -> &[u8] {
        &self.value
    }
}
