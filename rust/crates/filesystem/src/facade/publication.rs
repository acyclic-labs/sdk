use super::*;
mod prepare;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) struct LastCommit {
    pub(super) operation_id: OperationId,
    pub(super) generation_id: GenerationId,
    pub(super) head: Head,
}

// Single-use custody from the original authenticated publication.
pub(crate) type PublicationClosure<A, O> = Arc<Mutex<Option<VerifiedPublication<A, O>>>>;

pub(crate) struct VerifiedPublication<A: 'static, O: 'static> {
    pub(super) filesystem: Fs<A, O>,
    pub(super) generation_root: ObjectId,
    pub(super) volume_id: VolumeId,
    pub(super) limits: crate::kernel::ClosureLimits,
    pub(super) objects: Vec<ObjectId>,
    pub(super) proven_at: u64,
    pub(super) _allocation: KernelOwnedAllocation,
}

impl<A, O> VerifiedPublication<A, O> {
    pub(crate) fn retained_bytes(&self) -> u64 {
        usize_to_u64(self.objects.capacity() * size_of::<ObjectId>())
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Checkout<A, O> {
    pub(super) async fn publish_pending_against<E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
        permit: PublicationPermit,
        expected: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        before_publication: &mut F,
        resolved: Option<DurableCommit>,
        producer_custody: Option<&Arc<crate::kernel::ProofDataCustody>>,
    ) -> crate::MeasuredResult<OperationReceipt<LivePublicationObservation>, E>
    where
        E: From<FsError> + Send,
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::MeasuredResult<OperationReceipt<()>, E>> + ProviderTask,
    {
        cancellation
            .check()
            .map_err(|error| OperationFailure::before_work(E::from(FsError::from(error))))?;
        self.publication_closure = None;
        let (_read_fence, preconditions) = self
            .publication_dependencies()
            .map_err(|error| OperationFailure::before_work(E::from(error)))?;
        let checkpoint = self
            .checkpoint_root(budget, cancellation)
            .await
            .map_err(|failure| failure.map_error(E::from))?;
        let checkpoint_root = checkpoint.value;
        let mut work = checkpoint.work;
        let request = self.publication_request(operation_id, expected, checkpoint_root);
        let custody_bytes = crate::foundation::usize_to_u64(
            std::mem::size_of::<Mutex<Option<VerifiedPublication<A, O>>>>()
                + 2 * std::mem::size_of::<usize>(),
        );
        let prospective = add(
            work,
            WorkCounters {
                allocation_operations: 1,
                peak_allocation_bytes: custody_bytes,
                ..WorkCounters::default()
            },
        )
        .map_err(|failure| failure.map_error(E::from))?;
        remaining(prospective, budget)
            .map_err(|failure| OperationFailure::new(E::from(failure.error), work))?;
        let publication_slot = Arc::new(Mutex::new(None));
        work = prospective;
        let mut publication_budget =
            remaining(work, budget).map_err(|failure| failure.map_error(E::from))?;
        publication_budget.peak_allocation_bytes -= custody_bytes;
        let proven_at = self.volume.fs.inner.objects.collection_sweeps();
        let mut prepared_roots = None;
        let publication = {
            let mut prepare_roots = |root: &GenerationRoot, budget| {
                let prepared = self
                    .prepare_published_roots(root, budget, cancellation, producer_custody)
                    .map_err(|failure| failure.map_error(E::from))?;
                let bytes = prepare::root_storage_bytes(&prepared.value)
                    .map_err(|error| OperationFailure::new(E::from(error), prepared.work))?;
                prepared_roots = Some(prepared.value);
                Ok(OperationReceipt {
                    value: bytes,
                    work: prepared.work,
                })
            };
            crate::kernel::publish_generation_async_guarded(
                self.volume.fs.clone(),
                request,
                PublicationIntent {
                    preconditions,
                    operation_context,
                    original_evidence: operation_evidence.clone(),
                    fingerprint_permit: original_fingerprint_permit,
                    permit,
                },
                closure_limits(self.volume.config),
                Some(&self.volume.fs.inner.publication_proof.inner),
                publication_budget,
                cancellation,
                before_publication,
                resolved,
                Some(&mut prepare_roots),
            )
            .await
        }
        .map_err(|mut failure| {
            *failure.work = work
                .with_backend(*failure.work, custody_bytes)
                .unwrap_or(work);
            failure
        })?;
        drop(operation_evidence);
        work = merge_simultaneous_work(work, publication.work, custody_bytes, budget)
            .map_err(|failure| failure.map_error(E::from))?;
        let outcome = self
            .apply_publication(
                operation_id,
                (publication, KernelOwnedAllocation::Standalone),
                proven_at,
                publication_slot,
                prepared_roots,
            )
            .map_err(|error| OperationFailure::new(E::from(error), work))?;
        Ok(OperationReceipt {
            value: outcome,
            work,
        })
    }

    /// Publishes the unchanged root as the original companion head fence.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn commit_even_if_clean_with_native_work<'a, E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        permit: PublicationPermit,
        original: &'a crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        before_publication: &mut F,
        producer_custody: Option<&Arc<crate::kernel::ProofDataCustody>>,
        operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
    ) -> Result<LivePublicationObservation, E>
    where
        E: From<FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        if self.mode.access != AccessMode::ReadWrite
            || self.mode.mutations != MutationMode::PrivateOverlay
        {
            return Err(E::from(FsError::MutationNotAllowed));
        }
        let expected = self
            .authority_head
            .ok_or_else(|| E::from(FsError::WritableCheckoutRequiresHead))?;
        self.publish_pending_against_with_native_work(
            operation_id,
            operation_context,
            operation_evidence,
            original_fingerprint_permit,
            permit,
            expected,
            original,
            cancellation,
            before_publication,
            None,
            producer_custody,
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn publish_pending_with_native_work<'a, E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
        permit: PublicationPermit,
        original: &'a crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        before_publication: &mut F,
        producer_custody: Option<&Arc<crate::kernel::ProofDataCustody>>,
    ) -> Result<LivePublicationObservation, E>
    where
        E: From<FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        original
            .admit(WorkCounters::UNCHARGED)
            .map_err(FsError::from)
            .map_err(E::from)?;
        cancellation
            .check()
            .map_err(FsError::from)
            .map_err(E::from)?;
        if !self.has_pending_mutations()
            && permit == PublicationPermit::Unrestricted
            && operation_context.is_none()
            && operation_evidence.is_none()
        {
            return self
                .resolve_clean_commit(operation_id, original, cancellation)
                .await
                .map_err(E::from);
        }
        let expected = self
            .authority_head
            .ok_or_else(|| E::from(FsError::WritableCheckoutRequiresHead))?;
        self.publish_pending_against_with_native_work(
            operation_id,
            operation_context,
            operation_evidence,
            original_fingerprint_permit,
            permit,
            expected,
            original,
            cancellation,
            before_publication,
            None,
            producer_custody,
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn publish_pending_against_with_native_work<'a, E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
        permit: PublicationPermit,
        expected: Head,
        original: &'a crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        before_publication: &mut F,
        resolved: Option<DurableCommit>,
        producer_custody: Option<&Arc<crate::kernel::ProofDataCustody>>,
    ) -> Result<LivePublicationObservation, E>
    where
        E: From<FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        cancellation
            .check()
            .map_err(FsError::from)
            .map_err(E::from)?;
        let mut frame_custody = None;
        let create = || async move {
            original
                .admit(WorkCounters::UNCHARGED)
                .map_err(FsError::from)
                .map_err(E::from)?;
            cancellation
                .check()
                .map_err(FsError::from)
                .map_err(E::from)?;
            self.publication_closure = None;
            let (_read_fence, preconditions) = self.publication_dependencies().map_err(E::from)?;
            let checkpoint_root = self
                .checkpoint_root(original, cancellation)
                .await
                .map_err(E::from)?;
            let request = self.publication_request(operation_id, expected, checkpoint_root);
            let (publication_slot, mut slot_allocation) =
                VerifiedPublication::<A, O>::native_slot(original).map_err(E::from)?;
            let proven_at = self.volume.fs.inner.objects.collection_sweeps();
            let mut prepared_roots = None;
            let mut publication = {
                let mut prepare_roots =
                    |root: &GenerationRoot,
                     proof_guard: &mut crate::NativeAllocation,
                     original: &crate::NativeMaterializationWork| {
                        let prepared = self
                            .prepare_published_native_roots(
                                root,
                                proof_guard,
                                original,
                                cancellation,
                                producer_custody,
                            )
                            .map_err(E::from)?;
                        prepared_roots = Some(prepared);
                        Ok(())
                    };
                crate::kernel::publish_generation_async_guarded_with_native_work(
                    self.volume.fs.clone(),
                    request,
                    PublicationIntent {
                        preconditions,
                        operation_context,
                        original_evidence: operation_evidence.clone(),
                        fingerprint_permit: original_fingerprint_permit,
                        permit,
                    },
                    closure_limits(self.volume.config),
                    Some(&self.volume.fs.inner.publication_proof.inner),
                    original,
                    cancellation,
                    before_publication,
                    resolved,
                    Some(&mut prepare_roots),
                )
                .await?
            };
            drop(operation_evidence);
            publication
                .1
                .absorb(&mut slot_allocation)
                .map_err(FsError::from)
                .map_err(E::from)?;
            let retained = (
                publication.0,
                KernelOwnedAllocation::NativeOriginal(publication.1),
            );
            self.apply_publication(
                operation_id,
                retained,
                proven_at,
                publication_slot,
                prepared_roots,
            )
            .map_err(E::from)
        };
        let (mut future, _) = crate::heap_future::in_heap_admitted(
            (create, &mut frame_custody),
            |(_, custody), bytes| {
                **custody = Some(prepare::native_frame(original, bytes).map_err(E::from)?);
                Ok::<(), E>(())
            },
            |(create, _)| create(),
        )?;
        let result = future.as_mut().await;
        drop(future);
        drop(frame_custody);
        result
    }

    /// Resolves only the original durable operation. A lost acknowledgement
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn retry_stale_commit_with_native_work<'a, E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        permit: PublicationPermit,
        original: &'a crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        before_publication: &mut F,
        producer_custody: Option<&Arc<crate::kernel::ProofDataCustody>>,
        operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
    ) -> Result<Option<LivePublicationObservation>, E>
    where
        E: From<FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let mut authority = crate::AuthorityWork::native(original, cancellation);
        let resolved = self
            .volume
            .fs
            .inner
            .authority
            .find_operation(
                volume_authority_id(self.volume.id),
                operation_id,
                &mut authority,
            )
            .await
            .map_err(|error| {
                E::from(match error {
                    AuthorityStoreError::NativeOriginal(error) => FsError::NativeKernel(error),
                    error => FsError::Authority(error),
                })
            })?;
        let Some(commit) = resolved else {
            return Ok(None);
        };
        let expected = original_publication_head(
            &commit,
            &mut KernelAccounting::native(original, cancellation),
        )
        .map_err(E::from)?;
        self.publish_pending_against_with_native_work(
            operation_id,
            operation_context,
            operation_evidence,
            original_fingerprint_permit,
            permit,
            expected,
            original,
            cancellation,
            before_publication,
            Some(commit),
            producer_custody,
        )
        .await
        .map(Some)
    }

    pub(crate) fn publication_closure(
        &self,
        generation: GenerationId,
    ) -> Option<PublicationClosure<A, O>> {
        if self.generation_root != generation.object_id() {
            return None;
        }
        self.publication_closure.clone()
    }

    pub(super) fn apply_publication(
        &mut self,
        operation_id: OperationId,
        mut retained: (PublicationReceipt, KernelOwnedAllocation),
        proven_at: u64,
        publication_slot: PublicationClosure<A, O>,
        mut prepared_roots: Option<PreparedCheckoutRoots>,
    ) -> Result<LivePublicationObservation, FsError> {
        let generation_id = retained.0.proof.generation_id;
        let checkpoint_root = generation_id.object_id();
        self.publication_closure = if matches!(
            retained.0.outcome,
            AppendOutcome::Committed(_) | AppendOutcome::AlreadyCommitted(_)
        ) {
            *publication_slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(VerifiedPublication {
                filesystem: self.volume.fs.clone(),
                generation_root: checkpoint_root,
                volume_id: retained.0.proof.root.volume_id,
                limits: closure_limits(self.volume.config),
                objects: std::mem::take(&mut retained.0.proof.objects),
                proven_at,
                _allocation: std::mem::replace(&mut retained.1, KernelOwnedAllocation::Standalone),
            });
            Some(publication_slot)
        } else {
            None
        };
        let outcome = checkout_commit_outcome(&retained.0);
        if !matches!(
            outcome,
            LivePublicationObservation::Committed { .. }
                | LivePublicationObservation::AlreadyCommitted { .. }
        ) {
            drop(retained.0.proof);
            return Ok(outcome);
        }
        if let LivePublicationObservation::Committed { head, .. }
        | LivePublicationObservation::AlreadyCommitted { head, .. } = outcome
        {
            self.install_root_replacement(
                checkpoint_root,
                retained.0.proof.root,
                prepared_roots.take().ok_or(FsError::InvalidDiff)?,
            )?;
            self.generation_root = checkpoint_root;
            self.authority_head = Some(head);
            self.prepared_merge_parent = None;
            self.dependencies.proof().clear();
            self.observed_generation = super::checkout::ObservedGeneration::committed(LastCommit {
                operation_id,
                generation_id,
                head,
            });
        }
        Ok(outcome)
    }

    pub(crate) async fn retry_stale_commit<E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        permit: PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        before_publication: &mut F,
        producer_custody: Option<&Arc<crate::kernel::ProofDataCustody>>,
        operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
    ) -> crate::MeasuredResult<OperationReceipt<Option<LivePublicationObservation>>, E>
    where
        E: From<FsError> + Send,
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::MeasuredResult<OperationReceipt<()>, E>> + ProviderTask,
    {
        crate::heap_future::in_heap_measured_with_error(
            move |budget| async move {
                cancellation.check().map_err(|error| {
                    OperationFailure::before_work(E::from(FsError::from(error)))
                })?;
                let resolved = ({
                    let mut authority = crate::AuthorityWork::new(budget, cancellation);
                    let result = self
                        .volume
                        .fs
                        .inner
                        .authority
                        .find_operation(
                            volume_authority_id(self.volume.id),
                            operation_id,
                            &mut authority,
                        )
                        .await;
                    authority.receipt(result)
                })
                .map_err(|failure| {
                    OperationFailure::new(E::from(FsError::Authority(failure.error)), *failure.work)
                })?;
                let Some(commit) = resolved.value else {
                    return Ok(OperationReceipt {
                        value: None,
                        work: resolved.work,
                    });
                };
                let mut decoded = KernelAccounting::standalone(
                    remaining(resolved.work, budget)
                        .map_err(|failure| failure.map_error(E::from))?,
                    cancellation,
                );
                let expected =
                    original_publication_head(&commit, &mut decoded).map_err(|error| {
                        OperationFailure::new(
                            E::from(error),
                            resolved
                                .work
                                .checked_add(decoded.spent())
                                .unwrap_or(resolved.work),
                        )
                    })?;
                let resolved_work = add(resolved.work, decoded.spent())
                    .map_err(|failure| failure.map_error(E::from))?;
                let retried = self
                    .publish_pending_against(
                        operation_id,
                        operation_context,
                        operation_evidence,
                        original_fingerprint_permit,
                        permit,
                        expected,
                        remaining(resolved_work, budget)
                            .map_err(|failure| failure.map_error(E::from))?,
                        cancellation,
                        before_publication,
                        Some(commit),
                        producer_custody,
                    )
                    .await
                    .map_err(|mut failure| {
                        *failure.work = resolved_work
                            .checked_add(*failure.work)
                            .unwrap_or(resolved_work);
                        failure
                    })?;
                Ok(OperationReceipt {
                    value: Some(retried.value),
                    work: add(resolved_work, retried.work)
                        .map_err(|failure| failure.map_error(E::from))?,
                })
            },
            budget,
            cancellation,
            E::from(FsError::Cancelled(crate::CancellationError)),
            None,
            |error| E::from(FsError::from(error)),
        )
        .await
    }

    pub(super) async fn resolve_clean_commit<W: FsWork>(
        &self,
        operation_id: OperationId,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<LivePublicationObservation, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if accounting.native_work().is_some() {
                accounting.check()?;
            }
            if let Some(last_operation) = self.observed_generation.operation() {
                if last_operation != operation_id {
                    return Err(FsError::NoPendingMutations);
                }
                if let Some(last) = self.observed_generation.commit()
                    && last.generation_id == GenerationId::from_root(self.generation_root)
                {
                    return Ok(LivePublicationObservation::AlreadyCommitted {
                        generation_id: last.generation_id,
                        head: last.head,
                    });
                }
            }
            accounting.check()?;
            let head = self
                .authority_head
                .ok_or(FsError::WritableCheckoutRequiresHead)?;
            let mut authority = authority_work(&accounting, cancellation)?;
            let resolved = self
                .volume
                .fs
                .inner
                .authority
                .find_operation(
                    volume_authority_id(self.volume.id),
                    operation_id,
                    &mut authority,
                )
                .await;
            record_authority_work(&mut accounting, &authority)?;
            let resolved = resolved.map_err(|error| match error {
                #[cfg(not(target_arch = "wasm32"))]
                AuthorityStoreError::NativeOriginal(error) => FsError::NativeKernel(error),
                error => FsError::Authority(error),
            })?;
            let commit = resolved.as_ref().ok_or(FsError::NoPendingMutations)?;
            let generation_root = generation_from_record(commit, self.volume.id, &mut accounting)?;
            Ok(if generation_root == self.generation_root {
                LivePublicationObservation::AlreadyCommitted {
                    generation_id: GenerationId::from_root(generation_root),
                    head,
                }
            } else {
                LivePublicationObservation::IdempotencyConflict {
                    committed_fingerprint: commit.fingerprint,
                }
            })
        }
        .await;
        budget.finish(result, &accounting)
    }
}

pub(super) fn durable_head(commit: &DurableCommit) -> Head {
    Head {
        epoch: commit.epoch,
        sequence: commit.sequence,
        digest: commit.digest,
    }
}

pub(crate) fn closure_limits(config: VolumeConfig) -> ClosureLimits {
    ClosureLimits {
        decode: DecodeLimits::for_volume(config),
        maximum_objects: config.limits.maximum_objects_per_generation,
        maximum_files: config.limits.maximum_files_per_generation,
        maximum_object_bytes: config.limits.maximum_generation_bytes,
        profile: config.profile,
        symbolic_links: config.symbolic_links,
        hard_links: config.hard_links,
        sparse_files: config.sparse_files,
    }
}

fn original_publication_head(
    record: &DurableCommit,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Head, FsError> {
    let decoded = crate::kernel::decode_publication_evidence_with_accounting(
        &record.payload,
        MAXIMUM_VOLUME_EVENT_BYTES,
        accounting,
    )?;
    Ok(decoded.origin.map_or(
        Head {
            epoch: record.epoch,
            sequence: Sequence::new(
                record
                    .sequence
                    .get()
                    .checked_sub(1)
                    .ok_or(FsError::InvalidAuthorityHistory)?,
            ),
            digest: record.previous_digest,
        },
        |origin| origin.expected,
    ))
}
