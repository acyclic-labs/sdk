//! One fenced publication path shared by native mounts and host watchers.

use crate::MountSourceError;
use crate::kernel::RebaseDecision;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Checkout, CheckoutCommitOutcome,
    LiveMutationOutcome, OperationId, PublicationPermit, WorkBudget, WorkCounters,
};

const MAXIMUM_SAFE_REBASE_ATTEMPTS: usize = 8;

/// Seals the exact current checkout candidate under one stable operation ID.
///
/// Callers must retain `operation_id` across every ambiguous retry and must not
/// admit another mutation until this function acknowledges success.
///
/// # Errors
///
/// Returns stale for deterministic conflicts/fences and an engine failure for
/// storage, authentication, cancellation, or indeterminate authority results.
pub async fn seal_checkout<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    operation_id: OperationId,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), MountSourceError> {
    seal_checkout_with_permit(
        checkout,
        operation_id,
        PublicationPermit::Unrestricted,
        budget,
        cancellation,
    )
    .await
}

/// Seals a checkout under one authority-evaluated operation permit.
pub async fn seal_checkout_with_permit<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    operation_id: OperationId,
    permit: PublicationPermit,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), MountSourceError> {
    seal_checkout_with_permit_measured(checkout, operation_id, permit, budget, cancellation)
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
}

/// Seals the same candidate, retaining every attempted publication receipt.
pub async fn seal_checkout_with_permit_measured<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    operation_id: OperationId,
    permit: PublicationPermit,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> crate::MeasuredResult<crate::OperationReceipt<()>, MountSourceError> {
    let mut work = WorkCounters::default();
    let result = async {
    cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
    if !checkout.has_pending_mutations() {
        return Ok(());
    }
    match checkout.mode().mutations {
        crate::model::MutationMode::PrivateOverlay => {
            let receipt = checkout.commit(operation_id, None, None, None, permit, budget, cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })
                .await.map_err(|failure| publication_failure(failure, &mut work))?;
            work = receipt.work;
            match receipt.value {
            CheckoutCommitOutcome::Committed { .. }
            | CheckoutCommitOutcome::AlreadyCommitted { .. } => Ok(()),
            CheckoutCommitOutcome::Conflict { .. }
            | CheckoutCommitOutcome::Fenced { .. }
            | CheckoutCommitOutcome::IdempotencyConflict { .. } => Err(MountSourceError::Stale),
            }
        }
        crate::model::MutationMode::DirectLive if permit == PublicationPermit::Unrestricted => {
            let receipt = checkout.resume_live(operation_id, 8, 256, budget, cancellation)
                .await.map_err(|failure| publication_failure(failure, &mut work))?;
            work = receipt.work;
            match receipt.value {
                LiveMutationOutcome::Committed { .. }
                | LiveMutationOutcome::AlreadyCommitted { .. } => Ok(()),
                LiveMutationOutcome::Conflicted { .. }
                | LiveMutationOutcome::RetryLimit { .. }
                | LiveMutationOutcome::Fenced { .. }
                | LiveMutationOutcome::IdempotencyConflict { .. } => Err(MountSourceError::Stale),
            }
        }
        crate::model::MutationMode::DirectLive => Err(MountSourceError::Unsupported(
            "direct-live mounts do not support operation lease permits".to_owned(),
        )),
        crate::model::MutationMode::None => Err(MountSourceError::Unsupported(
            "checkout does not admit native writes".to_owned(),
        )),
    }
    }.await;
    result.map(|value| crate::OperationReceipt { value, work })
        .map_err(|error| crate::OperationFailure::new(error, work))
}

/// Publishes a checkout operation even when its only effect is the separately
/// journaled lazy source overlay.
pub async fn seal_checkout_with_permit_force<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    operation_id: OperationId,
    permit: PublicationPermit,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), MountSourceError> {
    seal_checkout_with_permit_force_measured(checkout, operation_id, permit, budget, cancellation)
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
}

/// Forces the same retained operation with cumulative rebase/publication work.
pub async fn seal_checkout_with_permit_force_measured<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    operation_id: OperationId,
    permit: PublicationPermit,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> crate::MeasuredResult<crate::OperationReceipt<()>, MountSourceError> {
    let mut work = WorkCounters::default();
    let result = async {
    cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
    if checkout.mode().mutations != crate::model::MutationMode::PrivateOverlay {
        return Err(MountSourceError::Unsupported(
            "forced publication requires a private-overlay checkout".to_owned(),
        ));
    }
    // The same safe-rebase bound governs the original native carrier below.
    for _ in 0..MAXIMUM_SAFE_REBASE_ATTEMPTS {
        let remaining = work.remaining(budget).map_err(engine_error)?;
        let publication = if checkout.has_pending_mutations() {
            checkout
                .commit(operation_id, None, None, None, permit, remaining, cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })
                .await
        } else {
            checkout
                .commit_even_if_clean(operation_id, None, None, None, permit, remaining, cancellation, &mut |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }, None)
                .await
        }
        .map_err(|failure| publication_failure(failure, &mut work))?;
        work = work.checked_add(publication.work).map_err(engine_error)?;
        work.verify(budget).map_err(engine_error)?;
        match publication.value {
            CheckoutCommitOutcome::Committed { .. }
            | CheckoutCommitOutcome::AlreadyCommitted { .. } => return Ok(()),
            CheckoutCommitOutcome::Conflict { .. } => {
                let maximum_conflicts = checkout
                    .volume_config()
                    .limits
                    .maximum_checkout_dependencies;
                let decision = checkout
                    .rebase_head(
                        maximum_conflicts,
                        work.remaining(budget).map_err(engine_error)?,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| publication_failure(failure, &mut work))?;
                work = work.checked_add(decision.work).map_err(engine_error)?;
                work.verify(budget).map_err(engine_error)?;
                if matches!(decision.value, RebaseDecision::Conflicted { .. }) {
                    return Err(MountSourceError::Stale);
                }
            }
            CheckoutCommitOutcome::Fenced { .. }
            | CheckoutCommitOutcome::IdempotencyConflict { .. } => {
                return Err(MountSourceError::Stale);
            }
        }
    }
    Err(MountSourceError::Engine(
        "publication raced with repeated head advances; retry retained operation".to_owned(),
    ))
    }.await;
    result.map(|value| crate::OperationReceipt { value, work })
        .map_err(|error| crate::OperationFailure::new(error, work))
}

/// Publishes the retained operation on its original allocation family.
/// `None` is a deterministic SDK rejection; an unchanged checkout is not an ACK.
pub(super) fn seal_checkout_with_permit_guarded_with_native_work<'a, A, O, T, F, Fut>(
    checkout: &'a mut Checkout<A, O>,
    operation_id: OperationId,
    permit: PublicationPermit,
    force: bool,
    original: &'a crate::NativeMaterializationWork,
    cancellation: &'a CancellationToken,
    before_publication: &'a mut F,
    producer_custody: Option<&'a std::sync::Arc<crate::kernel::ProofDataCustody>>,
    operation_context: Option<crate::Digest>,
    operation_evidence: Option<acyclic_stream::OwnedBytes>,
    original_fingerprint_permit: Option<PublicationPermit>,
) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<super::adapter::NativeSourcePublicationAck>, T>>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    T: From<crate::FsError> + From<MountSourceError> + Send + 'static,
    F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Result<(), T>> + Send + 'static,
{
    Box::pin(async move {
        cancellation.check().map_err(|error| T::from(crate::FsError::Cancelled(error)))?;
        use super::adapter::NativeSourcePublicationAck;
        let original_generation = checkout.generation_id();
        let mode = checkout.mode().mutations;
        if force && mode != crate::model::MutationMode::PrivateOverlay {
            return Err(T::from(MountSourceError::NativeUnsupported));
        }
        match mode {
            crate::model::MutationMode::PrivateOverlay => {
                // Resolve the original durable operation before another compile,
                // owner callback, flush or authority append. A known ACK converges
                // through the very same SDK pending-publication engine.
                if let Some(outcome) = checkout.retry_stale_commit_with_native_work(
                    operation_id, permit, original, cancellation, before_publication,
                    producer_custody, operation_context, operation_evidence.clone(), original_fingerprint_permit,
                ).await? {
                    return Ok(match outcome {
                        CheckoutCommitOutcome::Committed { generation_id, .. }
                        | CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } =>
                            Some(NativeSourcePublicationAck::Published { operation: operation_id, generation: generation_id }),
                        CheckoutCommitOutcome::Conflict { .. }
                        | CheckoutCommitOutcome::Fenced { .. }
                        | CheckoutCommitOutcome::IdempotencyConflict { .. } => None,
                    });
                }
                if !force && !checkout.has_pending_mutations() {
                    return Ok(Some(NativeSourcePublicationAck::Unchanged { generation: original_generation }));
                }
                let attempts = if force { MAXIMUM_SAFE_REBASE_ATTEMPTS } else { 1 };
                for _ in 0..attempts {
                    original.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })
                        .map_err(|error| T::from(MountSourceError::NativeWork(error)))?;
                    let outcome = checkout.commit_even_if_clean_with_native_work(
                        operation_id, permit, original, cancellation, before_publication,
                        producer_custody, operation_context, operation_evidence.clone(), original_fingerprint_permit,
                    ).await?;
                    match outcome {
                        CheckoutCommitOutcome::Committed { generation_id, .. }
                        | CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } =>
                            return Ok(Some(NativeSourcePublicationAck::Published { operation: operation_id, generation: generation_id })),
                        CheckoutCommitOutcome::Conflict { .. } if force => {
                            let (decision, allocation) = checkout.rebase_head_with_native_work(
                                checkout.volume_config().limits.maximum_checkout_dependencies,
                                original, cancellation,
                            ).await.map_err(T::from)?;
                            let conflicted = matches!(decision, RebaseDecision::Conflicted { .. });
                            drop(decision);
                            drop(allocation);
                            if conflicted { return Ok(None); }
                        }
                        CheckoutCommitOutcome::Conflict { .. }
                        | CheckoutCommitOutcome::Fenced { .. }
                        | CheckoutCommitOutcome::IdempotencyConflict { .. } => return Ok(None),
                    }
                }
                Err(T::from(MountSourceError::PublicationRetryLimit))
            }
            crate::model::MutationMode::DirectLive if permit == PublicationPermit::Unrestricted => {
                if !checkout.has_pending_mutations() {
                    return Ok(Some(NativeSourcePublicationAck::Unchanged { generation: original_generation }));
                }
                let (outcome, allocation) = checkout.resume_live_with_native_work(
                    operation_id, 8, 256, permit, original, cancellation, before_publication, producer_custody,
                    operation_context, operation_evidence, original_fingerprint_permit,
                ).await?;
                let ack = match &outcome {
                    LiveMutationOutcome::Committed { generation_id, .. }
                    | LiveMutationOutcome::AlreadyCommitted { generation_id, .. } =>
                        Some(NativeSourcePublicationAck::Published { operation: operation_id, generation: *generation_id }),
                    LiveMutationOutcome::Conflicted { .. }
                    | LiveMutationOutcome::RetryLimit { .. }
                    | LiveMutationOutcome::Fenced { .. }
                    | LiveMutationOutcome::IdempotencyConflict { .. } => None,
                };
                drop(outcome);
                drop(allocation);
                Ok(ack)
            }
            crate::model::MutationMode::DirectLive | crate::model::MutationMode::None =>
                Err(T::from(MountSourceError::NativeUnsupported)),
        }
    })
}

fn engine_error(error: impl std::fmt::Display) -> MountSourceError {
    MountSourceError::Engine(error.to_string())
}

fn publication_failure(
    failure: crate::OperationFailure<crate::FsError>,
    work: &mut WorkCounters,
) -> MountSourceError {
    let failure = failure.map_with_prior_work(*work, |error| match error {
        crate::FsError::Work(error) => MountSourceError::Work(error),
        crate::FsError::Cancelled(_) => MountSourceError::Cancelled,
        error => engine_error(error),
    });
    *work = *failure.work;
    failure.error
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Fs;
    use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
    use crate::model::{
        AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, Lifecycle, MutationMode,
        VolumeConfig,
    };
    use bytes::Bytes;

    #[tokio::test]
    async fn conflicting_publication_retry_has_one_cumulative_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let fs = Fs::memory();
        let cancellation = CancellationToken::new();
        let config = VolumeConfig::portable(Lifecycle::Ephemeral);
        let volume = fs
            .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        let mode = CheckoutMode {
            access: AccessMode::ReadWrite,
            consistency: ConsistencyMode::Manual,
            mutations: MutationMode::PrivateOverlay,
        };
        let mut first = volume
            .checkout(
                GenerationSelector::Head,
                mode,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let mut stale = volume
            .checkout(
                GenerationSelector::Head,
                mode,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let mut budgeted = volume
            .checkout(
                GenerationSelector::Head,
                mode,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        for (checkout, name) in [
            (&mut first, b"first".as_slice()),
            (&mut stale, b"stale"),
            (&mut budgeted, b"stale"),
        ] {
            let path = NamespacePath::new(
                vec![LogicalName::new(
                    NameEncoding::Utf8,
                    name.to_vec(),
                    config.limits.maximum_component_bytes,
                )?],
                config.limits,
            )?;
            checkout
                .create(crate::AuthoredMutation::CreateFile { path: path, bytes: (Bytes::from_static(b"data")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)
                .await?;
        }
        assert!(matches!(
            first
                .commit(OperationId::from_bytes([1; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })
                .await?
                .value,
            CheckoutCommitOutcome::Committed { .. }
        ));
        let conflict = stale
            .commit(OperationId::from_bytes([2; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })
            .await?;
        assert!(matches!(
            conflict.value,
            CheckoutCommitOutcome::Conflict { .. }
        ));
        assert!(conflict.work.authority_records_read > 0);
        let rebase = stale
            .rebase_head(
                config.limits.maximum_checkout_dependencies,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        assert!(matches!(rebase.value, RebaseDecision::Safe { .. }));
        assert!(rebase.work.authority_records_read > 0);

        // Each step individually fits; their sum does not. A per-attempt
        // reset would admit work beyond the caller's budget.
        let budget = WorkBudget {
            authority_records_read: conflict
                .work
                .authority_records_read
                .max(rebase.work.authority_records_read),
            ..WorkBudget::UNBOUNDED
        };
        let result = seal_checkout_with_permit_force(
            &mut budgeted,
            OperationId::from_bytes([3; 16]),
            PublicationPermit::Unrestricted,
            budget,
            &cancellation,
        )
        .await;
        assert!(
            matches!(
                result,
                Err(MountSourceError::Engine(ref error)) if error.contains("authority_records_read")
            ),
            "{result:?}"
        );
        assert!(budgeted.has_pending_mutations());
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[derive(Debug, thiserror::Error)]
    enum NativePublicationTestError {
        #[error(transparent)]
        Filesystem(#[from] crate::FsError),
        #[error(transparent)]
        Source(#[from] MountSourceError),
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_clean_source_seal_is_unchanged_without_append()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let fs = Fs::memory();
        let volume = fs.create_volume(
            VolumeConfig::portable(Lifecycle::Ephemeral), WorkBudget::UNBOUNDED, &cancellation,
        ).await?.value;
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let mut checkout = volume.checkout_with_native_work(
            GenerationSelector::Head, CheckoutMode::tracking_transaction(), &original, &cancellation,
        ).await?;
        let generation = checkout.generation_id();
        let before = original.receipt();
        let ack = seal_checkout_with_permit_guarded_with_native_work(
            &mut checkout, OperationId::from_bytes([0x61; 16]), PublicationPermit::Unrestricted,
            false, &original, &cancellation,
            &mut |_: &crate::NativeMaterializationWork| async { Ok::<(), NativePublicationTestError>(()) },
            None, None, None, None,
        ).await?;
        assert_eq!(ack, Some(super::super::adapter::NativeSourcePublicationAck::Unchanged { generation }));
        let after = original.receipt();
        assert_eq!(after.authority_records_appended, before.authority_records_appended);
        assert_eq!(after.backend_write_operations, before.backend_write_operations);
        assert_eq!(checkout.generation_id(), generation);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_source_lost_ack_reuses_original_context_and_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let simulation = crate::Simulation::default();
        let (authority, objects) = simulation.stores();
        let fs = Fs::new(authority, objects, crate::EmbeddedCapabilities::MEMORY);
        let volume = fs.create_volume(
            VolumeConfig::portable(Lifecycle::Ephemeral), WorkBudget::UNBOUNDED, &cancellation,
        ).await?.value;
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let mut checkout = volume.checkout_with_native_work(
            GenerationSelector::Head, CheckoutMode::tracking_transaction(), &original, &cancellation,
        ).await?;
        let generation = checkout.generation_id();
        let operation = OperationId::from_bytes([0x62; 16]);
        let context = crate::Digest::from_bytes([0x63; 32]);
        let (bytes, allocation) = original.copy_bytes(b"original-source-cut")?;
        let evidence = acyclic_stream::OwnedBytes::from_vec_with_guard(
            bytes, allocation, original.runtime_meter_owner(),
        ).map_err(|failure| format!("{:?}", failure.into_error()))?;
        simulation.schedule_next(
            crate::SimulationOperation::AuthorityAppend, crate::SimulationFault::AmbiguousAuthorityAppend,
        )?;
        let lost = seal_checkout_with_permit_guarded_with_native_work(
            &mut checkout, operation, PublicationPermit::Unrestricted, true, &original, &cancellation,
            &mut |_: &crate::NativeMaterializationWork| async { Ok::<(), NativePublicationTestError>(()) },
            None, Some(context), Some(evidence.clone()), None,
        ).await;
        assert!(matches!(lost, Err(NativePublicationTestError::Filesystem(crate::FsError::Authority(
            crate::AuthorityStoreError::Indeterminate { .. },
        )))));
        let appended = original.receipt().authority_records_appended;
        assert!(appended > 0);
        let mismatch = seal_checkout_with_permit_guarded_with_native_work(
            &mut checkout, operation, PublicationPermit::Unrestricted, true, &original, &cancellation,
            &mut |_: &crate::NativeMaterializationWork| async { Ok::<(), NativePublicationTestError>(()) },
            None, Some(crate::Digest::from_bytes([0x64; 32])), Some(evidence.clone()), None,
        ).await?;
        assert_eq!(mismatch, None);
        assert_eq!(original.receipt().authority_records_appended, appended);
        let (changed_bytes, changed_allocation) = original.copy_bytes(b"changed-source-cut")?;
        let changed_evidence = acyclic_stream::OwnedBytes::from_vec_with_guard(
            changed_bytes, changed_allocation, original.runtime_meter_owner(),
        ).map_err(|failure| format!("{:?}", failure.into_error()))?;
        let changed_payload = seal_checkout_with_permit_guarded_with_native_work(
            &mut checkout, operation, PublicationPermit::Unrestricted, true, &original, &cancellation,
            &mut |_: &crate::NativeMaterializationWork| async { Ok::<(), NativePublicationTestError>(()) },
            None, Some(context), Some(changed_evidence), None,
        ).await?;
        assert_eq!(changed_payload, None);
        assert_eq!(original.receipt().authority_records_appended, appended);
        let ack = seal_checkout_with_permit_guarded_with_native_work(
            &mut checkout, operation, PublicationPermit::Unrestricted, true, &original, &cancellation,
            &mut |_: &crate::NativeMaterializationWork| async { Ok::<(), NativePublicationTestError>(()) },
            None, Some(context), Some(evidence.clone()), None,
        ).await?;
        assert_eq!(ack, Some(super::super::adapter::NativeSourcePublicationAck::Published {
            operation, generation,
        }));
        assert_eq!(original.receipt().authority_records_appended, appended);
        let (durable, custody) =(async { let __authority_args = (crate::kernel::volume_authority_id(volume.id()), operation, &original, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = fs.authority().find_operation(__authority_args.0, __authority_args.1, &mut __authority_work).await.map(|(value, allocation)| (value, allocation.expect("native authority allocation"))); __authority_result }.await)?;
        let durable = durable.ok_or("original append disappeared")?;
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
        let decoded = crate::kernel::decode_publication_evidence_with_accounting(
            &durable.payload, u64::MAX, &mut accounting,
        )?;
        assert_eq!(decoded.original_evidence, Some(evidence.as_slice()));
        drop(durable);
        drop(custody);
        Ok(())
    }
}
