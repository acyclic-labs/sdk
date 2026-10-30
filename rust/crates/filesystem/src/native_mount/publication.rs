//! One fenced publication path shared by native mounts and host watchers.

use crate::MountSourceError;
use crate::model::MutationMode;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Checkout, LiveMutationOutcome,
    LivePublicationObservation, OperationId, PublicationPermit, WorkBudget,
};

#[derive(Default)]
pub(super) struct PublicationProof<'a> {
    pub(super) producer: Option<&'a std::sync::Arc<crate::kernel::ProofDataCustody>>,
    pub(super) context: Option<crate::Digest>,
    pub(super) evidence: Option<acyclic_stream::OwnedBytes>,
    pub(super) fingerprint_permit: Option<PublicationPermit>,
}

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
    cancellation
        .check()
        .map_err(|_| MountSourceError::Cancelled)?;
    if !checkout.has_pending_mutations() {
        return Ok(());
    }
    match checkout.mode().mutations {
        MutationMode::PrivateOverlay => {
            let receipt = checkout
                .commit(
                    operation_id,
                    None,
                    None,
                    None,
                    permit,
                    budget,
                    cancellation,
                    |_| async {
                        crate::FsResult::Ok(crate::OperationReceipt {
                            value: (),
                            work: crate::WorkCounters::UNCHARGED,
                        })
                    },
                )
                .await
                .map_err(|failure| publication_error(failure.error))?;
            private_ack(operation_id, receipt.value)
                .map(|_| ())
                .ok_or(MountSourceError::Stale)
        }
        MutationMode::DirectLive if permit == PublicationPermit::Unrestricted => {
            let receipt = checkout
                .resume_live(operation_id, budget, cancellation)
                .await
                .map_err(|failure| publication_error(failure.error))?;
            live_ack(operation_id, &receipt.value)
                .map(|_| ())
                .ok_or(MountSourceError::Stale)
        }
        MutationMode::DirectLive => Err(MountSourceError::Unsupported(
            "direct-live mounts do not support operation lease permits".into(),
        )),
        MutationMode::None => Err(MountSourceError::Unsupported(
            "checkout does not admit native writes".into(),
        )),
    }
}

/// Publishes the retained operation on its original allocation family.
/// `None` is a deterministic SDK rejection; an unchanged checkout is not an ACK.
pub(super) async fn seal_checkout_with_permit_guarded_with_native_work<'a, A, O, T, F, Fut>(
    checkout: &'a mut Checkout<A, O>,
    operation_id: OperationId,
    permit: PublicationPermit,
    force: bool,
    accounting: crate::kernel::KernelAccounting<'a>,
    before_publication: &'a mut F,
    proof: PublicationProof<'a>,
) -> Result<Option<(Option<OperationId>, crate::GenerationId)>, T>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    T: From<crate::FsError> + From<MountSourceError> + Send + 'static,
    F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Result<(), T>> + Send + 'static,
{
    let crate::kernel::KernelAccounting::NativeOriginal {
        work: original,
        cancellation,
    } = accounting
    else {
        return Err(T::from(MountSourceError::OriginalSnapshotUnavailable));
    };
    cancellation
        .check()
        .map_err(|error| T::from(crate::FsError::Cancelled(error)))?;
    let mode = checkout.mode().mutations;
    if mode != MutationMode::PrivateOverlay
        && (force || permit != PublicationPermit::Unrestricted || mode == MutationMode::None)
    {
        return Err(T::from(MountSourceError::NativeUnsupported));
    }
    if mode == MutationMode::PrivateOverlay
        && let Some(outcome) = checkout
            .retry_stale_commit_with_native_work(
                operation_id,
                permit,
                original,
                cancellation,
                before_publication,
                proof.producer,
                proof.context,
                proof.evidence.clone(),
                proof.fingerprint_permit,
            )
            .await?
    {
        return Ok(private_ack(operation_id, outcome));
    }
    if !force && !checkout.has_pending_mutations() {
        return Ok(Some((None, checkout.generation_id())));
    }
    if mode == MutationMode::PrivateOverlay {
        original
            .charge(crate::materializer::NativeResourceCredit::Items.delta(1))
            .map_err(|error| T::from(MountSourceError::NativeWork(error)))?;
        let outcome = checkout
            .commit_even_if_clean_with_native_work(
                operation_id,
                permit,
                original,
                cancellation,
                before_publication,
                proof.producer,
                proof.context,
                proof.evidence,
                proof.fingerprint_permit,
            )
            .await?;
        Ok(private_ack(operation_id, outcome))
    } else {
        let (outcome, allocation) = checkout
            .resume_live_with_native_work(
                operation_id,
                permit,
                original,
                cancellation,
                before_publication,
                proof.producer,
                proof.context,
                proof.evidence,
                proof.fingerprint_permit,
            )
            .await?;
        let ack = live_ack(operation_id, &outcome);
        drop(allocation);
        Ok(ack)
    }
}

fn private_ack(
    operation: OperationId,
    outcome: LivePublicationObservation,
) -> Option<(Option<OperationId>, crate::GenerationId)> {
    match outcome {
        LivePublicationObservation::Committed { generation_id, .. }
        | LivePublicationObservation::AlreadyCommitted { generation_id, .. } => {
            Some((Some(operation), generation_id))
        }
        _ => None,
    }
}

fn live_ack(
    operation: OperationId,
    outcome: &LiveMutationOutcome,
) -> Option<(Option<OperationId>, crate::GenerationId)> {
    match outcome {
        LiveMutationOutcome::Committed { generation_id, .. }
        | LiveMutationOutcome::AlreadyCommitted { generation_id, .. } => {
            Some((Some(operation), *generation_id))
        }
        _ => None,
    }
}

fn engine_error(error: impl std::fmt::Display) -> MountSourceError {
    MountSourceError::Engine(std::borrow::Cow::Owned(error.to_string()))
}

fn publication_error(error: crate::FsError) -> MountSourceError {
    match error {
        crate::FsError::Work(error) => MountSourceError::Work(error),
        crate::FsError::Cancelled(_) => MountSourceError::Cancelled,
        error => engine_error(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Fs;
    use crate::model::{CheckoutMode, GenerationSelector, Lifecycle, VolumeConfig};

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
    async fn native_source_head_conflict_returns_without_rebase()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let fs = Fs::memory();
        let volume = fs
            .create_volume(
                VolumeConfig::portable(Lifecycle::Ephemeral),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let mut winner = volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                &original,
                &cancellation,
            )
            .await?;
        let mut stale = volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                &original,
                &cancellation,
            )
            .await?;
        let generation = stale.generation_id();
        let winning_operation = OperationId::from_bytes([0x65; 16]);
        let stale_operation = OperationId::from_bytes([0x66; 16]);
        let mut guard = |_: &crate::NativeMaterializationWork| async {
            Ok::<(), NativePublicationTestError>(())
        };
        let won = seal_checkout_with_permit_guarded_with_native_work(
            &mut winner,
            winning_operation,
            PublicationPermit::Unrestricted,
            true,
            crate::kernel::KernelAccounting::native(&original, &cancellation),
            &mut guard,
            PublicationProof::default(),
        )
        .await?;
        assert_eq!(won, Some((Some(winning_operation), generation)));
        let lost = seal_checkout_with_permit_guarded_with_native_work(
            &mut stale,
            stale_operation,
            PublicationPermit::Unrestricted,
            true,
            crate::kernel::KernelAccounting::native(&original, &cancellation),
            &mut guard,
            PublicationProof::default(),
        )
        .await?;
        assert_eq!(lost, None);
        assert_eq!(stale.generation_id(), generation);
        let (decision, allocation) = stale.rebase_head(1, &original, &cancellation).await?;
        assert!(matches!(
            decision,
            crate::kernel::RebaseDecision::Safe { .. }
        ));
        drop(allocation);
        let published = seal_checkout_with_permit_guarded_with_native_work(
            &mut stale,
            stale_operation,
            PublicationPermit::Unrestricted,
            true,
            crate::kernel::KernelAccounting::native(&original, &cancellation),
            &mut guard,
            PublicationProof::default(),
        )
        .await?;
        assert_eq!(published, Some((Some(stale_operation), generation)));
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_clean_source_seal_is_unchanged_without_append()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let fs = Fs::memory();
        let volume = fs
            .create_volume(
                VolumeConfig::portable(Lifecycle::Ephemeral),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let mut checkout = volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                &original,
                &cancellation,
            )
            .await?;
        let generation = checkout.generation_id();
        let before = original.receipt();
        let ack = seal_checkout_with_permit_guarded_with_native_work(
            &mut checkout,
            OperationId::from_bytes([0x61; 16]),
            PublicationPermit::Unrestricted,
            false,
            crate::kernel::KernelAccounting::native(&original, &cancellation),
            &mut |_: &crate::NativeMaterializationWork| async {
                Ok::<(), NativePublicationTestError>(())
            },
            PublicationProof {
                producer: None,
                context: None,
                evidence: None,
                fingerprint_permit: None,
            },
        )
        .await?;
        assert_eq!(ack, Some((None, generation)));
        let after = original.receipt();
        assert_eq!(
            after.authority_records_appended,
            before.authority_records_appended
        );
        assert_eq!(
            after.backend_write_operations,
            before.backend_write_operations
        );
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
        let volume = fs
            .create_volume(
                VolumeConfig::portable(Lifecycle::Ephemeral),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let mut checkout = volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                &original,
                &cancellation,
            )
            .await?;
        let generation = checkout.generation_id();
        let operation = OperationId::from_bytes([0x62; 16]);
        let context = crate::Digest::from_bytes([0x63; 32]);
        let (bytes, allocation) = original.copy_bytes(b"original-source-cut")?;
        let evidence = acyclic_stream::OwnedBytes::from_vec_with_guard(
            bytes,
            allocation,
            original.runtime_meter(),
        )
        .map_err(|failure| format!("{:?}", failure.into_error()))?;
        simulation.schedule_next(
            crate::SimulationOperation::AuthorityAppend,
            crate::SimulationFault::AmbiguousAuthorityAppend,
        )?;
        let lost = seal_checkout_with_permit_guarded_with_native_work(
            &mut checkout,
            operation,
            PublicationPermit::Unrestricted,
            true,
            crate::kernel::KernelAccounting::native(&original, &cancellation),
            &mut |_: &crate::NativeMaterializationWork| async {
                Ok::<(), NativePublicationTestError>(())
            },
            PublicationProof {
                producer: None,
                context: Some(context),
                evidence: Some(evidence.clone()),
                fingerprint_permit: None,
            },
        )
        .await;
        assert!(matches!(
            lost,
            Err(NativePublicationTestError::Filesystem(
                crate::FsError::Authority(crate::AuthorityStoreError::Indeterminate { .. },)
            ))
        ));
        let appended = original.receipt().authority_records_appended;
        assert!(appended > 0);
        let mismatch = seal_checkout_with_permit_guarded_with_native_work(
            &mut checkout,
            operation,
            PublicationPermit::Unrestricted,
            true,
            crate::kernel::KernelAccounting::native(&original, &cancellation),
            &mut |_: &crate::NativeMaterializationWork| async {
                Ok::<(), NativePublicationTestError>(())
            },
            PublicationProof {
                producer: None,
                context: Some(crate::Digest::from_bytes([0x64; 32])),
                evidence: Some(evidence.clone()),
                fingerprint_permit: None,
            },
        )
        .await?;
        assert_eq!(mismatch, None);
        assert_eq!(original.receipt().authority_records_appended, appended);
        let (changed_bytes, changed_allocation) = original.copy_bytes(b"changed-source-cut")?;
        let changed_evidence = acyclic_stream::OwnedBytes::from_vec_with_guard(
            changed_bytes,
            changed_allocation,
            original.runtime_meter(),
        )
        .map_err(|failure| format!("{:?}", failure.into_error()))?;
        let changed_payload = seal_checkout_with_permit_guarded_with_native_work(
            &mut checkout,
            operation,
            PublicationPermit::Unrestricted,
            true,
            crate::kernel::KernelAccounting::native(&original, &cancellation),
            &mut |_: &crate::NativeMaterializationWork| async {
                Ok::<(), NativePublicationTestError>(())
            },
            PublicationProof {
                producer: None,
                context: Some(context),
                evidence: Some(changed_evidence),
                fingerprint_permit: None,
            },
        )
        .await?;
        assert_eq!(changed_payload, None);
        assert_eq!(original.receipt().authority_records_appended, appended);
        let ack = seal_checkout_with_permit_guarded_with_native_work(
            &mut checkout,
            operation,
            PublicationPermit::Unrestricted,
            true,
            crate::kernel::KernelAccounting::native(&original, &cancellation),
            &mut |_: &crate::NativeMaterializationWork| async {
                Ok::<(), NativePublicationTestError>(())
            },
            PublicationProof {
                producer: None,
                context: Some(context),
                evidence: Some(evidence.clone()),
                fingerprint_permit: None,
            },
        )
        .await?;
        assert_eq!(ack, Some((Some(operation), generation)));
        assert_eq!(original.receipt().authority_records_appended, appended);
        let durable = (async {
            let __authority_args = (
                crate::kernel::volume_authority_id(volume.id()),
                operation,
                &original,
                &cancellation,
            );
            let mut __authority_work =
                crate::AuthorityWork::native(__authority_args.2, __authority_args.3);
            fs.authority()
                .find_operation(
                    __authority_args.0,
                    __authority_args.1,
                    &mut __authority_work,
                )
                .await
        }
        .await)?;
        let durable = durable.ok_or("original append disappeared")?;
        let mut accounting =
            crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
        let decoded = crate::kernel::decode_publication_evidence_with_accounting(
            &durable.payload,
            u64::MAX,
            &mut accounting,
        )?;
        assert_eq!(decoded.original_evidence, Some(evidence.as_slice()));
        drop(durable);
        Ok(())
    }
}
