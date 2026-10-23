use super::*;

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "This state transition keeps admission, effects and resource retirement in one frame."
)]
pub(super) async fn publish_generation_async_inner<O, A, E, F, Fut, G, Dispatch, R>(
    objects: &O,
    authority: &A,
    request: PublishGenerationRequest,
    intent: PublicationIntent,
    closure_limits: ClosureLimits,
    cache: Option<&Mutex<CachedProofs>>,
    cancellation: &CancellationToken,
    accounting: &mut KernelAccounting<'_>,
    before_publication: &mut F,
    dispatch: &mut G,
    resolved: Option<crate::foundation::DurableCommit>,
    prepare_roots: &mut R,
) -> Result<(PublicationReceipt, ProofGuard), E>
where
    O: crate::AsyncObjectStore,
    A: crate::AsyncAuthorityStore,
    E: From<crate::FsError> + Send,
    F: FnMut(Option<WorkBudget>) -> Fut + ProviderTask,
    Fut: Future<Output = Result<Option<WorkCounters>, PublicationStepFailure<E>>> + ProviderTask,
    G: FnMut(GuardedAppend, Option<WorkBudget>) -> Dispatch + acyclic_stream::ProviderTask,
    Dispatch: Future<Output = Result<(AppendOutcome, Option<WorkCounters>), PublicationStepFailure<E>>>
        + acyclic_stream::ProviderTask,
    R: FnMut(
        &crate::kernel::GenerationRoot,
        &mut ProofGuard,
        &mut KernelAccounting<'_>,
    ) -> Result<(), E>,
{
    let expected_authority = volume_authority_id(request.volume_id);
    if request.authority_id != expected_authority {
        return Err(E::from(crate::FsError::Publication(
            PublicationError::AuthorityMismatch {
                volume_id: request.volume_id,
                expected: expected_authority,
                actual: request.authority_id,
            },
        )));
    }
    cancellation
        .check()
        .map_err(crate::FsError::from)
        .map_err(E::from)?;
    accounting.check().map_err(E::from)?;
    let proven_at = objects.collection_sweeps();
    let (proof, guard, pending) = match accounting {
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { work: original, .. } => {
            crate::kernel::closure::ProofContext::new(
                objects,
                closure_limits,
                crate::kernel::closure::ProofMeter::Native {
                    original,
                    cancellation,
                },
            )
            .prove_cached(request.generation_root, cache)
            .await
            .map_err(E::from)?
        }
        _ => {
            let (_, work, budget, _) = accounting
                .standalone_parts()
                .ok_or_else(|| E::from(crate::FsError::Work(WorkError::Overflow)))?;
            if request.generation_root.kind != ObjectKind::GenerationRoot {
                *work = WorkCounters::UNCHARGED;
                return Err(E::from(crate::FsError::Closure(
                    ClosureError::WrongObjectKind,
                )));
            }
            if closure_limits.maximum_objects == 0 {
                *work = WorkCounters::UNCHARGED;
                return Err(E::from(crate::FsError::Closure(
                    ClosureError::TooManyObjects {
                        observed: 1,
                        maximum: 0,
                    },
                )));
            }
            let meter = crate::kernel::closure::ProofMeter::standalone(budget, cancellation)
                .map_err(|error| {
                    *work = WorkCounters::UNCHARGED;
                    E::from(crate::FsError::Closure(
                        crate::kernel::closure::closure_error(error),
                    ))
                })?;
            match crate::kernel::closure::ProofContext::new(objects, closure_limits, meter.clone())
                .prove_cached(request.generation_root, cache)
                .await
            {
                Ok((proof, guard, pending)) => {
                    *work = proof.work;
                    (proof, guard, pending)
                }
                Err(failure) => {
                    *work = meter.work();
                    return Err(E::from(crate::FsError::Closure(
                        crate::kernel::closure::closure_error(failure),
                    )));
                }
            }
        }
    };
    // Field order keeps the concrete proof data physically ahead of its guard.
    let mut output = (proof, guard);
    match &output.1 {
        ProofGuard::Standalone { .. } => {
            retain(accounting, output.1.retained_bytes()).map_err(E::from)?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        ProofGuard::Native(_) => {}
    }
    if output.0.root.volume_id != request.volume_id {
        return Err(E::from(crate::FsError::Publication(
            PublicationError::VolumeMismatch {
                expected: request.volume_id,
                actual: output.0.root.volume_id,
            },
        )));
    }
    let fingerprint_permit = intent.fingerprint_permit.unwrap_or(intent.permit);
    let operation_context =
        permit_context(intent.operation_context, fingerprint_permit, accounting)
            .map_err(E::from)?;
    let mut commit = prepare_publication_commit(
        request,
        operation_context,
        intent.original_evidence,
        accounting,
        cancellation,
    )
    .map_err(E::from)?;
    if let Some(resolved) = resolved.as_ref()
        && resolved.fingerprint != commit.fingerprint
    {
        return Ok((
            PublicationReceipt {
                proof: output.0,
                outcome: AppendOutcome::IdempotencyConflict {
                    committed_fingerprint: resolved.fingerprint,
                },
                work: accounting.spent(),
            },
            output.1,
        ));
    }
    prepare_roots(&output.0.root, &mut output.1, accounting)?;
    if let Some(resolved) = resolved {
        let outcome = AppendOutcome::AlreadyCommitted(resolved);
        if let Some(pending) = pending {
            pending.publish(&outcome);
        }
        return Ok((
            PublicationReceipt {
                proof: output.0,
                outcome,
                work: accounting.spent(),
            },
            output.1,
        ));
    }
    commit.publication = crate::PublicationChanges::between(
        objects, authority, request, &output.0.root, accounting, cancellation,
    ).await.map_err(E::from)?;
    // The lease spans validation and the authority CAS. It excludes new physical
    // projections; existing projections admit only candidates preserving policy.
    let (_projection_fence, projected) =
        crate::access::projection::publication(authority.operation_claims(), request.volume_id)
            .map_err(E::from)?;
    if projected {
        projection::check(
            objects,
            authority,
            projection::Candidate { request, root: &output.0.root, lease: &_projection_fence },
            closure_limits.decode,
            accounting,
            cancellation,
        )
        .await
        .map_err(E::from)?;
    }
    let scope = if cache.is_some() && objects.collection().is_none() {
        crate::PublicationScope::Everything
    } else {
        crate::PublicationScope::Closure {
            objects: &output.0.objects,
            proven_at,
        }
    };
    let _publication_hold = match accounting {
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { work: original, .. } => {
            let mut work = crate::AuthorityWork::native(original, cancellation);
            objects
                .flush_before_publish(scope, &mut work)
                .await
                .map_err(native_object_error)
                .map_err(E::from)?
        }
        _ => {
            let mut work =
                crate::AuthorityWork::new(accounting.remaining().map_err(E::from)?, cancellation);
            let result = objects.flush_before_publish(scope, &mut work).await;
            let result = work
                .receipt(result)
                .map(|receipt| (receipt.value, Some(receipt.work)))
                .map_err(|failure| PublicationStepFailure {
                    cause: E::from(crate::FsError::Object(failure.error)),
                    work: Some(*failure.work),
                });
            accept_step(accounting, result)?
        }
    };
    let append = crate::GuardedAppend {
        authority_id: request.authority_id,
        epoch: request.epoch,
        expected: request.expected,
        commit,
        permit: intent.permit,
    };
    if matches!(
        accounting,
        KernelAccounting::Standalone { .. } | KernelAccounting::BorrowedStandalone { .. }
    ) {
        let checked = before_publication(Some(accounting.remaining().map_err(E::from)?)).await;
        accept_step(accounting, checked.map(|work| ((), work)))?;
    }
    cancellation
        .check()
        .map_err(crate::FsError::from)
        .map_err(E::from)?;
    let allowance = match accounting {
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { .. } => None,
        _ => Some(accounting.remaining().map_err(E::from)?),
    };
    let changes = append.commit.publication.clone();
    let appended = accept_step(accounting, dispatch(append, allowance).await);
    if let Some(changes) = changes {
        changes.retire(accounting).map_err(E::from)?;
    }
    let appended = appended?;
    // Only a durable generation's proof may stand in for its closure later.
    if let Some(pending) = pending {
        pending.publish(&appended);
    }
    Ok((
        PublicationReceipt {
            proof: output.0,
            outcome: appended,
            work: accounting.spent(),
        },
        output.1,
    ))
}

pub(super) fn publication_error(error: crate::FsError) -> PublicationError {
    match error {
        crate::FsError::Publication(error) => error,
        crate::FsError::Closure(error) => error.into(),
        crate::FsError::Decode(error) => ClosureError::Decode(error).into(),
        crate::FsError::Object(error) => error.into(),
        crate::FsError::Authority(error) => error.into(),
        crate::FsError::Work(error) => error.into(),
        crate::FsError::Cancelled(_) => AuthorityStoreError::Cancelled.into(),
        _ => unreachable!("unguarded publication has no caller-owned error"),
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn native_object_error(error: crate::storage::ObjectStoreError) -> crate::FsError {
    match error {
        crate::storage::ObjectStoreError::Native(error) => crate::FsError::NativeKernel(error),
        error => crate::FsError::Object(error),
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn native_authority_error(error: AuthorityStoreError) -> crate::FsError {
    match error {
        AuthorityStoreError::NativeOriginal(error) => crate::FsError::NativeKernel(error),
        error => crate::FsError::Authority(error),
    }
}
