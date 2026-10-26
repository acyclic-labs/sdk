use super::*;
use crate::{AsyncAuthorityStore, AsyncObjectStore};

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "This state transition keeps admission, effects and resource retirement in one frame."
)]
pub(super) async fn publish_generation_async_inner<P, E, F, Fut, G, Dispatch, R>(
    owner: &P,
    request: PublishGenerationRequest,
    mut intent: PublicationIntent,
    closure_limits: ClosureLimits,
    cache: Option<&Mutex<CachedProofs>>,
    cancellation: &CancellationToken,
    accounting: &mut KernelAccounting<'_>,
    before_publication: &mut F,
    dispatch: &mut G,
    resolved: Option<crate::foundation::DurableCommit>,
    prepare_roots: &mut R,
    batch_seal: Option<combiner::SharedSeal>,
) -> Result<(PublicationReceipt, ProofGuard), E>
where
    P: PublicationOwner,
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
    let objects = owner.objects();
    let authority = owner.authority();
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
    let mut candidate = if let Some(record) = resolved.as_ref() {
        let decoded = decode_publication_evidence_with_accounting(
            &record.payload,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            accounting,
        )
        .map_err(E::from)?;
        combiner::Candidate {
            root: decoded.publication.generation_root,
            expected: request.expected,
            predecessor: None,
            ticket: None,
        }
    } else {
        combiner::prepare(
            owner,
            request,
            &mut intent,
            closure_limits,
            accounting,
            cancellation,
        )
        .await
        .map_err(E::from)?
    };
    let proven_at = objects.collection_sweeps();
    let prepared_authentication = candidate
        .ticket
        .as_ref()
        .map(|ticket| ticket.take_authentication(candidate.root, accounting))
        .transpose()
        .map_err(E::from)?
        .flatten();
    let authenticated = if let Some(authenticated) = prepared_authentication {
        authenticated
    } else {
        authenticate::prove(
            objects,
            candidate.root,
            candidate
                .ticket
                .as_ref()
                .map(combiner::Ticket::authority_root),
            closure_limits,
            cache,
            accounting,
            cancellation,
        )
        .await
        .map_err(E::from)?
    };
    let (proof, guard) = (authenticated.proof, authenticated.guard);
    let mut pending = authenticated.pending;
    // Field order keeps the concrete proof data physically ahead of its guard.
    let mut output = (proof, guard);
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
        Some(candidate.root),
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
    commit.publication = if let Some(ticket) = candidate.ticket.as_ref() {
        Some(ticket.changes(candidate.root).map_err(E::from)?)
    } else {
        crate::PublicationChanges::between(
            objects,
            authority,
            crate::publication_changes::PublicationBasis {
                request: PublishGenerationRequest {
                    generation_root: candidate.root,
                    expected: candidate.expected,
                    ..request
                },
                predecessor: candidate.predecessor,
            },
            &output.0.root,
            accounting,
            cancellation,
        )
        .await
        .map_err(E::from)?
    };
    // The lease spans validation and the authority CAS. It excludes new physical
    // projections; existing projections admit only candidates preserving policy.
    let (_projection_fence, projected) =
        crate::access::projection::publication(authority.operation_claims(), request.volume_id)
            .map_err(E::from)?;
    if projected {
        projection::check(
            objects,
            authority,
            projection::Candidate {
                request: PublishGenerationRequest {
                    expected: candidate.expected,
                    generation_root: candidate.root,
                    ..request
                },
                root: &output.0.root,
                lease: &_projection_fence,
            },
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
        expected: candidate.expected,
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
    let appended = if let Some(ticket) = candidate.ticket.as_mut() {
        ticket
            .submit(owner, append, batch_seal, accounting, cancellation)
            .await
            .map_err(E::from)
    } else {
        accept_step(accounting, dispatch(append, allowance).await)
    };
    if let Some(changes) = changes {
        changes.retire(accounting).map_err(E::from)?;
    }
    let appended = appended?;
    if let AppendOutcome::AlreadyCommitted(record) = &appended {
        let decoded = decode_publication_evidence_with_accounting(
            &record.payload,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            accounting,
        )
        .map_err(E::from)?;
        let root = decoded.publication.generation_root;
        if root.digest != output.0.generation_id.digest() {
            authenticate::retire(output, accounting).map_err(E::from)?;
            let authenticated = authenticate::prove(
                objects,
                root,
                None,
                closure_limits,
                cache,
                accounting,
                cancellation,
            )
            .await
            .map_err(E::from)?;
            output = (authenticated.proof, authenticated.guard);
            pending = authenticated.pending;
            prepare_roots(&output.0.root, &mut output.1, accounting)?;
        }
    }
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
        error => PublicationError::ChangeValidation(Box::new(error)),
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
