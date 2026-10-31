use super::*;

pub(super) struct PublicationCallbacks<'a, F, G, R> {
    pub(super) before_publication: &'a mut F,
    pub(super) dispatch: &'a mut G,
    pub(super) prepare_roots: &'a mut R,
    pub(super) batch_seal: Option<combiner::SharedSeal>,
}

pub(super) async fn publish_generation_async_inner<P, E, F, Fut, G, Dispatch, R>(
    preparation: preparation::Preparation<'_, P>,
    accounting: &mut KernelAccounting<'_>,
    callbacks: PublicationCallbacks<'_, F, G, R>,
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
    let owner = preparation.owner;
    let closure_limits = preparation.limits;
    let cache = preparation.cache;
    let cancellation = preparation.cancellation;
    let PublicationCallbacks {
        before_publication,
        dispatch,
        prepare_roots,
        batch_seal,
    } = callbacks;
    let prepared = match preparation.prepare(accounting, prepare_roots).await? {
        preparation::Prepared::Complete(output) => return Ok(output),
        preparation::Prepared::Ready(prepared) => prepared,
    };
    let preparation::PreparedGeneration {
        mut candidate,
        authentication,
        append,
        projection_fence: _projection_fence,
        publication_hold: _publication_hold,
    } = prepared;
    let objects = owner.objects();
    let mut output = (authentication.proof, authentication.guard);
    let mut pending = authentication.pending;
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
        if root != output.0.generation_id.object_id() {
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
