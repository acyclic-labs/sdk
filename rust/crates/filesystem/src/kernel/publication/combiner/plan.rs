use super::*;

pub(in crate::kernel::publication) struct Candidate {
    pub(in crate::kernel::publication) root: ObjectId,
    pub(in crate::kernel::publication) expected: Head,
    pub(in crate::kernel::publication) predecessor: Option<ObjectId>,
    pub(in crate::kernel::publication) ticket: Option<Ticket>,
}

pub(in crate::kernel::publication) async fn prepare<P: PublicationOwner>(
    owner: &P,
    request: PublishGenerationRequest,
    intent: &mut PublicationIntent,
    limits: ClosureLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Candidate, crate::FsError> {
    let authority = owner.authority();
    let mut candidate = Candidate {
        root: request.generation_root,
        expected: request.expected,
        predecessor: None,
        ticket: None,
    };
    if !authority.supports_atomic_group_commit()
        || matches!(intent.permit, PublicationPermit::Reservation { .. })
    {
        return Ok(candidate);
    }
    let Some(preconditions) = intent.preconditions.take() else {
        return Ok(candidate);
    };
    if preconditions.head != request.expected {
        return Ok(candidate);
    }
    let Some(mut ticket) = Ticket::enter(
        owner,
        (request, preconditions, intent),
        limits,
        accounting,
        cancellation,
    )
    .await?
    else {
        return Ok(candidate);
    };
    let (root, predecessor) = loop {
        if let Some(prefix) = ticket
            .prepare_prefix(owner.clone(), limits, accounting, cancellation)
            .await?
        {
            break prefix;
        }
        let certified = ticket.certificate(accounting)?;
        let core = Arc::clone(&ticket.proposal.core);
        drop(ticket);
        let Some(next) =
            Ticket::enter_core(owner, core, (limits, certified), accounting, cancellation).await?
        else {
            return Ok(candidate);
        };
        ticket = next;
    };
    let anchor = ticket.anchor()?;
    candidate.root = root;
    candidate.expected = anchor.head;
    candidate.predecessor = predecessor;
    candidate.ticket = Some(ticket);
    Ok(candidate)
}

pub(super) async fn compose_metadata<O: crate::AsyncObjectStore>(
    objects: &O,
    predecessor: ObjectId,
    composition: Composition<'_>,
    limits: crate::kernel::DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<ObjectId, crate::FsError> {
    let (root, allocation) = crate::kernel::read_generation_root_with_limits_and_accounting(
        objects,
        predecessor,
        limits,
        accounting,
        cancellation,
    )
    .await?;
    let bytes = usize_to_u64(root.parents.capacity() * std::mem::size_of::<crate::GenerationId>());
    let root = Owned::new(root, allocation, bytes);
    let table = crate::kernel::apply_generation_delta(
        objects,
        root.value.file_table,
        composition.delta,
        limits,
        accounting,
        cancellation,
    )
    .await;
    let table = root.retire(accounting).and(table)?;
    crate::kernel::build_checkpoint_with_accounting(
        objects,
        crate::kernel::CheckpointRequest {
            base: predecessor,
            file_table: table,
            merge_parent: composition.retained_merge,
        },
        limits,
        accounting,
        cancellation,
    )
    .await
}
