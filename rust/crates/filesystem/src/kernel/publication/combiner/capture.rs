//! Capture the immutable proposal once; later rounds only retarget its seals.
use super::*;

pub(super) fn quote(
    request: PublishGenerationRequest,
    intent: &PublicationIntent,
    changes: crate::PublicationChanges,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(GuardedAppend, usize), crate::FsError> {
    let evidence = intent
        .original_evidence
        .as_ref()
        .map(|data| data.as_slice());
    let composed_length = publication_payload_length_with_evidence(
        evidence,
        Some(PublicationOrigin {
            expected: request.expected,
            generation_root: request.generation_root,
        }),
    )?;
    let operation_context = permit_context(
        intent.operation_context,
        intent.fingerprint_permit.unwrap_or(intent.permit),
        accounting,
    )?;
    let mut commit = prepare_publication_commit(
        request,
        None,
        operation_context,
        intent.original_evidence.clone(),
        accounting,
        cancellation,
    )?;
    let growth = composed_length
        .checked_sub(commit.payload.len())
        .ok_or(WorkError::Overflow)?;
    commit.publication = Some(changes);
    Ok((
        GuardedAppend {
            authority_id: request.authority_id,
            epoch: request.epoch,
            expected: request.expected,
            commit,
            permit: intent.permit,
        },
        growth,
    ))
}

impl ProposalCore {
    pub(super) async fn capacity<A: crate::AsyncAuthorityStore>(
        &self,
        authority: &A,
        anchor: Anchor,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<crate::AuthorityGroupCost>, crate::FsError> {
        let cached = *self
            .cost
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cached.is_some() {
            return Ok(cached);
        }
        let mut quote = self.quote.clone();
        quote.expected = anchor.head;
        quote.commit.publication = Some(self.changes(anchor.head, self.request.generation_root)?);
        let mut work = dispatch::backend_work(accounting, cancellation)?;
        let cost = authority
            .quote_group_append(&quote, self.growth, &mut work)
            .await;
        dispatch::merge_work(accounting, &work)?;
        let cost = cost?;
        *self
            .cost
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = cost;
        Ok(cost)
    }

    fn changes(
        &self,
        expected: Head,
        root: ObjectId,
    ) -> Result<crate::PublicationChanges, crate::FsError> {
        self.quote
            .commit
            .publication
            .as_ref()
            .ok_or(crate::FsError::InvalidDiff)?
            .composed(self.request.expected, expected, root)
    }
}

impl Ticket {
    pub(in crate::kernel::publication) fn changes(
        &self,
        root: ObjectId,
    ) -> Result<crate::PublicationChanges, crate::FsError> {
        self.proposal
            .core
            .changes(self.proposal.admission()?.anchor.head, root)
    }
}
