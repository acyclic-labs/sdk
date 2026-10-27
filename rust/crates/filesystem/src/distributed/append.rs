use super::*;

pub(super) async fn seal<G: AuthorityGuard>(
    work: &mut AuthorityWork<'_>,
    guard: Option<&mut G>,
) -> Result<(), AuthorityError<G::Error>> {
    work.check()?;
    let mut child = work.operation()?;
    let result = crate::async_storage::seal(guard, &mut child).await;
    work.absorb(&child)?;
    result?;
    work.check()?;
    Ok(())
}

mod group;
mod preparation;
mod quotation;
mod recovery;
mod view;
use view::AppendCursor;
pub(super) use view::AppendView;
#[cfg(test)]
pub(super) mod group_tests;

enum Preparation<T> {
    Ready(T),
    Outcome(FsAppendOutcome),
}

struct PreparedAppend<'a> {
    receipt: AppendReceipt<'a>,
    request: CommitRequest,
    deadline: Option<u64>,
}

// Purpose determines which capability may leave the canonical builder. A sizing
// command deliberately models fresh lineage and can only return a cost, never
// an append request whose absent-generation conditions could be submitted.
trait AppendPurpose {
    type Output<'a>;
    const FRESH_LINEAGE: bool;

    fn finish<'a, P: crate::distributed::AuthorityProvider + ?Sized>(
        self,
        store: &StreamAuthorityStore<P>,
        prepared: PreparedAppend<'a>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Self::Output<'a>, AuthorityStoreError>;
}

struct Publication;

impl AppendPurpose for Publication {
    type Output<'a> = PreparedAppend<'a>;
    const FRESH_LINEAGE: bool = false;

    fn finish<'a, P: crate::distributed::AuthorityProvider + ?Sized>(
        self,
        _: &StreamAuthorityStore<P>,
        prepared: PreparedAppend<'a>,
        _: &mut AuthorityWork<'_>,
    ) -> Result<Self::Output<'a>, AuthorityStoreError> {
        Ok(prepared)
    }
}

struct AppendReceipt<'a> {
    authority_id: AuthorityId,
    epoch: Epoch,
    commit: &'a ProposedCommit,
    durable: crate::streams_record::BorrowedStreamsDurableRecord<'a>,
    records: StreamPath,
    gate: StreamPath,
    lease_path: Option<StreamPath>,
}

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    pub(super) async fn append<G: AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<FsAppendOutcome, AuthorityError<G::Error>> {
        match self
            .compare_and_append_group(std::slice::from_ref(&request), &[], work, guard)
            .await?
        {
            AppendGroupOutcome::Rejected {
                index: 0, outcome, ..
            } => Ok(outcome),
            AppendGroupOutcome::Published {
                mut receipts,
                allocation,
            } => {
                if receipts.len() != 1 {
                    return Err(
                        authority_corrupt("single append has multiple receipts", work).into(),
                    );
                }
                let outcome = receipts
                    .pop()
                    .ok_or_else(|| authority_corrupt("single append has no receipt", work))?;
                drop(receipts);
                drop(allocation);
                Ok(outcome)
            }
            AppendGroupOutcome::Rejected { .. } => {
                Err(authority_corrupt("single append rejected another member", work).into())
            }
        }
    }

    async fn recover_append_error<G: AuthorityGuard>(
        &self,
        prepared: &AppendReceipt<'_>,
        error: AuthorityError<G::Error>,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<FsAppendOutcome, AuthorityError<G::Error>> {
        match error {
            AuthorityError::Authority(AuthorityStoreError::NativeStream(
                acyclic_stream::StreamError::IdempotencyMismatch,
            )) => {
                let existing = self
                    .authority_find_operation(
                        prepared.authority_id,
                        prepared.commit.operation_id,
                        work,
                    )
                    .await?
                    .ok_or_else(|| {
                        authority_corrupt(
                            "Stream idempotency mismatch has no operation record",
                            work,
                        )
                    })?;
                seal(work, guard).await?;
                Ok(if existing.fingerprint == prepared.commit.fingerprint {
                    FsAppendOutcome::AlreadyCommitted(existing)
                } else {
                    FsAppendOutcome::IdempotencyConflict {
                        committed_fingerprint: existing.fingerprint,
                    }
                })
            }
            AuthorityError::Authority(AuthorityStoreError::NativeStream(
                acyclic_stream::StreamError::DeadlineElapsed,
            )) => Ok(FsAppendOutcome::Fenced {
                actual_epoch: prepared.epoch,
            }),
            error => Err(error),
        }
    }
    async fn settle_append<G: AuthorityGuard>(
        prepared: &AppendReceipt<'_>,
        outcome: &CommitOutcome,
        actual: Option<Head>,
        recovered: Option<DurableCommit>,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<FsAppendOutcome, AuthorityError<G::Error>> {
        let epoch = prepared.epoch;
        let commit = prepared.commit;
        let records = &prepared.records;
        let gate = &prepared.gate;
        let lease_path = &prepared.lease_path;
        let durable = prepared.durable;
        let sequence = durable.sequence;
        match outcome {
            CommitOutcome::Committed(envelope) => {
                envelope
                    .appends
                    .iter()
                    .find(|append| {
                        &append.path == records
                            && append.start <= sequence.get()
                            && sequence.get() < append.tail
                    })
                    .ok_or_else(|| {
                        authority_corrupt("commit envelope omits its authority record", work)
                    })?;
                Ok(FsAppendOutcome::Committed(durable_commit(
                    durable,
                    envelope.committed_at_micros,
                    commit.payload.clone(),
                )))
            }
            CommitOutcome::Conflict(conflicts) => {
                if let Some(existing) = recovered {
                    seal(work, guard).await?;
                    return Ok(if existing.fingerprint == commit.fingerprint {
                        FsAppendOutcome::AlreadyCommitted(existing)
                    } else {
                        FsAppendOutcome::IdempotencyConflict {
                            committed_fingerprint: existing.fingerprint,
                        }
                    });
                }
                let actual = actual.ok_or_else(|| {
                    authority_corrupt("conflicted append has no recovered head", work)
                })?;
                let rejected = conflicts.iter().any(|conflict| {
                    let path = match conflict {
                        acyclic_stream::CommitConflict::Tail { path, .. }
                        | acyclic_stream::CommitConflict::Head { path, .. }
                        | acyclic_stream::CommitConflict::Missing { path }
                        | acyclic_stream::CommitConflict::Exists { path } => path,
                    };
                    path == gate || lease_path.as_ref().is_some_and(|lease| path == lease)
                });
                Ok(if rejected || actual.epoch != epoch {
                    FsAppendOutcome::Fenced {
                        actual_epoch: actual.epoch,
                    }
                } else {
                    FsAppendOutcome::Conflict { actual }
                })
            }
        }
    }
}

fn permits_append(
    permit: PublicationPermit,
    authority_id: AuthorityId,
    expected: Head,
    gate_tail: u64,
    active: Option<OperationId>,
) -> bool {
    match permit {
        PublicationPermit::Unrestricted => active.is_none(),
        PublicationPermit::Reservation {
            operation_id,
            gate_tail: tail,
            expected: head,
        } => {
            tail == gate_tail
                && active == Some(OperationId::from_bytes(operation_id))
                && head == expected
        }
        PublicationPermit::Lease {
            authority_id: authority,
            workspace_id,
            ..
        } => {
            active.is_none()
                && AuthorityId::from_bytes(authority) == authority_id
                && volume_authority_id(crate::WorkspaceId::from_bytes(workspace_id).volume_id())
                    == authority_id
        }
    }
}
