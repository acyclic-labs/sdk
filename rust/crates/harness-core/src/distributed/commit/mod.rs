use super::*;
use acyclic_stream::StreamProviderExt as _;

mod observation;
mod request;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod retained;
mod sdk;
pub(super) use observation::observe_commit;
pub(crate) use request::{add_condition, commit_request};
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use retained::NativeCommit;
pub(super) use sdk::inspect_idempotency;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use sdk::native_call_result;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitPhase {
    Uncommitted,
    Unknown,
    Committed,
}

struct Admission {
    binding: acyclic_stream::request::CommitBinding,
    vectors: u64,
    acknowledgment: (u64, u64),
}

fn admit(
    request: &mut acyclic_stream::CommitRequest,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<Admission> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if quota
        .work()
        .as_deref()
        .is_some_and(|io| io.requires_prepared_native_dispatch())
    {
        return Err(guarded_boundary(
            quota.work(),
            wire::ErrorCode::Invalid,
            "accepted completion requires its pre-action SDK dispatch",
        ));
    }
    let binding = acyclic_stream::request::commit_binding_with(request, |bytes| {
        if let Some(io) = quota.work().as_deref_mut() {
            io.hashed_bytes(bytes)?;
        }
        Ok::<(), Error>(())
    })?
    .map_err(|error| operations::stream_error_with_work(error, quota.work()))?;
    let mut vectors = 0;
    let (mut bytes, mut count) = (0, 0);
    if let Some(io) = quota.work().as_deref_mut() {
        vectors = (request.conditions.capacity()
            * std::mem::size_of::<acyclic_stream::CommitCondition>()
            + request.mutations.capacity() * std::mem::size_of::<acyclic_stream::CommitMutation>())
            as u64;
        for mutation in &request.mutations {
            let (acyclic_stream::CommitMutation::Append { records, .. }
            | acyclic_stream::CommitMutation::Fork { records, .. }) = mutation;
            vectors +=
                (records.capacity() * std::mem::size_of::<acyclic_stream::AppendRecord>()) as u64;
            count += records.len() as u64;
            bytes += records
                .iter()
                .map(|record| record.payload_bytes() as u64)
                .sum::<u64>();
        }
        io.admit_acknowledgment(bytes, count)?;
        if !operations::original_native_decode_mode(io) {
            io.begin_write()?;
        }
    }
    Ok(Admission {
        binding,
        vectors,
        acknowledgment: (bytes, count),
    })
}

pub(crate) async fn commit<P: crate::store::JournalProvider + ?Sized>(
    (mut request, mut quota): (acyclic_stream::CommitRequest, CodecQuota<'_, '_>),
    client: &Arc<P>,
    operation: Option<OperationId>,
    mut phase: Option<&mut CommitPhase>,
    deadline_unix_millis: Option<u64>,
) -> Result<(acyclic_stream::CommitOutcome, u64)> {
    if let Some(phase) = phase.as_deref_mut() {
        *phase = CommitPhase::Uncommitted;
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if quota
        .work()
        .as_deref()
        .and_then(|io| io.original_native_work())
        .is_some()
    {
        let (pending, producer) = crate::distributed::NativeCommit::prepare(
            request,
            &mut quota,
            Arc::clone(client),
            deadline_unix_millis,
        )?;
        if let Some(phase) = phase.as_deref_mut() {
            *phase = CommitPhase::Unknown;
        }
        native_call_result(pending.original(), producer.await)?
            .map_err(|error| operations::stream_error_with_work(error, quota.work()))?;
        return pending.finish(operation, phase, &mut quota).await;
    }
    let Admission {
        binding,
        vectors,
        acknowledgment,
    } = admit(&mut request, &mut quota)?;
    let input = quota.retained() - vectors;
    request.deadline_unix_millis = request
        .deadline_unix_millis
        .into_iter()
        .chain(deadline_unix_millis)
        .min();
    let settlement = Settlement {
        binding,
        key: request.idempotency_key.clone(),
        acknowledgment,
        vectors,
        input,
    };
    if let Some(phase) = phase.as_deref_mut() {
        *phase = CommitPhase::Unknown;
    }
    let committed = client.commit(request).await;
    settlement
        .finish(committed, &mut quota, client, operation, phase)
        .await
}

struct Settlement {
    binding: acyclic_stream::request::CommitBinding,
    key: StreamIdempotencyKey,
    acknowledgment: (u64, u64),
    vectors: u64,
    input: u64,
}

impl Settlement {
    async fn finish<P: crate::store::JournalProvider + ?Sized>(
        self,
        committed: std::result::Result<acyclic_stream::CommitOutcome, StreamError>,
        quota: &mut CodecQuota<'_, '_>,
        client: &Arc<P>,
        operation: Option<OperationId>,
        mut phase: Option<&mut CommitPhase>,
    ) -> Result<(acyclic_stream::CommitOutcome, u64)> {
        let Self {
            binding,
            key,
            acknowledgment,
            vectors,
            input,
        } = self;
        if let Err(error) = &committed
            && *error != StreamError::Unavailable
            && let Some(phase) = phase.as_deref_mut()
        {
            *phase = CommitPhase::Uncommitted;
        }
        let released = quota.relinquish(vectors);
        let outcome = match (committed, operation) {
            (Ok(outcome), _) => {
                let observed =
                    observe_commit(&outcome, acknowledgment, phase.as_deref_mut(), quota);
                released.and(observed)?;
                outcome
            }
            (Err(StreamError::Unavailable), Some(operation)) => {
                released?;
                let before = quota.retained();
                match inspect_idempotency(client, key, &binding, operation, quota, phase).await? {
                    (IdempotencyOutcome::Commit(envelope), Some(revision)) => {
                        let facts_owned = quota.retained() - before;
                        let receipt = observation::receipt(&envelope, revision, quota)?;
                        drop(envelope);
                        quota.relinquish(facts_owned)?;
                        acyclic_stream::CommitOutcome::Committed(receipt)
                    }
                    // A publication whose authority issued no revision stays unknown.
                    (IdempotencyOutcome::Commit(_), None) => {
                        return Err(Error::Indeterminate(operation));
                    }
                    _ => {
                        return Err(guarded_boundary(
                            quota.work(),
                            wire::ErrorCode::Conflict,
                            "commit recovery identity reused",
                        ));
                    }
                }
            }
            (Err(error), _) => return Err(operations::stream_error_with_work(error, quota.work())),
        };
        if let Some(io) = quota.work().as_deref_mut() {
            io.check_cancellation()?;
        }
        quota.relinquish(input)?;
        let retained = quota.retained();
        quota.transfer();
        Ok((outcome, retained))
    }
}
