use super::*;
use acyclic_stream::StreamProviderExt as _;

mod observation;
mod request;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod retained;
mod sdk;
pub(super) use observation::observe_commit;
pub(crate) use request::{add_condition, commit_request};
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

#[allow(
    clippy::too_many_lines,
    reason = "Acknowledgment and same-key recovery settle one originally admitted commit quota."
)]
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
    let Admission {
        binding,
        vectors,
        acknowledgment,
    } = admit(&mut request, &mut quota)?;
    let input = quota.retained() - vectors;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let (mut vectors, mut input) = (vectors, input);
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let mut original_request = None;
    let key = request.idempotency_key.clone();
    if let Some(phase) = phase.as_deref_mut() {
        *phase = CommitPhase::Unknown;
    }
    let committed = async {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(original) = quota
            .work()
            .as_deref()
            .and_then(|io| io.original_native_work())
            .cloned()
        {
            // Move the existing construction custody before dispatch. Its exact
            // request survives through result adoption and same-key recovery.
            let (pending, producer) = retained::prepare(
                request,
                &mut quota,
                Arc::clone(client),
                &original,
                deadline_unix_millis,
            )?;
            vectors = 0;
            input = 0;
            native_call_result(&original, producer.await)?
                .map_err(|error| operations::stream_error_with_work(error, quota.work()))?;
            let (result, retained_request) = retained::take(pending, &original)?;
            original_request = Some(retained_request);
            return match native_call_result(&original, result)? {
                Ok(output) => {
                    let (outcome, retained) = crate::store::history::adopt_native_sdk_output(
                        &original,
                        output,
                        quota.work(),
                    )?;
                    quota.adopt(retained)?;
                    Ok(Ok(outcome))
                }
                Err(error) => Ok(Err(error)),
            };
        }
        let mut request = request;
        request.deadline_unix_millis = request
            .deadline_unix_millis
            .into_iter()
            .chain(deadline_unix_millis)
            .min();
        Ok::<_, Error>(client.commit(request).await)
    }
    .await?;
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
                observe_commit(&outcome, acknowledgment, phase.as_deref_mut(), &mut quota);
            released.and(observed)?;
            outcome
        }
        (Err(StreamError::Unavailable), Some(operation)) => {
            released?;
            let before = quota.retained();
            match inspect_idempotency(client, key, &binding, operation, &mut quota, phase).await? {
                (IdempotencyOutcome::Commit(envelope), Some(revision)) => {
                    let facts_owned = quota.retained() - before;
                    let receipt = observation::receipt(&envelope, revision, &mut quota)?;
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
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    drop(original_request);
    let retained = quota.retained();
    quota.transfer();
    Ok((outcome, retained))
}
