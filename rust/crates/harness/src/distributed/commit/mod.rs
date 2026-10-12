use super::*;

mod observation;
mod request;
mod sdk;
pub(super) use observation::observe_commit;
pub(crate) use request::{add_condition, add_record, commit_request};
use sdk::inspect_idempotency;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use sdk::native_call_result;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitPhase {
    Uncommitted,
    Unknown,
    Committed,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn append<P: StreamProvider, const N: usize>(
    (stream, client): (&Stream<P>, &Arc<P>),
    input: [OwnedBytes; N],
    retained_request: u64,
    if_tail: Option<u64>,
    key: StreamIdempotencyKey,
    operation_id: OperationId,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<AppendOutcome> {
    let mut quota = CodecQuota::from_retained(work, retained_request);
    let mut records = crate::core::reserve_copy_vector(N, &mut quota)?;
    records.extend(input);
    let bytes = records.iter().map(|record| record.len() as u64).sum();
    if let Some(io) = quota.work().as_deref_mut() {
        io.admit_acknowledgment(bytes, N as u64)?;
    }
    let appended = async {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(original) = quota
            .work()
            .as_deref()
            .and_then(|io| io.original_native_work())
        {
            let result = client
                .native_append(
                    acyclic_stream::AppendRequest {
                        path: stream.path().clone(),
                        records,
                        if_tail,
                        idempotency_key: Some(key.clone()),
                    },
                    original.runtime_meter(),
                )
                .await;
            return native_call_result(original, result)
                .map(|result| result.map(|output| output.into_parts().0));
        }
        if let Some(io) = quota.work().as_deref_mut() {
            io.begin_write()?;
        }
        Ok(stream
            .append_batch(records, if_tail, Some(key.clone()))
            .await)
    }
    .await?;
    quota.relinquish(quota.retained())?;
    match appended {
        Ok(outcome) => {
            if let Some(io) = quota.work().as_deref_mut() {
                if let AppendOutcome::Committed(receipt) = &outcome
                    && receipt.end.checked_sub(receipt.start) == Some(N as u64)
                    && receipt.tail >= receipt.end
                {
                    io.acknowledged(bytes, N as u64)?;
                }
                io.check_cancellation()?;
            }
            Ok(outcome)
        }
        Err(StreamError::Unavailable) => {
            match inspect_idempotency(client, key, operation_id, &mut quota, None).await? {
                IdempotencyOutcome::Append(outcome) => Ok(outcome),
                _ => Err(guarded_boundary(
                    quota.work(),
                    wire::ErrorCode::Conflict,
                    "append recovery identity has another operation kind",
                )),
            }
        }
        Err(StreamError::IdempotencyMismatch)
            if !quota
                .work()
                .as_deref()
                .is_some_and(operations::original_native_decode_mode) =>
        {
            Err(Error::Conflict("append retry identity was reused".into()))
        }
        Err(error) => Err(operations::stream_error_with_work(error, quota.work())),
    }
}

pub(crate) async fn commit<P: StreamProvider>(
    (request, mut quota): (acyclic_stream::CommitRequest, CodecQuota<'_, '_>),
    client: &Arc<P>,
    operation: Option<OperationId>,
    mut phase: Option<&mut CommitPhase>,
) -> Result<(acyclic_stream::CommitOutcome, u64)> {
    if let Some(phase) = phase.as_deref_mut() {
        *phase = CommitPhase::Uncommitted;
    }
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
    let mut vectors = 0;
    if let Some(io) = quota.work().as_deref_mut() {
        vectors = (request.conditions.capacity()
            * std::mem::size_of::<acyclic_stream::CommitCondition>()
            + request.mutations.capacity() * std::mem::size_of::<acyclic_stream::CommitMutation>())
            as u64;
        let (mut bytes, mut count) = (0, 0);
        for mutation in &request.mutations {
            let (acyclic_stream::CommitMutation::Append { records, .. }
            | acyclic_stream::CommitMutation::Fork { records, .. }) = mutation;
            vectors += (records.capacity() * std::mem::size_of::<OwnedBytes>()) as u64;
            count += records.len() as u64;
            bytes += records
                .iter()
                .map(|record| record.len() as u64)
                .sum::<u64>();
        }
        io.admit_acknowledgment(bytes, count)?;
        if !operations::original_native_decode_mode(io) {
            io.begin_write()?;
        }
    }
    let input = quota.retained() - vectors;
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
            let result = client
                .native_commit(request, original.runtime_meter())
                .await;
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
            let observed = observe_commit(&outcome, true, phase.as_deref_mut(), &mut quota);
            released.and(observed)?;
            outcome
        }
        (Err(StreamError::Unavailable), Some(operation)) => {
            released?;
            match inspect_idempotency(client, key, operation, &mut quota, phase).await? {
                IdempotencyOutcome::Commit(outcome) => outcome,
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

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_if_execution_idle<P: StreamProvider>(
    client: &Arc<P>,
    stream: &Stream<P>,
    record: OwnedBytes,
    tail: u64,
    key: StreamIdempotencyKey,
    operation: OperationId,
    execution: OperationId,
    expected_tail: u64,
    workflow_guard: Option<(&str, u64)>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<AppendOutcome> {
    use acyclic_stream::{CommitCondition, CommitOutcome, CommittedMutation};
    let mut quota = CodecQuota::new(work, 0)?;
    let path = execution_path(execution, quota.work())?;
    if quota.work().is_some() {
        quota.adopt(path.as_str().len() as u64)?;
    }
    let bounds = operations::bounds_with_work(client, &path, quota.work()).await?;
    let guard = match bounds {
        Ok(bounds) if bounds.tail == expected_tail => CommitCondition::Tail {
            path,
            expected: expected_tail,
        },
        Ok(_) => {
            return Err(guarded_boundary(
                quota.work(),
                wire::ErrorCode::Conflict,
                "execution changed before suspension",
            ));
        }
        Err(error) if error == StreamError::NotFound && expected_tail == 0 => {
            CommitCondition::Absent { path }
        }
        Err(error) => {
            return Err(crate::store::operations::stream_error_with_work(
                error,
                quota.work(),
            ));
        }
    };
    if let Some(io) = quota.work().as_deref_mut() {
        io.hashed_bytes((b"harness/v2/idle-execution\0".len() + key.as_bytes().len() + 1) as u64)?;
    }
    let mut digest = blake3::Hasher::new();
    digest.update(b"harness/v2/idle-execution\0");
    digest.update(key.as_bytes());
    digest.update(&[u8::from(matches!(guard, CommitCondition::Absent { .. }))]);
    let workflow_condition = if let Some((name, tail)) = workflow_guard {
        let path = stream_path(name, quota.work())?;
        if quota.work().is_some() {
            quota.adopt(path.as_str().len() as u64)?;
        }
        if let Some(io) = quota.work().as_deref_mut() {
            io.hashed_bytes((b"workflow\0".len() + name.len() + 8) as u64)?;
        }
        digest.update(b"workflow\0");
        digest.update(name.as_bytes());
        digest.update(&tail.to_le_bytes());
        Some(CommitCondition::Tail {
            path,
            expected: tail,
        })
    } else {
        None
    };
    let (key, held) = digest_key(digest.finalize().as_bytes(), quota.work())?;
    let mut request = commit_request(key, held, quota.work());
    add_condition(
        &mut request,
        CommitCondition::Tail {
            path: stream.path().clone(),
            expected: tail,
        },
    )?;
    add_condition(&mut request, guard)?;
    if let Some(condition) = workflow_condition {
        add_condition(&mut request, condition)?;
    }
    add_record(&mut request, stream.path().clone(), record.clone())?;
    let (outcome, retained) = commit(request, client, Some(operation), None).await?;
    quota.adopt(retained)?;
    let CommitOutcome::Committed(envelope) = &outcome else {
        return Err(guarded_boundary(
            quota.work(),
            wire::ErrorCode::Conflict,
            "execution or coordinator changed before suspension",
        ));
    };
    if let [CommittedMutation::Append(append)] = envelope.mutations.as_slice()
        && append.path == *stream.path()
        && append.start == tail
        && append.end == next_revision(tail)?
        && append.tail == append.end
        && append.records.len() == 1
        && append.records.first().is_some_and(|entry| {
            entry.sequence == tail && entry.value == record && entry.commit_id == envelope.commit_id
        })
    {
        return Ok(AppendOutcome::Committed(acyclic_stream::AppendReceipt {
            start: append.start,
            end: append.end,
            tail: append.tail,
            commit_id: envelope.commit_id,
        }));
    }
    Err(guarded_boundary(
        quota.work(),
        wire::ErrorCode::Storage,
        "invalid guarded suspension receipt",
    ))
}
