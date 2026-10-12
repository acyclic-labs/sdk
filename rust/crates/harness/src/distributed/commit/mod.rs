use super::*;

mod observation;
mod request;
mod sdk;
use observation::observe_commit;
pub(super) use request::reserve_prepared_vector;
pub(crate) use request::{add_condition, add_record, commit_request};
use sdk::inspect_idempotency;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use sdk::native_call_result;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) use sdk::receive_native_commit;
pub(super) use sdk::{PreparedNativeRecovery, resolve_commit};
pub(crate) use sdk::{SdkCallError, sdk_error, sdk_source};

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
            .cloned()
        {
            return Ok(native_call_result(
                &original,
                client
                    .native_append(
                        acyclic_stream::AppendRequest {
                            path: stream.path().clone(),
                            records,
                            if_tail,
                            idempotency_key: Some(key.clone()),
                        },
                        original.runtime_meter(),
                    )
                    .await,
            )?
            .map(|output| output.into_parts().0));
        }
        if let Some(io) = quota.work().as_deref_mut() {
            io.begin_write()?;
        }
        Ok::<_, Error>(
            stream
                .append_batch(records, if_tail, Some(key.clone()))
                .await
                .map_err(SdkCallError::Ok),
        )
    }
    .await?;
    let released = quota.relinquish(quota.retained());
    if !matches!(&appended, Err(Err(_))) {
        released?;
    }
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
        Err(error) if *sdk_source(&error) == StreamError::Unavailable => {
            match inspect_idempotency(client, key, &mut quota, None, None).await? {
                Ok(Some(outcome)) => match outcome {
                    IdempotencyOutcome::Append(outcome) => Ok(outcome),
                    _ => Err(coordinator_boundary(
                        quota.work(),
                        wire::ErrorCode::Conflict,
                        "append recovery identity has another operation kind",
                    )),
                },
                Ok(None) | Err(_) => Err(Error::Indeterminate(operation_id)),
            }
        }
        Err(Ok(StreamError::IdempotencyMismatch)) => Err(coordinator_boundary(
            quota.work(),
            wire::ErrorCode::Conflict,
            "append retry identity was reused",
        )),
        Err(error) => Err(sdk_error(error, quota.work())),
    }
}

pub(crate) async fn commit<P: StreamProvider>(
    (request, mut custody): (acyclic_stream::CommitRequest, CodecQuota<'_, '_>),
    client: &std::sync::Arc<P>,
    operation: Option<OperationId>,
    mut phase: Option<&mut CommitPhase>,
) -> Result<(acyclic_stream::CommitOutcome, u64)> {
    if let Some(phase) = phase.as_deref_mut() {
        *phase = CommitPhase::Uncommitted;
    }
    let key = request.idempotency_key.clone();
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if custody
        .work()
        .as_deref()
        .is_some_and(|io| io.requires_prepared_native_dispatch())
    {
        return Err(coordinator_boundary(
            custody.work(),
            wire::ErrorCode::Invalid,
            "accepted completion requires its pre-action SDK dispatch",
        ));
    }
    if custody.work().is_some() {
        let records = request
            .mutations
            .iter()
            .flat_map(|mutation| match mutation {
                acyclic_stream::CommitMutation::Append { records, .. }
                | acyclic_stream::CommitMutation::Fork { records, .. } => records,
            });
        let (bytes, count) = records.fold((0, 0), |(bytes, count), record| {
            (bytes + record.len() as u64, count + 1)
        });
        if let Some(work) = custody.work().as_deref_mut() {
            work.admit_acknowledgment(bytes, count)?;
            if !operations::original_native_decode_mode(work) {
                work.begin_write()?;
            }
        }
    }
    let vectors = (request.conditions.capacity()
        * std::mem::size_of::<acyclic_stream::CommitCondition>()
        + request.mutations.capacity() * std::mem::size_of::<acyclic_stream::CommitMutation>()
        + request
            .mutations
            .iter()
            .map(|mutation| {
                let (acyclic_stream::CommitMutation::Append { records, .. }
                | acyclic_stream::CommitMutation::Fork { records, .. }) = mutation;
                records.capacity() * std::mem::size_of::<OwnedBytes>()
            })
            .sum::<usize>()) as u64;
    // Request builders retain each vector's actual capacity before dispatch.
    let remaining_request = custody.retained() - if custody.work().is_some() { vectors } else { 0 };
    if let Some(phase) = phase.as_deref_mut() {
        *phase = CommitPhase::Unknown;
    }
    let committed = async {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(original) = custody
            .work()
            .as_deref()
            .and_then(|io| io.original_native_work())
            .cloned()
        {
            let result = client
                .native_commit(request, original.runtime_meter())
                .await;
            return receive_native_commit(
                &original,
                result,
                phase.as_deref_mut(),
                vectors,
                &mut custody,
            );
        }
        let result = client.commit(request).await;
        if let Err(error) = &result
            && *error != StreamError::Unavailable
            && let Some(phase) = phase.as_deref_mut()
        {
            *phase = CommitPhase::Uncommitted;
        }
        if custody.work().is_some() {
            custody.relinquish(vectors)?;
        }
        match result {
            Err(source) => Ok(Err(SdkCallError::Ok(source))),
            Ok(outcome) => {
                observe_commit(&outcome, true, phase.as_deref_mut(), &mut custody)?;
                Ok(Ok(outcome))
            }
        }
    }
    .await?;
    let outcome =
        resolve_commit(committed, client, key, operation, phase, None, &mut custody).await?;
    custody.relinquish(remaining_request)?;
    let retained = custody.retained();
    custody.transfer();
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
            return Err(coordinator_boundary(
                quota.work(),
                wire::ErrorCode::Conflict,
                "execution changed before suspension",
            ));
        }
        Err(error) if *sdk_source(&error) == StreamError::NotFound && expected_tail == 0 => {
            CommitCondition::Absent { path }
        }
        Err(error) => return Err(sdk_error(error, quota.work())),
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
        return Err(coordinator_boundary(
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
    Err(coordinator_boundary(
        quota.work(),
        wire::ErrorCode::Storage,
        "invalid guarded suspension receipt",
    ))
}
