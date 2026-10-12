use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitPhase {
    Uncommitted,
    Unknown,
    Committed,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) type PreparedNativeRecovery =
    acyclic_stream::NativeProviderFuture<'static, Option<acyclic_stream::IdempotencyObservation>>;
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
pub(super) type PreparedNativeRecovery = std::convert::Infallible;

pub(crate) type SdkCallError = std::result::Result<StreamError, Error>;

pub(crate) fn sdk_source(error: &SdkCallError) -> &StreamError {
    match error {
        Ok(source) => source,
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        Err(Error::NativeStream { source, .. }) => source,
        Err(_) => unreachable!("non-SDK failures use the outer result"),
    }
}

pub(crate) fn sdk_error(error: SdkCallError, work: &Option<&mut dyn HistoryIo>) -> Error {
    match error {
        Ok(source) => operations::stream_error_with_work(source, work),
        Err(error) => error,
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn native_call_result<T>(
    original: &acyclic_fs::NativeMaterializationWork,
    result: acyclic_stream::NativeStreamResult<T>,
) -> Result<std::result::Result<acyclic_stream::NativeStreamOutput<T>, SdkCallError>> {
    match crate::store::history::native_sdk_result(original, result) {
        Ok(output) => Ok(Ok(output)),
        Err(error @ Error::NativeStream { .. }) => Ok(Err(Err(error))),
        Err(error) => Err(error),
    }
}

pub(super) fn coordinator_retry_conflict(error: &Error) -> bool {
    if matches!(error, Error::Conflict(_)) {
        return true;
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    return matches!(
        error,
        Error::NativeStream {
            source: StreamError::IdempotencyMismatch,
            ..
        } | Error::NativeBoundary {
            code: wire::ErrorCode::Conflict,
            ..
        }
    );
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    false
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
    let bytes = records.iter().try_fold(0u64, |total, record| {
        total.checked_add(record.len() as u64).ok_or_else(|| {
            coordinator_boundary(
                quota.work(),
                wire::ErrorCode::Invalid,
                "append request byte count exhausted",
            )
        })
    })?;
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
    let native_failure = appended.is_err()
        && quota
            .work()
            .as_deref()
            .is_some_and(operations::original_native_decode_mode);
    let released = quota.relinquish(quota.retained());
    if !native_failure {
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
            match inspect_idempotency(client, key, quota.work(), None, None).await? {
                Ok(Some((outcome, retained))) => {
                    quota.adopt(retained)?;
                    match outcome {
                        IdempotencyOutcome::Append(outcome) => Ok(outcome),
                        _ => Err(coordinator_boundary(
                            quota.work(),
                            wire::ErrorCode::Conflict,
                            "append recovery identity has another operation kind",
                        )),
                    }
                }
                Ok(None) | Err(_) => Err(Error::Indeterminate(operation_id)),
            }
        }
        Err(error)
            if *sdk_source(&error) == StreamError::IdempotencyMismatch
                && !quota
                    .work()
                    .as_deref()
                    .is_some_and(operations::original_native_decode_mode) =>
        {
            Err(coordinator_boundary(
                quota.work(),
                wire::ErrorCode::Conflict,
                "append retry identity was reused",
            ))
        }
        Err(error) => Err(sdk_error(error, quota.work())),
    }
}

async fn inspect_idempotency<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    key: StreamIdempotencyKey,
    work: &mut Option<&mut dyn HistoryIo>,
    phase: Option<&mut CommitPhase>,
    recovery: Option<PreparedNativeRecovery>,
) -> Result<std::result::Result<Option<(IdempotencyOutcome, u64)>, SdkCallError>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if recovery.is_none()
        && work
            .as_deref()
            .is_some_and(|observer| observer.requires_prepared_native_dispatch())
    {
        return Err(coordinator_boundary(
            work,
            wire::ErrorCode::Invalid,
            "accepted completion requires its pre-action SDK recovery",
        ));
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = recovery;
    let mut custody = CodecQuota::from_retained(work, 0);
    let inspected = async {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(original) = custody
            .work()
            .as_deref()
            .and_then(|observer| observer.original_native_work())
            .cloned()
        {
            let inspected = native_call_result(
                &original,
                match recovery {
                    Some(recovery) => {
                        drop(key);
                        recovery.await
                    }
                    None => {
                        client
                            .native_inspect_idempotency(key, original.runtime_meter())
                            .await
                    }
                },
            )?;
            let output = match inspected {
                Ok(output) => output,
                Err(error) => return Ok(Err(error)),
            };
            if let Some(observation) = output.value() {
                observe_recovery(observation, phase, &mut custody)?;
            }
            let (observation, retained) =
                crate::store::history::adopt_native_sdk_output(&original, output, custody.work())?;
            custody.adopt(retained)?;
            return Ok(Ok(observation));
        }
        if let Some(work) = custody.work().as_deref_mut() {
            work.begin_call()?;
        }
        match client.inspect_idempotency(key).await {
            Err(source) => Ok::<_, Error>(Err(SdkCallError::Ok(source))),
            Ok(observation) => {
                if let Some(observation) = &observation {
                    observe_recovery(observation, phase, &mut custody)?;
                }
                Ok(Ok(observation))
            }
        }
    }
    .await?;
    let observation = match inspected {
        Ok(Some(observation)) => observation,
        Ok(None) => return Ok(Ok(None)),
        Err(error) => return Ok(Err(error)),
    };
    if let Some(work) = custody.work().as_deref_mut() {
        work.check_cancellation()?;
    }
    let metadata = if custody.work().is_some()
        && !custody
            .work()
            .as_deref()
            .is_some_and(operations::original_native_decode_mode)
    {
        observation.idempotency_key.as_bytes().len() as u64
    } else {
        0
    };
    let acyclic_stream::IdempotencyObservation {
        idempotency_key,
        outcome,
        ..
    } = observation;
    drop(idempotency_key);
    custody.relinquish(metadata)?;
    let retained = custody.retained();
    custody.transfer();
    Ok(Ok(Some((outcome, retained))))
}

pub(super) fn reserve_prepared_vector<T>(
    values: &mut Vec<T>,
    capacity: usize,
    custody: &mut CodecQuota<'_, '_>,
) -> Result<()> {
    if capacity <= values.capacity() {
        return Ok(());
    }
    let old = values.capacity();
    if let Some(work) = custody.work().as_deref_mut() {
        crate::store::history::reserve(values, capacity, work)?;
        custody.adopt(((values.capacity() - old) * std::mem::size_of::<T>()) as u64)?;
    } else {
        values.try_reserve_exact(capacity - values.len())?;
    }
    Ok(())
}

pub(crate) fn commit_request<'a, 'b>(
    key: StreamIdempotencyKey,
    retained_key_bytes: u64,
    work: &'a mut Option<&'b mut dyn HistoryIo>,
) -> (acyclic_stream::CommitRequest, CodecQuota<'a, 'b>) {
    (
        acyclic_stream::CommitRequest {
            conditions: Vec::new(),
            mutations: Vec::new(),
            idempotency_key: key,
        },
        CodecQuota::from_retained(work, retained_key_bytes),
    )
}

pub(crate) fn add_condition(
    (request, custody): &mut (acyclic_stream::CommitRequest, CodecQuota<'_, '_>),
    condition: acyclic_stream::CommitCondition,
) -> Result<()> {
    let length = request.conditions.len() + 1;
    reserve_prepared_vector(&mut request.conditions, length, custody)?;
    request.conditions.push(condition);
    Ok(())
}

pub(crate) fn add_record(
    (request, custody): &mut (acyclic_stream::CommitRequest, CodecQuota<'_, '_>),
    path: acyclic_stream::StreamPath,
    record: OwnedBytes,
) -> Result<()> {
    let length = request.mutations.len() + 1;
    reserve_prepared_vector(&mut request.mutations, length, custody)?;
    let mut records = Vec::new();
    reserve_prepared_vector(&mut records, 1, custody)?;
    records.push(record);
    request
        .mutations
        .push(acyclic_stream::CommitMutation::Append { path, records });
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn receive_native_commit(
    original: &acyclic_fs::NativeMaterializationWork,
    committed: acyclic_stream::NativeStreamResult<acyclic_stream::CommitOutcome>,
    phase: Option<&mut CommitPhase>,
    vectors: u64,
    custody: &mut CodecQuota<'_, '_>,
) -> Result<std::result::Result<(acyclic_stream::CommitOutcome, u64), SdkCallError>> {
    if let Some(phase) = phase {
        *phase = match &committed {
            Ok(Ok(output)) => match output.value() {
                acyclic_stream::CommitOutcome::Committed(_) => CommitPhase::Committed,
                acyclic_stream::CommitOutcome::Conflict(_) => CommitPhase::Uncommitted,
            },
            Ok(Err(error)) if *error == StreamError::Unavailable => CommitPhase::Unknown,
            Ok(Err(_)) => CommitPhase::Uncommitted,
            Err(_) => CommitPhase::Unknown,
        };
    }
    let released = custody.relinquish(vectors);
    let output = match native_call_result(original, committed)? {
        Ok(output) => output,
        Err(error) => return Ok(Err(error)),
    };
    let observed = observe_commit(output.value(), true, custody);
    released.and(observed)?;
    let mut received = CodecQuota::from_retained(custody.work(), 0);
    let (outcome, retained) =
        crate::store::history::adopt_native_sdk_output(original, output, received.work())?;
    received.adopt(retained)?;
    received
        .work()
        .as_deref_mut()
        .expect("original native observer")
        .check_cancellation()?;
    let retained = received.retained();
    received.transfer();
    Ok(Ok((outcome, retained)))
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
        let (bytes, count) = request
            .mutations
            .iter()
            .flat_map(|mutation| match mutation {
                acyclic_stream::CommitMutation::Append { records, .. }
                | acyclic_stream::CommitMutation::Fork { records, .. } => records,
            })
            .try_fold((0u64, 0u64), |(bytes, count), record| {
                let bytes = bytes.checked_add(record.len() as u64).ok_or_else(|| {
                    coordinator_boundary(
                        &None,
                        wire::ErrorCode::Invalid,
                        "commit payload byte count overflows",
                    )
                })?;
                let count = count.checked_add(1).ok_or_else(|| {
                    coordinator_boundary(
                        &None,
                        wire::ErrorCode::Invalid,
                        "commit record count overflows",
                    )
                })?;
                Ok::<_, Error>((bytes, count))
            })?;
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
    if let Some(phase) = phase.as_deref_mut() {
        *phase = CommitPhase::Unknown;
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(original) = custody
        .work()
        .as_deref()
        .and_then(|observer| observer.original_native_work())
        .cloned()
    {
        let committed = client
            .native_commit(request, original.runtime_meter())
            .await;
        let committed = receive_native_commit(
            &original,
            committed,
            phase.as_deref_mut(),
            vectors,
            &mut custody,
        )?;
        return resolve_commit(
            committed,
            client,
            key,
            operation,
            phase,
            None,
            custody.work(),
        )
        .await;
    }
    let committed = client.commit(request).await;
    if let Some(phase) = phase.as_deref_mut() {
        *phase = match &committed {
            Ok(acyclic_stream::CommitOutcome::Committed(_)) => CommitPhase::Committed,
            Ok(acyclic_stream::CommitOutcome::Conflict(_)) => CommitPhase::Uncommitted,
            Err(StreamError::Unavailable) => CommitPhase::Unknown,
            Err(_) => CommitPhase::Uncommitted,
        };
    }
    if custody.work().is_some() {
        custody.relinquish(vectors)?;
    }
    let mut received = CodecQuota::from_retained(custody.work(), 0);
    let committed = match committed {
        Err(source) => Err(SdkCallError::Ok(source)),
        Ok(outcome) => {
            observe_commit(&outcome, true, &mut received)?;
            if let Some(work) = received.work().as_deref_mut() {
                work.check_cancellation()?;
            }
            let retained = received.retained();
            received.transfer();
            Ok((outcome, retained))
        }
    };
    drop(received);
    resolve_commit(
        committed,
        client,
        key,
        operation,
        phase,
        None,
        custody.work(),
    )
    .await
}

pub(super) async fn resolve_commit<P: StreamProvider>(
    committed: std::result::Result<(acyclic_stream::CommitOutcome, u64), SdkCallError>,
    client: &Arc<P>,
    key: StreamIdempotencyKey,
    operation: Option<OperationId>,
    phase: Option<&mut CommitPhase>,
    recovery: Option<PreparedNativeRecovery>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(acyclic_stream::CommitOutcome, u64)> {
    let mut quota = CodecQuota::from_retained(work, 0);
    let (outcome, retained) = match committed {
        Ok((outcome, retained)) => {
            quota.adopt(retained)?;
            (outcome, retained)
        }
        Err(error) => {
            let Some(operation) =
                operation.filter(|_| *sdk_source(&error) == StreamError::Unavailable)
            else {
                return Err(sdk_error(error, quota.work()));
            };
            match inspect_idempotency(client, key, quota.work(), phase, recovery).await? {
                Ok(Some((IdempotencyOutcome::Commit(outcome), retained))) => {
                    quota.adopt(retained)?;
                    (outcome, retained)
                }
                Ok(Some((outcome, retained))) => {
                    quota.adopt(retained)?;
                    drop(outcome);
                    return Err(coordinator_boundary(
                        quota.work(),
                        wire::ErrorCode::Conflict,
                        "commit recovery identity reused",
                    ));
                }
                Ok(None) | Err(_) => return Err(Error::Indeterminate(operation)),
            }
        }
    };
    if let Some(io) = quota.work().as_deref_mut() {
        io.check_cancellation()?;
    }
    quota.transfer();
    Ok((outcome, retained))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_if_execution_idle<P: StreamProvider>(
    client: &std::sync::Arc<P>,
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
    let mut custody = CodecQuota::new(work, 0)?;
    let path = execution_path(execution, custody.work())?;
    if custody.work().is_some() {
        custody.adopt(path.as_str().len() as u64)?;
    }
    let bounds = {
        let mut source_custody = CodecQuota::new(custody.work(), 0)?;
        if let Some(io) = source_custody.work().as_deref_mut()
            && !operations::original_native_decode_mode(io)
        {
            let bytes = path.as_str().len() as u64;
            io.admit_allocation(bytes)?;
            io.copy_bytes(bytes)?;
            io.retain_owned(bytes)?;
            source_custody.adopt(bytes)?;
        }
        operations::bounds_with_work(client, &path, source_custody.work()).await?
    };
    let guard = match bounds {
        Ok(bounds) if bounds.tail == expected_tail => CommitCondition::Tail {
            path,
            expected: expected_tail,
        },
        Ok(_) => {
            return Err(coordinator_boundary(
                custody.work(),
                wire::ErrorCode::Conflict,
                "execution changed before suspension",
            ));
        }
        Err(error) if *sdk_source(&error) == StreamError::NotFound && expected_tail == 0 => {
            CommitCondition::Absent { path }
        }
        Err(error) => return Err(sdk_error(error, custody.work())),
    };
    if let Some(work) = custody.work().as_deref_mut() {
        work.hashed_bytes(
            (b"harness/v2/idle-execution\0".len() + key.as_bytes().len() + 1) as u64,
        )?;
    }
    let mut digest = blake3::Hasher::new();
    digest.update(b"harness/v2/idle-execution\0");
    digest.update(key.as_bytes());
    digest.update(&[u8::from(matches!(guard, CommitCondition::Absent { .. }))]);
    let workflow_condition = if let Some((workflow_path, workflow_tail)) = workflow_guard {
        let path = stream_path(workflow_path, custody.work())?;
        if custody.work().is_some() {
            custody.adopt(path.as_str().len() as u64)?;
        }
        if let Some(work) = custody.work().as_deref_mut() {
            work.hashed_bytes((b"workflow\0".len() + workflow_path.len() + 8) as u64)?;
        }
        digest.update(b"workflow\0");
        digest.update(workflow_path.as_bytes());
        digest.update(&workflow_tail.to_le_bytes());
        Some(CommitCondition::Tail {
            path,
            expected: workflow_tail,
        })
    } else {
        None
    };
    let (key, held) = digest_key(digest.finalize().as_bytes(), custody.work())?;
    let mut request = commit_request(key, held, custody.work());
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
    let (outcome, outcome_bytes) = commit(request, client, Some(operation), None).await?;
    custody.adopt(outcome_bytes)?;
    let CommitOutcome::Committed(envelope) = &outcome else {
        return Err(coordinator_boundary(
            custody.work(),
            wire::ErrorCode::Conflict,
            "execution or coordinator changed before suspension",
        ));
    };
    let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
        return Err(coordinator_boundary(
            custody.work(),
            wire::ErrorCode::Storage,
            "invalid guarded suspension receipt",
        ));
    };
    if append.path != *stream.path()
        || append.start != tail
        || append.end != next_revision(tail)?
        || append.tail != append.end
        || append.records.len() != 1
        || append.records.first().is_none_or(|entry| {
            entry.sequence != tail || entry.value != record || entry.commit_id != envelope.commit_id
        })
    {
        return Err(coordinator_boundary(
            custody.work(),
            wire::ErrorCode::Storage,
            "guarded suspension differs from its commit",
        ));
    }
    Ok(AppendOutcome::Committed(acyclic_stream::AppendReceipt {
        start: append.start,
        end: append.end,
        tail: append.tail,
        commit_id: envelope.commit_id,
    }))
}

mod observation;
use observation::{observe_commit, observe_recovery};
