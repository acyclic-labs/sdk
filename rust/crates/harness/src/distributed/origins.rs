use super::*;
use acyclic_stream::{CommitCondition, CommitOutcome, CommittedMutation, StreamPath};

pub(super) fn path(
    operation: OperationId,
    phase: &str,
    fence: Option<(&str, &str)>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<StreamPath> {
    use std::io::Write as _;
    let mut buffer = [0; 192];
    let mut writer = std::io::Cursor::new(buffer.as_mut_slice());
    write!(writer, "harness/v3/coordinator/origins/{operation}/{phase}")
        .map_err(|_| guarded_boundary(work, wire::ErrorCode::Invalid, "origin path overflow"))?;
    if let Some(fence) = fence {
        let digest = crate::executor::json::canonical_json_digest(
            &fence,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            work,
        )?;
        write!(writer, "/{}", blake3::Hash::from_bytes(digest).to_hex()).map_err(|_| {
            guarded_boundary(work, wire::ErrorCode::Invalid, "origin path overflow")
        })?;
    }
    let length = writer.position() as usize;
    let text = std::str::from_utf8(&buffer[..length]).map_err(|_| {
        guarded_boundary(work, wire::ErrorCode::Invalid, "origin path is not UTF-8")
    })?;
    stream_path(text, work)
}

pub(super) fn event_path(
    event: &SchedulerEvent,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Option<StreamPath>> {
    let (phase, fence) = match event {
        SchedulerEvent::Declared { .. } => ("declared", None),
        SchedulerEvent::Admitted { reservation, .. } => (
            "admitted",
            Some((reservation.id.as_str(), reservation.placement.as_str())),
        ),
        SchedulerEvent::Started { fence, .. } => (
            "started",
            Some((fence.reservation_id.as_str(), fence.placement.as_str())),
        ),
        _ => return Ok(None),
    };
    path(event.operation_id(), phase, fence, work).map(Some)
}

// The locator and its canonical source are one atomic StreamProvider commit.
// A locator is never authority without the matching immutable commit identity.
pub(super) async fn append_origin<const FENCE: bool, P: StreamProvider, const N: usize>(
    (stream, client): (&Stream<P>, &Arc<P>),
    input: [OwnedBytes; N],
    retained_request: u64,
    tail: u64,
    key: StreamIdempotencyKey,
    operation: OperationId,
    path: StreamPath,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<AppendOutcome> {
    let mut quota = CodecQuota::from_retained(work, retained_request);
    let (sequence, retained) = {
        let mut sequence_quota = CodecQuota::new(quota.work(), 0)?;
        let key: &[u8; 32] = key.as_bytes().try_into().map_err(|_| {
            guarded_boundary(
                sequence_quota.work(),
                wire::ErrorCode::Invalid,
                "origin commit key must be a digest",
            )
        })?;
        let bytes =
            crate::core::copy_copy_vector(&origin_locator::encode(tail, key), &mut sequence_quota)?;
        sequence_quota.transfer();
        operations::owned_bytes_from_retained_vec(bytes, sequence_quota.work())?
    };
    quota.adopt(retained)?;
    let expected = if FENCE || !cfg!(all(test, acyclic_harness_loom, acyclic_loom)) {
        (tail != 0).then_some(tail)
    } else {
        // Negative control: re-fence a stale intent using today's source tail.
        // Every mutation still needs a condition under the SDK contract.
        operations::tail_optional_with_work(client, stream.path(), quota.work()).await?
    };
    let mut request = commit_request(key, 0, quota.work());
    request.1.reserve(2, &mut request.0.conditions)?;
    request.1.reserve(2, &mut request.0.mutations)?;
    add_condition(
        &mut request,
        match expected {
            Some(expected) => CommitCondition::Tail {
                path: stream.path().clone(),
                expected,
            },
            None => CommitCondition::Absent {
                path: stream.path().clone(),
            },
        },
    )?;
    add_condition(&mut request, CommitCondition::Absent { path: path.clone() })?;
    let mut records = Vec::new();
    request.1.reserve(N, &mut records)?;
    records.extend(input.iter().cloned());
    request
        .0
        .mutations
        .push(acyclic_stream::CommitMutation::Append {
            path: stream.path().clone(),
            records,
        });
    add_record(&mut request, path.clone(), sequence.clone())?;
    let (outcome, retained) = commit(request, client, Some(operation), None).await?;
    quota.adopt(retained)?;
    let CommitOutcome::Committed(envelope) = &outcome else {
        return Ok(AppendOutcome::TailConflict { actual_tail: tail });
    };
    let mut canonical = None;
    let mut indexed = false;
    for mutation in &envelope.mutations {
        let CommittedMutation::Append(append) = mutation else {
            continue;
        };
        if append.tail != append.end
            || append.records.iter().enumerate().any(|(offset, record)| {
                append.start.checked_add(offset as u64) != Some(record.sequence)
                    || record.commit_id != envelope.commit_id
            })
        {
            continue;
        }
        if append.path == *stream.path()
            && append.start == tail
            && append.end.checked_sub(append.start) == Some(N as u64)
            && append.records.len() == N
            && append
                .records
                .iter()
                .zip(&input)
                .all(|(record, original)| record.value == *original)
        {
            canonical = Some(append);
        }
        if append.path == path
            && append.start == 0
            && append.end == 1
            && append.records.len() == 1
            && append.records[0].value == sequence
        {
            indexed = true;
        }
    }
    if let Some(append) = canonical
        && indexed
        && envelope.mutations.len() == 2
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
        "invalid origin publication receipt",
    ))
}

#[cfg(any(test, feature = "filesystem"))]
pub(super) async fn read<P: StreamProvider>(
    client: &Arc<P>,
    operation: OperationId,
    path: StreamPath,
    expected_key: Option<&str>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(u64, u64, SchedulerEvent)> {
    let mut quota = CodecQuota::new(work, 0)?;
    quota.adopt(path.as_str().len() as u64)?;
    let mut records = match operations::read_records(client, &path, 0, 2, quota.work()).await? {
        Ok(records) => records,
        Err(StreamError::NotFound) => {
            return Err(guarded_boundary(
                quota.work(),
                wire::ErrorCode::NotFound,
                "original reservation source",
            ));
        }
        Err(error) => return Err(operations::stream_error_with_work(error, quota.work())),
    };
    let index = records.next(quota.work()).await?.ok_or_else(|| {
        guarded_boundary(
            quota.work(),
            wire::ErrorCode::NotFound,
            "original reservation source",
        )
    })?;
    let held = operations::sdk_record_retained_bytes(&index, quota.work());
    quota.adopt(held)?;
    if let Some(extra) = records.next(quota.work()).await? {
        let held = operations::sdk_record_retained_bytes(&extra, quota.work());
        quota.adopt(held)?;
        return Err(guarded_boundary(
            quota.work(),
            wire::ErrorCode::Conflict,
            "distinct original reservation sources",
        ));
    }
    let (sequence, _) = origin_locator::decode(&index.value)
        .filter(|_| index.sequence == 0)
        .ok_or_else(|| {
            guarded_boundary(
                quota.work(),
                wire::ErrorCode::Storage,
                "invalid origin locator",
            )
        })?;
    let key = StreamIdempotencyKey::new(index.value.slice(8..40))
        .map_err(|error| operations::stream_error_with_work(error, quota.work()))?;
    let outcome =
        super::commit::inspect_idempotency(client, key, operation, &mut quota, None).await?;
    let IdempotencyOutcome::Commit(CommitOutcome::Committed(envelope)) = outcome else {
        return Err(guarded_boundary(
            quota.work(),
            wire::ErrorCode::Storage,
            "origin is not an atomic source commit",
        ));
    };
    if envelope.commit_id != index.commit_id || !envelope.mutations.iter().any(|mutation| {
        matches!(mutation, CommittedMutation::Append(append) if append.path == path
            && append.records.len() == 1 && append.records[0].sequence == 0
            && append.records[0].commit_id == index.commit_id && append.records[0].value == index.value)
    }) {
        return Err(guarded_boundary(quota.work(), wire::ErrorCode::Storage, "origin locator is not atomic with its source"));
    }
    let source = envelope
        .mutations
        .iter()
        .find_map(|mutation| match mutation {
            CommittedMutation::Append(append) if append.path.as_str() == COORDINATOR_PATH => append
                .records
                .iter()
                .find(|record| record.sequence == sequence && record.commit_id == index.commit_id),
            _ => None,
        })
        .ok_or_else(|| {
            guarded_boundary(
                quota.work(),
                wire::ErrorCode::Storage,
                "missing atomic origin source",
            )
        })?;
    let (revision, key, _, time, event) = decode(source, quota.work())?;
    let owned = event.logical_owned_bytes()?;
    quota.adopt(owned + key.capacity() as u64)?;
    if expected_key.is_some_and(|expected| expected != key) {
        return Err(guarded_boundary(
            quota.work(),
            wire::ErrorCode::Conflict,
            "original declaration retry identity differs",
        ));
    }
    let expected = event_path(&event, quota.work())?.ok_or_else(|| {
        guarded_boundary(
            quota.work(),
            wire::ErrorCode::Storage,
            "origin source is not an origin event",
        )
    })?;
    quota.adopt(expected.as_str().len() as u64)?;
    if expected != path {
        return Err(guarded_boundary(
            quota.work(),
            wire::ErrorCode::Storage,
            "origin source selector differs",
        ));
    }
    drop((key, expected, envelope, index, records, path));
    quota.replace(owned)?;
    quota.transfer();
    Ok((revision, time, event))
}
