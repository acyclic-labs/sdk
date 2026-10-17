use super::*;

/// Reads one bounded page of verified coordinator events.
pub async fn read_coordinator_event_page<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    after_revision: u64,
    limit: u32,
) -> Result<Vec<SchedulerEvent>> {
    if limit == 0 {
        return Err(Error::Invalid(
            "coordinator page limit is out of bounds".into(),
        ));
    }
    let stream = Stream::new(std::sync::Arc::clone(client), COORDINATOR_PATH)?;
    let limit = limit.min(READ_PAGE_SIZE);
    let mut work = None;
    let mut quota = CodecQuota::new(&mut work, 0)?;
    let mut source = read_records(client, &stream, after_revision, limit, quota.work()).await?;
    let mut next = after_revision;
    let mut events = Vec::new();
    while let Some(record) = next_record(
        &mut source,
        &mut next,
        after_revision.saturating_add(u64::from(limit)),
        &mut quota,
    )
    .await?
    {
        events.push(decode(&record, quota.work())?.4);
    }
    Ok(events)
}

pub(super) async fn read_records<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    stream: &Stream<P>,
    from: u64,
    maximum: u32,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Option<operations::ObservedRecords>> {
    match operations::read_records(client, stream.path(), from, maximum, work).await? {
        Ok(source) => Ok(Some(source)),
        Err(error) if error == StreamError::NotFound && from == 0 => Ok(None),
        Err(error) => Err(crate::store::operations::stream_error_with_work(
            error, work,
        )),
    }
}

pub(super) async fn next_record(
    source: &mut Option<operations::ObservedRecords>,
    next: &mut u64,
    end: u64,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<Option<operations::ObservedRecord>> {
    if let Some(work) = quota.work().as_deref_mut() {
        work.check_cancellation()?;
    }
    let record = match source {
        Some(source) => source.next(quota.work()).await?,
        None => None,
    };
    let Some(record) = record else {
        if let Some(work) = quota.work().as_deref_mut() {
            work.check_cancellation()?;
        }
        return Ok(None);
    };
    if quota.work().is_some() {
        let retained = operations::sdk_record_retained_bytes(&record, quota.work());
        quota.adopt(retained)?;
    }
    if *next >= end || record.value.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(operations::stream_error_with_work(
            StreamError::LimitExceeded,
            quota.work(),
        ));
    }
    if record.sequence != *next {
        return Err(guarded_boundary(
            quota.work(),
            wire::ErrorCode::Storage,
            "coordinator record prefix has a cursor gap",
        ));
    }
    *next = next_revision(*next)?;
    Ok(Some(record))
}

pub(super) fn coordinator_stream<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(
    acyclic_stream::Stream<P>,
    Option<crate::runtime::NativeCustody>,
)> {
    let path = COORDINATOR_PATH;
    let portable = !work
        .as_deref()
        .is_some_and(operations::original_native_decode_mode);
    if let Some(work) = work.as_deref_mut().filter(|_| portable) {
        work.admit_allocation(path.len() as u64)?;
        work.copy_bytes(path.len() as u64)?;
    }
    let stream = operations::stream(client, path, work)?;
    if let Some(work) = work.as_deref_mut().filter(|_| portable) {
        work.retain_owned(path.len() as u64)?;
    }
    Ok(stream)
}

pub(super) async fn read_intent<P: StreamProvider, T>(
    client: &std::sync::Arc<P>,
    key: &str,
    maximum_revision: Option<u64>,
    project: fn(&acyclic_stream::Record, &mut CodecQuota<'_, '_>) -> Result<T>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Option<T>> {
    validate_idempotency_key(key, work)?;
    if maximum_revision == Some(0) {
        return Ok(None);
    }
    let mut quota = CodecQuota::new(work, 0)?;
    let stream = coordinator_stream(client, quota.work())?;
    let path_bytes = if quota.work().is_some()
        && !quota
            .work()
            .as_deref()
            .is_some_and(operations::original_native_decode_mode)
    {
        COORDINATOR_PATH.len() as u64
    } else {
        0
    };
    quota.adopt(path_bytes)?;
    let tail = match maximum_revision {
        Some(tail) => tail,
        None => operations::tail_optional_with_work(client, stream.0.path(), quota.work())
            .await?
            .unwrap_or(0),
    };
    let mut next = 0;
    let mut intent = None;
    while next < tail {
        let from = next;
        let maximum = u32::try_from(tail - from)
            .unwrap_or(READ_PAGE_SIZE)
            .min(READ_PAGE_SIZE);
        let mut source = read_records(client, &stream.0, from, maximum, quota.work()).await?;
        while let Some(record) = next_record(
            &mut source,
            &mut next,
            from + u64::from(maximum),
            &mut quota,
        )
        .await?
        {
            let raw_bytes = operations::sdk_record_retained_bytes(&record, quota.work());
            if inspect_envelope(&record.value, false, quota.work())?.2 == Some(key) {
                let projected = project(&record, &mut quota)?;
                if intent.is_some() {
                    return Err(guarded_boundary(
                        quota.work(),
                        wire::ErrorCode::Conflict,
                        "coordinator history repeats an idempotency key",
                    ));
                }
                intent = Some(projected);
            }
            drop(record);
            if quota.work().is_some() {
                quota.relinquish(raw_bytes)?;
            }
        }
        if next == from {
            return Err(guarded_boundary(
                quota.work(),
                wire::ErrorCode::Storage,
                "coordinator intent prefix is incomplete",
            ));
        }
    }
    drop(stream);
    quota.relinquish(path_bytes)?;
    quota.transfer();
    Ok(intent)
}

// The full event is needed for declarations, leases and workflow observations.
pub(super) fn event_intent(
    record: &acyclic_stream::Record,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<([u8; 32], SchedulerEvent, u64, acyclic_stream::CommitId)> {
    let (revision, key, digest, _, event) = decode(record, quota.work())?;
    let owned = if quota.work().is_some() {
        event.logical_owned_bytes()?
    } else {
        0
    };
    let key_bytes = if quota.work().is_some() {
        key.capacity() as u64
    } else {
        0
    };
    quota.adopt(owned + key_bytes)?;
    drop(key);
    quota.relinquish(key_bytes)?;
    Ok((digest, event, revision, record.commit_id))
}

// Only the coordinator uses this projection, against a prefix it has already
// fully decoded and applied. Revalidate framing and the content digest without
// decoding that same SchedulerEvent JSON again. The stream prefix is immutable.
pub(super) fn replayed_intent_digest(
    record: &acyclic_stream::Record,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<[u8; 32]> {
    let mut frame = CodecQuota::new(quota.work(), 0)?;
    let (envelope, digest, fields) = super::serializer::decode_envelope(record, frame.work())?;
    frame.adopt(fields)?;
    drop(envelope);
    Ok(digest)
}
