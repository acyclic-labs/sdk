use super::*;

/// Reads one bounded page of verified coordinator events.
pub async fn read_coordinator_event_page<P: StreamProvider>(
    client: &std::sync::Arc<P>, after_revision: u64, limit: u32,
) -> Result<Vec<SchedulerEvent>> {
    if limit == 0 {
        return Err(Error::Invalid("coordinator page limit is out of bounds".into()));
    }
    let stream = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), COORDINATOR_PATH)?;
    let page = read_records(client, &stream, after_revision,
        limit.min(READ_PAGE_SIZE), &mut None).await?.0;
    let mut events = Vec::with_capacity(page.len());
    for record in page {
        events.push(decode(&record, &mut None)?.4);
    }
    Ok(events)
}

pub(super) async fn read_records<P: StreamProvider>(
    client: &std::sync::Arc<P>, stream: &Stream<P>, from: u64, maximum: u32,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(Vec<operations::ObservedRecord>, u64)> {
    let mut custody = CodecQuota::new(work, 0)?;
    let mut source = match operations::read_records_with_work(
        client, stream, from, maximum, custody.work(),
    ).await? {
        Ok(source) => source,
        Err(error) if *sdk_source(&error) == StreamError::NotFound && from == 0 => return Ok((Vec::new(), 0)),
        Err(error) => return Err(sdk_error(error, custody.work())),
    };
    let mut records = Vec::new();
    loop {
        if let Some(work) = custody.work().as_deref_mut() { work.check_cancellation()?; }
        let Some(record) = source.next(custody.work()).await? else {
            if let Some(work) = custody.work().as_deref_mut() { work.check_cancellation()?; }
            break;
        };
        if custody.work().is_some()
            && !custody.work().as_deref().is_some_and(operations::original_native_decode_mode)
        {
            custody.adopt(record.value.len() as u64)?;
        }
        if records.len() >= maximum as usize
            || record.value.len() > acyclic_stream::MAX_RECORD_BYTES
        {
            return Err(operations::stream_error_with_work(
                StreamError::LimitExceeded, custody.work(),
            ));
        }
        let old_capacity = records.capacity();
        if let Some(work) = custody.work().as_deref_mut() {
            work.reserve_records(&mut records, maximum as usize)?;
            custody.adopt(((records.capacity() - old_capacity)
                * std::mem::size_of::<operations::ObservedRecord>()) as u64)?;
        }
        records.push(record);
    }
    let mut expected = from;
    for record in &records {
        if record.sequence != expected {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                "coordinator record prefix has a cursor gap"));
        }
        expected = next_revision(expected)?;
    }
    let retained = custody.retained();
    custody.transfer();
    Ok((records, retained))
}

pub(super) fn coordinator_stream<P: StreamProvider>(
    client: &std::sync::Arc<P>, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(acyclic_stream::Stream<P>, Option<crate::runtime::NativeCustody>)> {
    let path = COORDINATOR_PATH;
    let portable = work.is_some()
        && !work.as_deref().is_some_and(operations::original_native_decode_mode);
    if portable {
        let work = work.as_deref_mut().expect("portable observer");
        work.admit_allocation(path.len() as u64)?;
        work.copy_bytes(path.len() as u64)?;
    }
    let stream = operations::stream(client, path, work)?;
    if portable {
        work.as_deref_mut().expect("portable observer").retain_owned(path.len() as u64)?;
    }
    Ok(stream)
}

pub(super) async fn read_intent<P: StreamProvider>(
    client: &std::sync::Arc<P>, key: &str, maximum_revision: Option<u64>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Option<([u8; 32], SchedulerEvent, u64, acyclic_stream::CommitId)>> {
    validate_idempotency_key(key, work)?;
    if maximum_revision == Some(0) { return Ok(None); }
    let mut custody = CodecQuota::new(work, 0)?;
    let source = coordinator_stream(client, custody.work())?;
    let path_bytes = if custody.work().is_some()
        && !custody.work().as_deref().is_some_and(operations::original_native_decode_mode)
    { COORDINATOR_PATH.len() as u64 } else { 0 };
    custody.adopt(path_bytes)?;
    let tail = match maximum_revision {
        Some(tail) => tail,
        None => operations::tail_optional_with_work(client, source.0.path(), custody.work())
            .await?.unwrap_or(0),
    };
    let mut next = 0;
    let mut intent = None;
    while next < tail {
        let maximum = (tail - next).min(u64::from(READ_PAGE_SIZE)) as u32;
        let (records, retained) = read_records(client, &source.0, next, maximum, custody.work()).await?;
        if records.is_empty() {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                "coordinator intent prefix is incomplete"));
        }
        custody.adopt(retained)?;
        let slots = (records.capacity() * std::mem::size_of::<operations::ObservedRecord>()) as u64;
        for record in records {
            let raw_bytes = operations::sdk_record_retained_bytes(&record, custody.work());
            if inspect_envelope(&record.value, false, custody.work())?.2 == Some(key) {
                let mut event_custody = CodecQuota::new(custody.work(), 0)?;
                let (revision, source_key, digest, _, event) = decode(&record, event_custody.work())?;
                let owned = if event_custody.work().is_some() { event.logical_owned_bytes()? } else { 0 };
                let key_bytes = if event_custody.work().is_some() { source_key.capacity() as u64 } else { 0 };
                event_custody.adopt(owned + key_bytes)?;
                drop(source_key);
                event_custody.relinquish(key_bytes)?;
                if intent.is_some() {
                    return Err(coordinator_boundary(event_custody.work(), wire::ErrorCode::Conflict,
                        "coordinator history repeats an idempotency key"));
                }
                event_custody.transfer();
                drop(event_custody);
                custody.adopt(owned)?;
                intent = Some((digest, event, revision, record.commit_id));
            }
            next = next_revision(record.sequence)?;
            drop(record);
            if custody.work().is_some() { custody.relinquish(raw_bytes)?; }
        }
        if custody.work().is_some() { custody.relinquish(slots)?; }
    }
    drop(source);
    custody.relinquish(path_bytes)?;
    custody.transfer();
    Ok(intent)
}

pub(super) fn coordinator_boundary(
    work: &Option<&mut dyn HistoryIo>, code: wire::ErrorCode, reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(operations::original_native_decode_mode) {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    match code {
        wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        wire::ErrorCode::NotFound => Error::NotFound(reason.into()),
        wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        _ => Error::Storage(reason.into()),
    }
}
