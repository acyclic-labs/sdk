use super::*;
pub(crate) async fn find_operation_bounded_with_work<P: StreamProvider>(
    client: &Arc<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    operation: OperationId,
    maximum_bytes: u64,
    return_event: bool,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(Option<Event>, u64)> {
    bounded_operation_with_retained_quota(
        client,
        authority,
        verifier,
        operation,
        maximum_bytes,
        return_event,
        work,
    )
    .await
    .map(|(event, bytes)| (event.map(|(event, _)| event), bytes))
}

pub(crate) async fn bounded_operation_with_retained_quota<P: StreamProvider>(
    client: &Arc<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    operation: OperationId,
    maximum_bytes: u64,
    return_event: bool,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(Option<(Event, u64)>, u64)> {
    let mut quota = CodecQuota::new(work, 0)?;
    verifier.verify_audience(authority, quota.work())?;
    if maximum_bytes == 0 {
        return Err(boundary(
            quota.work(),
            Invalid,
            "operation lookup byte bound must be positive",
        ));
    }
    let (path, held) = publication_path(
        authority,
        "harness/v2/aggregate-operations/",
        Some(&operation),
        quota.work(),
    )?;
    quota.adopt(held)?;
    let Some(index) = index_record(client, &path, None, maximum_bytes, &mut quota).await? else {
        return Ok((None, 0));
    };
    let raw_owned = sdk_record_retained_bytes(&index, quota.work());
    let location = decode_location_with_work(&index.value, quota.work())?;
    quota.adopt(location.authority.id.capacity() as u64)?;
    if location.operation_id != operation {
        return Err(boundary(
            quota.work(),
            Storage,
            "operation location identity is invalid",
        ));
    }
    let (event, bytes) = verify_operation_locator(
        client,
        authority,
        verifier,
        &index,
        &location,
        maximum_bytes,
        quota.work(),
    )
    .await?;
    quota.adopt(event.1)?;
    drop(location);
    drop(index);
    drop(path);
    quota.replace(event.1)?;
    if return_event && let Some(io) = quota.work().as_deref_mut() {
        io.stage_output(bytes - raw_owned, 1)?;
    }
    quota.transfer();
    Ok((Some(event), bytes))
}

pub(in crate::store) async fn verify_operation_locator<P: StreamProvider>(
    client: &Arc<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    index: &Record,
    location: &OperationLocation,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<((Event, u64), u64)> {
    let mut quota = CodecQuota::new(work, 0)?;
    verifier.verify_audience(authority, quota.work())?;
    let index_bytes = index.value.len() as u64;
    // Callers bound the locator before decoding it.
    if &location.authority != authority || location.revision == 0 {
        return Err(boundary(
            quota.work(),
            Storage,
            "operation location identity is invalid",
        ));
    }
    let path = publication_path(authority, "harness/v2/", None, quota.work())?;
    let sequence = location.revision - 1;
    let record = publication_record(client, path, sequence, &mut quota)
        .await?
        .ok_or_else(|| boundary(quota.work(), Storage, "indexed canonical event is missing"))?;
    let bytes = index_bytes
        .checked_add(record.value.len() as u64)
        .filter(|bytes| *bytes <= maximum_bytes)
        .ok_or_else(|| boundary(quota.work(), Invalid, "operation lookup exceeds byte bound"))?;
    let (event, owned) = crate::store::history::verify_history_record_with_retained_quota(
        verifier,
        authority,
        sequence,
        &record,
        quota.work(),
    )?;
    quota.adopt(owned)?;
    verify_operation_binding(authority, index, location, &record, &event, quota.work())?;
    drop(record);
    quota.replace(owned)?;
    quota.transfer();
    Ok(((event, owned), bytes))
}

pub(in crate::store) fn verify_operation_binding(
    authority: &Authority,
    index: &Record,
    location: &OperationLocation,
    canonical: &Record,
    event: &Event,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    if &location.authority != authority
        || location.revision != event.revision
        || canonical.sequence.checked_add(1) != Some(event.revision)
        || event.operation_id != location.operation_id
        || event.intent_digest != location.intent_digest
        || canonical.commit_id != index.commit_id
    {
        return Err(boundary(
            work,
            Storage,
            "operation location differs from its atomic canonical event",
        ));
    }
    Ok(())
}

pub(crate) enum MessageKey {
    Id(uuid::Uuid),
    Sequence(u64),
    Head,
}

impl From<uuid::Uuid> for MessageKey {
    fn from(id: uuid::Uuid) -> Self {
        Self::Id(id)
    }
}

pub(crate) async fn find_message_with_work<P: StreamProvider>(
    client: &Arc<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    key: impl Into<MessageKey>,
    through_revision: u64,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    let key = key.into();
    let mut quota = CodecQuota::new(work, 0)?;
    verifier.verify_audience(authority, quota.work())?;
    if maximum_bytes == 0 || matches!(key, MessageKey::Sequence(0)) {
        return Err(boundary(
            quota.work(),
            Invalid,
            "message lookup bounds are invalid",
        ));
    }
    let Some((path, held, sequence)) =
        message_index_path(client, authority, &key, &mut quota).await?
    else {
        return Ok((None, 0));
    };
    let at = matches!(key, MessageKey::Head).then_some(sequence);
    let Some(index) = index_record(client, &path, at, maximum_bytes, &mut quota).await? else {
        return Ok((None, 0));
    };
    let index_bytes = index.value.len() as u64;
    drop(path);
    quota.relinquish(held)?;
    let location = decode_message_location_with_work(&index.value, quota.work())?;
    quota.adopt(location.location.authority.id.capacity() as u64)?;
    validate_message_location(authority, &key, sequence, &location, &mut quota)?;
    let ((event, owned), bytes) = verify_operation_locator(
        client,
        authority,
        verifier,
        &index,
        &location.location,
        maximum_bytes,
        quota.work(),
    )
    .await?;
    quota.adopt(owned)?;
    if let Some(io) = quota.work().as_deref() {
        io.check_cancellation()?;
    }
    if location.sequence == 0 {
        return if location.message_id.is_nil()
            && matches!(event.payload, EventPayload::ConversationBound { .. })
        {
            Ok((None, bytes))
        } else {
            Err(boundary(
                quota.work(),
                Storage,
                "empty message head differs from its canonical binding",
            ))
        };
    }
    let message = match event.payload {
        EventPayload::ConversationMessageAppended { message } => *message,
        EventPayload::ProjectMergePublished { receipt } => receipt.notice,
        _ => {
            return Err(boundary(
                quota.work(),
                Storage,
                "message location points to another event kind",
            ));
        }
    };
    if message.id != location.message_id || message.sequence != location.sequence {
        return Err(boundary(
            quota.work(),
            Storage,
            "message location differs from its canonical message",
        ));
    }
    if event.revision > through_revision {
        return Ok((None, bytes));
    }
    if let Some(io) = quota.work().as_deref_mut() {
        io.check_cancellation()?;
        io.stage_output(bytes - index_bytes, 1)?;
    }
    drop(event.scope);
    drop(event.causal_parent);
    drop(location);
    drop(index);
    quota.replace(owned)?;
    quota.transfer();
    Ok((Some(message), bytes))
}

async fn index_record<P: StreamProvider>(
    client: &Arc<P>,
    path: &StreamPath,
    at: Option<u64>,
    maximum_bytes: u64,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<Option<ObservedRecord>> {
    let Some(index) = one_record(client, path, at.unwrap_or(0), quota.work()).await? else {
        return match tail_optional_with_work(client, path, quota.work()).await? {
            None if at.is_none() => Ok(None),
            _ => Err(boundary(
                quota.work(),
                Storage,
                "index is missing before its committed tail",
            )),
        };
    };
    let raw_owned = sdk_record_retained_bytes(&index, quota.work());
    quota.adopt(raw_owned)?;
    if index.value.len() as u64 >= maximum_bytes {
        return Err(boundary(
            quota.work(),
            Invalid,
            "indexed event exceeds byte bound",
        ));
    }
    if at.is_none() && tail_with_work(client, path, quota.work()).await? != 1 {
        return Err(boundary(quota.work(), Storage, "index is not immutable"));
    }
    Ok(Some(index))
}

fn validate_message_location(
    authority: &Authority,
    key: &MessageKey,
    sequence: u64,
    location: &MessageLocation,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<()> {
    let identity = match key {
        MessageKey::Id(id) => location.message_id == *id && location.sequence > 0,
        MessageKey::Sequence(sequence) => location.sequence == *sequence,
        MessageKey::Head => location.sequence == sequence,
    };
    if &location.location.authority != authority || !identity || location.location.revision == 0 {
        return Err(boundary(
            quota.work(),
            Storage,
            "message location identity is invalid",
        ));
    }
    Ok(())
}

async fn message_index_path<P: StreamProvider>(
    client: &Arc<P>,
    authority: &Authority,
    key: &MessageKey,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<Option<(StreamPath, u64, u64)>> {
    let (prefix, suffix): (_, Option<&dyn std::fmt::Display>) = match key {
        MessageKey::Id(id) => ("harness/v2/conversation-messages/", Some(id)),
        MessageKey::Sequence(sequence) => ("harness/v2/conversation-sequences/", Some(sequence)),
        MessageKey::Head => ("harness/v2/conversation-heads/", None),
    };
    let (path, held) = publication_path(authority, prefix, suffix, quota.work())?;
    quota.adopt(held)?;
    let sequence = if matches!(key, MessageKey::Head) {
        match tail_optional_with_work(client, &path, quota.work()).await? {
            Some(0) => return Err(boundary(quota.work(), Storage, "message head is empty")),
            Some(tail) => tail - 1,
            None => {
                let (canonical, held) =
                    publication_path(authority, "harness/v2/", None, quota.work())?;
                quota.adopt(held)?;
                return match tail_optional_with_work(client, &canonical, quota.work()).await? {
                    None | Some(0) => Ok(None),
                    Some(_) => Err(boundary(
                        quota.work(),
                        Storage,
                        "message head is absent for a nonempty aggregate",
                    )),
                };
            }
        }
    } else {
        0
    };
    Ok(Some((path, held, sequence)))
}
