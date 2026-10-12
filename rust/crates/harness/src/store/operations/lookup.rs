use super::*;

pub(crate) async fn find_operation_bounded_with_work<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    operation: OperationId,
    maximum_bytes: u64,
    return_event: bool,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
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
    client: &std::sync::Arc<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    operation: OperationId,
    maximum_bytes: u64,
    return_event: bool,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(Option<(Event, u64)>, u64)> {
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    verifier.verify_audience(authority, quota.work())?;
    if maximum_bytes == 0 {
        return Err(crate::store::guarded_boundary(
            quota.work(),
            crate::wire::ErrorCode::Invalid,
            "operation lookup byte bound must be positive",
        ));
    }
    let (path, path_owned) = publication_path(
        authority,
        "harness/v2/aggregate-operations/",
        Some((&operation, 36)),
        quota.work(),
    )?;
    quota.adopt(path_owned)?;
    let result = async {
        let Some(index) = one_record(client, &path, 0, quota.work()).await? else {
            return match tail_optional_with_work(client, &path, quota.work()).await? {
                None => Ok((None, 0, 0)),
                Some(_) => Err(crate::store::guarded_boundary(
                    quota.work(),
                    crate::wire::ErrorCode::Storage,
                    "operation location is missing before its committed tail",
                )),
            };
        };
        let raw_owned = sdk_record_retained_bytes(&index, quota.work());
        quota.adopt(raw_owned)?;
        if index.value.len() as u64 >= maximum_bytes {
            return Err(crate::store::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Invalid,
                "operation lookup exceeds byte bound",
            ));
        }
        if tail_with_work(client, &path, quota.work()).await? != 1 {
            return Err(crate::store::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Storage,
                "operation location is not a single immutable record",
            ));
        }
        let location = decode_location_with_work(&index.value, quota.work())?;
        quota.adopt(location.authority.id.capacity() as u64)?;
        if &location.authority != authority
            || location.operation_id != operation
            || location.revision == 0
        {
            return Err(crate::store::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Storage,
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
        Ok((Some(event), bytes, raw_owned))
    }
    .await;
    drop(path);
    let (event, bytes, raw_owned) = result?;
    quota.replace(event.as_ref().map_or(0, |(_, owned)| *owned))?;
    if return_event
        && event.is_some()
        && let Some(io) = quota.work().as_deref_mut()
    {
        io.stage_output(bytes - raw_owned, 1)?;
    }
    quota.transfer();
    Ok((event, bytes))
}

pub(in crate::store) async fn verify_operation_locator<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    index: &Record,
    location: &OperationLocation,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<((Event, u64), u64)> {
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    verifier.verify_audience(authority, quota.work())?;
    let index_bytes = index.value.len() as u64;
    if index_bytes >= maximum_bytes {
        return Err(crate::store::guarded_boundary(
            quota.work(),
            crate::wire::ErrorCode::Invalid,
            "operation lookup exceeds byte bound",
        ));
    }
    if &location.authority != authority || location.revision == 0 {
        return Err(crate::store::guarded_boundary(
            quota.work(),
            crate::wire::ErrorCode::Storage,
            "operation location identity is invalid",
        ));
    }
    let (path, path_owned) = publication_path(authority, "harness/v2/", None, quota.work())?;
    quota.adopt(path_owned)?;
    let sequence = location.revision - 1;
    let received = one_record(client, &path, sequence, quota.work()).await;
    drop(path);
    let released = quota.relinquish(path_owned);
    let record = received?;
    released?;
    let record = record.ok_or_else(|| {
        crate::store::guarded_boundary(
            quota.work(),
            crate::wire::ErrorCode::Storage,
            "indexed canonical event is missing",
        )
    })?;
    let retained = sdk_record_retained_bytes(&record, quota.work());
    quota.adopt(retained)?;
    let bytes = index_bytes
        .checked_add(record.value.len() as u64)
        .filter(|bytes| *bytes <= maximum_bytes)
        .ok_or_else(|| {
            crate::store::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Invalid,
                "operation lookup exceeds byte bound",
            )
        })?;
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
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if &location.authority != authority
        || location.revision != event.revision
        || canonical.sequence.checked_add(1) != Some(event.revision)
        || event.operation_id != location.operation_id
        || event.intent_digest != location.intent_digest
        || canonical.commit_id != index.commit_id
    {
        return Err(crate::store::guarded_boundary(
            work,
            crate::wire::ErrorCode::Storage,
            "operation location differs from its atomic canonical event",
        ));
    }
    Ok(())
}

pub(crate) async fn find_message_with_work<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    message_id: uuid::Uuid,
    through_revision: u64,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    verifier.verify_audience(authority, &mut None)?;
    let (path, owned) = {
        if authority.kind != crate::core::AggregateKind::Conversation {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "message index requires a conversation",
            ));
        }
        publication_path(
            authority,
            "harness/v2/conversation-messages/",
            Some((&message_id, 36)),
            work,
        )?
    };
    let result = find_message_at_locator_with_work(
        client,
        authority,
        verifier,
        &path,
        Some(message_id),
        None,
        through_revision,
        maximum_bytes,
        work,
    )
    .await;
    drop(path);
    release_owned_after_drop(result, owned, work)
}

pub(crate) async fn find_message_sequence_with_work<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    sequence: u64,
    through_revision: u64,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    verifier.verify_audience(authority, &mut None)?;
    let (path, owned) = {
        if sequence == 0 || authority.kind != crate::core::AggregateKind::Conversation {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "message sequence index requires a positive conversation sequence",
            ));
        }
        publication_path(
            authority,
            "harness/v2/conversation-sequences/",
            Some((&sequence, 20)),
            work,
        )?
    };
    let result = find_message_at_locator_with_work(
        client,
        authority,
        verifier,
        &path,
        None,
        Some(sequence),
        through_revision,
        maximum_bytes,
        work,
    )
    .await;
    drop(path);
    release_owned_after_drop(result, owned, work)
}

#[allow(
    clippy::too_many_arguments,
    reason = "same exact indexed locator and pinned cut with optional SDK receipts"
)]
pub(super) async fn find_message_at_locator_with_work<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    path: &StreamPath,
    expected_id: Option<uuid::Uuid>,
    expected_sequence: Option<u64>,
    through_revision: u64,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    verifier.verify_audience(authority, &mut None)?;
    if maximum_bytes == 0 {
        return Err(crate::store::guarded_boundary(
            work,
            crate::wire::ErrorCode::Invalid,
            "message lookup byte bound must be positive",
        ));
    }
    let Some(index) = one_record(client, path, 0, work).await? else {
        return match tail_optional_with_work(client, path, work).await? {
            None => Ok((None, 0)),
            Some(_) => Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Storage,
                "message location is missing before its committed tail",
            )),
        };
    };
    let index_bytes = index.value.len() as u64;
    let result = async {
        if index_bytes > maximum_bytes {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "message lookup exceeds byte bound",
            ));
        }
        if tail_with_work(client, path, work).await? != 1 {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Storage,
                "message location is not immutable",
            ));
        }
        verify_message_locator(
            client,
            authority,
            verifier,
            &index,
            expected_id,
            expected_sequence,
            through_revision,
            maximum_bytes,
            work,
        )
        .await
    }
    .await;
    let index_retained = sdk_record_retained_bytes(&index, work);
    drop(index);
    let released = match work.as_deref_mut() {
        Some(observer) => observer.release_owned(index_retained),
        None => Ok(()),
    };
    match result {
        Ok(value) => released.map(|()| value),
        Err(error) => Err(error),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "binds an exact locator and pinned cut"
)]
pub(super) async fn verify_message_locator<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    index: &Record,
    expected_id: Option<uuid::Uuid>,
    expected_sequence: Option<u64>,
    through_revision: u64,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let location = decode_message_location_with_work(&index.value, quota.work())?;
    let location_owned = location.location.authority.id.capacity() as u64;
    quota.adopt(location_owned)?;
    if &location.location.authority != authority
        || expected_id.is_some_and(|id| location.message_id != id)
        || expected_sequence.is_some_and(|sequence| location.sequence != sequence)
        || location.location.revision == 0
        || (location.sequence == 0 && expected_sequence != Some(0))
    {
        return Err(crate::store::guarded_boundary(
            quota.work(),
            crate::wire::ErrorCode::Storage,
            "message location identity is invalid",
        ));
    }
    let ((event, owned), bytes) = verify_operation_locator(
        client,
        authority,
        verifier,
        index,
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
        if !location.message_id.is_nil()
            || !matches!(
                event.payload,
                crate::core::EventPayload::ConversationBound { .. }
            )
        {
            return Err(crate::store::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Storage,
                "empty message head differs from its canonical binding",
            ));
        }
        return Ok((None, bytes));
    }
    let message = match event.payload {
        crate::core::EventPayload::ConversationMessageAppended { message } => *message,
        crate::core::EventPayload::ProjectMergePublished { receipt } => receipt.notice,
        _ => {
            return Err(crate::store::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Storage,
                "message location points to another event kind",
            ));
        }
    };
    if message.id != location.message_id || message.sequence != location.sequence {
        return Err(crate::store::guarded_boundary(
            quota.work(),
            crate::wire::ErrorCode::Storage,
            "message location differs from its canonical message",
        ));
    }
    if event.revision > through_revision {
        return Ok((None, bytes));
    }
    if let Some(io) = quota.work().as_deref_mut() {
        io.check_cancellation()?;
        io.stage_output(bytes - index.value.len() as u64, 1)?;
    }
    drop(event.scope);
    drop(event.causal_parent);
    drop(location);
    quota.relinquish(location_owned)?;
    quota.transfer();
    Ok((Some(message), bytes))
}

/// Captures one logical message head without a reducer or retained-history scan.
pub(crate) async fn latest_message<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    maximum_bytes: u64,
) -> Result<Option<crate::conversation::ConversationMessage>> {
    verifier.verify_audience(authority, &mut None)?;
    if maximum_bytes == 0 {
        return Err(Error::Invalid(
            "message lookup byte bound must be positive".into(),
        ));
    }
    let path = publication_path(authority, "harness/v2/conversation-heads/", None, &mut None)?.0;
    let through = match acyclic_stream::Stream::new(std::sync::Arc::clone(&client), path.as_str())?
        .tail()
        .await
    {
        Ok(0) => return Err(Error::Storage("message head is empty".into())),
        Ok(through) => through,
        Err(acyclic_stream::StreamError::NotFound) => {
            return match acyclic_stream::Stream::new(
                std::sync::Arc::clone(&client),
                authority.stream_path(&mut None)?,
            )?
            .tail()
            .await
            {
                Ok(0) | Err(acyclic_stream::StreamError::NotFound) => Ok(None),
                Ok(_) => Err(Error::Storage(
                    "message head is absent for a nonempty aggregate".into(),
                )),
                Err(error) => Err(error.into()),
            };
        }
        Err(error) => return Err(error.into()),
    };
    let index = one_record(client, &path, through - 1, &mut None)
        .await?
        .ok_or_else(|| {
            Error::Storage("message head is missing before its committed tail".into())
        })?;
    if index.value.len() as u64 > maximum_bytes {
        return Err(Error::Invalid("message lookup exceeds byte bound".into()));
    }
    verify_message_locator(
        client,
        authority,
        verifier,
        &index,
        None,
        Some(through - 1),
        u64::MAX,
        maximum_bytes,
        &mut None,
    )
    .await
    .map(|(message, _)| message)
}
