use super::*;

pub(crate) async fn observe_declaration<P: StreamProvider>(
    client: &std::sync::Arc<P>, owner: &Authority, scope: &Scope, verifier: &AuthorityVerifier,
    operation_id: OperationId, declaration_key: &IdempotencyKey,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Option<OperationSpec>> {
    authorize_operation_scope(
        owner, scope, verifier, capability::OPERATION_OBSERVE, work,
    )?;
    let mut custody = CodecQuota::new(work, 0)?;
    let Some((_, event, ..)) = read_intent(
        client, declaration_key.as_str(), None, custody.work(),
    ).await?
    else {
        return Ok(None);
    };
    if custody.work().is_some() { custody.adopt(event.logical_owned_bytes()?)?; }
    let SchedulerEvent::Declared { spec } = event else {
        return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
            "declaration key does not identify a declaration"));
    };
    if spec.operation_id != operation_id {
        return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
            "declaration operation identity differs"));
    }
    spec.verify_owner(owner, verifier, custody.work())?;
    let spec = *spec;
    if custody.work().is_some() { custody.relinquish(std::mem::size_of::<OperationSpec>() as u64)?; }
    custody.transfer();
    Ok(Some(spec))
}

#[cfg(feature = "filesystem")]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn original_reservation_proof<'a, P: StreamProvider>(
    client: &std::sync::Arc<P>, owner: &Authority, scope: &Scope, verifier: &AuthorityVerifier,
    operation: OperationId, candidate: &'a LeaseFence, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(OperationSpec, &'a LeaseFence)> {
    if work.is_none() {
        return Err(Error::Unsupported("historical reservation needs a finite original observer".into()));
    }
    authorize_operation_scope(
        owner, scope, verifier, capability::OPERATION_OBSERVE, work,
    )?;
    let mut custody = CodecQuota::new(work, 0)?;
    let key = formatted_text(
        "task-admission:".len() + 36, format_args!("task-admission:{operation}"), custody.work(),
    )?;
    custody.adopt(key.capacity() as u64)?;
    let declaration = read_intent(
        client, &key, None, custody.work(),
    ).await;
    let key_capacity = key.capacity() as u64;
    drop(key);
    custody.relinquish(key_capacity)?;
    let (_, event, declaration_revision, _) = declaration?
        .ok_or_else(|| coordinator_boundary(custody.work(), wire::ErrorCode::NotFound,
            "original task declaration source"))?;
    custody.adopt(event.logical_owned_bytes()?)?;
    let SchedulerEvent::Declared { spec: declaration } = event else {
        return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
            "task admission does not identify a declaration"));
    };
    let mut started = false;
    if declaration.operation_id != operation {
        return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
            "historical declaration identifies another operation"));
    }
    declaration.verify_owner(owner, verifier, custody.work())?;
    let stream = coordinator_stream(client, custody.work())?;
    let path_bytes = if custody.work().as_deref()
        .is_some_and(operations::original_native_decode_mode)
    { 0 } else { COORDINATOR_PATH.len() as u64 };
    custody.adopt(path_bytes)?;
    let tail = operations::tail_with_work(client, stream.0.path(), custody.work()).await?;
    if tail < declaration_revision {
        return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
            "declaration lies beyond coordinator source"));
    }
    let mut next = declaration_revision;
    let mut admitted = false;
    let mut last_time = None;
    while next < tail {
        let maximum = (tail - next).min(u64::from(READ_PAGE_SIZE)) as u32;
        let (records, retained) = read_records(client, &stream.0, next, maximum, custody.work()).await?;
        custody.adopt(retained)?;
        if records.is_empty() {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                "historical coordinator prefix is incomplete"));
        }
        let capacity = records.capacity();
        for record in records {
            let raw_bytes = operations::sdk_record_retained_bytes(&record, custody.work());
            let mut event_custody = CodecQuota::new(custody.work(), 0)?;
            let (revision, key, _, time, event) = decode(&record, event_custody.work())?;
            let owned = event.logical_owned_bytes()?;
            event_custody.adopt(owned)?;
            event_custody.adopt(key.capacity() as u64)?;
            if last_time.is_some_and(|previous| time <= previous)
            {
                return Err(coordinator_boundary(event_custody.work(), wire::ErrorCode::Storage,
                    "historical coordinator source identity differs"));
            }
            last_time = Some(time);
            let key_capacity = key.capacity() as u64;
            drop(key);
            event_custody.relinquish(key_capacity)?;
            match &event {
                SchedulerEvent::Admitted { operation_id, reservation }
                    if *operation_id == operation && reservation.id == candidate.reservation_id
                        && reservation.placement == candidate.placement =>
                {
                    if admitted {
                        return Err(coordinator_boundary(event_custody.work(), wire::ErrorCode::Conflict,
                            "historical reservation has distinct admission origins"));
                    }
                    admitted = true;
                }
                SchedulerEvent::Started { operation_id, fence }
                    if *operation_id == operation && fence == candidate =>
                {
                    if !admitted || started {
                        return Err(coordinator_boundary(event_custody.work(), wire::ErrorCode::Conflict,
                            "historical reservation has no unique admitted start"));
                    }
                    started = true;
                }
                _ => {}
            }
            drop(event);
            drop(event_custody);
            next = revision;
            drop(record);
            custody.relinquish(raw_bytes)?;
        }
        custody.relinquish((capacity * std::mem::size_of::<operations::ObservedRecord>()) as u64)?;
    }
    drop(stream);
    custody.relinquish(path_bytes)?;
    if !started {
        return Err(coordinator_boundary(custody.work(), wire::ErrorCode::NotFound,
            "selected original reservation start"));
    }
    let declaration = *declaration;
    custody.relinquish(std::mem::size_of::<OperationSpec>() as u64)?;
    custody.transfer();
    Ok((declaration, candidate))
}
