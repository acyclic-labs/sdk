use super::*;

pub(crate) async fn observe_declaration<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    owner: &Authority,
    scope: &Scope,
    verifier: &AuthorityVerifier,
    operation_id: OperationId,
    declaration_key: &IdempotencyKey,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Option<OperationSpec>> {
    authorize_operation_scope(owner, scope, verifier, capability::OPERATION_OBSERVE, work)?;
    let mut custody = CodecQuota::new(work, 0)?;
    let Some((_, event, ..)) =
        read_intent(client, declaration_key.as_str(), None, custody.work()).await?
    else {
        return Ok(None);
    };
    if custody.work().is_some() {
        custody.adopt(event.logical_owned_bytes()?)?;
    }
    let SchedulerEvent::Declared { spec } = event else {
        return Err(guarded_boundary(
            custody.work(),
            wire::ErrorCode::Conflict,
            "declaration key does not identify a declaration",
        ));
    };
    if spec.operation_id != operation_id {
        return Err(guarded_boundary(
            custody.work(),
            wire::ErrorCode::Conflict,
            "declaration operation identity differs",
        ));
    }
    spec.verify_owner(owner, verifier, custody.work())?;
    let spec = *spec;
    if custody.work().is_some() {
        custody.relinquish(std::mem::size_of::<OperationSpec>() as u64)?;
    }
    custody.transfer();
    Ok(Some(spec))
}

#[cfg(feature = "filesystem")]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn original_reservation_proof<'a, P: StreamProvider>(
    client: &std::sync::Arc<P>,
    owner: &Authority,
    scope: &Scope,
    verifier: &AuthorityVerifier,
    operation: OperationId,
    candidate: &'a LeaseFence,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(OperationSpec, &'a LeaseFence)> {
    if work.is_none() {
        return Err(Error::Unsupported(
            "historical reservation needs a finite original observer".into(),
        ));
    }
    authorize_operation_scope(owner, scope, verifier, capability::OPERATION_OBSERVE, work)?;
    let mut quota = CodecQuota::new(work, 0)?;
    let admission_key = formatted_text(
        "task-admission:".len() + 36,
        format_args!("task-admission:{operation}"),
        quota.work(),
    )?;
    quota.adopt(admission_key.capacity() as u64)?;
    let stream = coordinator_stream(client, quota.work())?;
    let path_bytes = if quota
        .work()
        .as_deref()
        .is_some_and(operations::original_native_decode_mode)
    {
        0
    } else {
        COORDINATOR_PATH.len() as u64
    };
    quota.adopt(path_bytes)?;
    let tail = operations::tail_optional_with_work(client, stream.0.path(), quota.work())
        .await?
        .unwrap_or(0);
    let mut declaration = None;
    let mut admitted = false;
    let mut started = false;
    let mut last_time = None;
    let mut next = 0;
    while next < tail {
        let from = next;
        let maximum = (tail - from).min(u64::from(READ_PAGE_SIZE)) as u32;
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
            if declaration.is_some()
                || inspect_envelope(&record.value, false, quota.work())?.2
                    == Some(admission_key.as_str())
            {
                let mut event_quota = CodecQuota::new(quota.work(), 0)?;
                let (_, key, _, time, event) = decode(&record, event_quota.work())?;
                let owned = event.logical_owned_bytes()?;
                event_quota.adopt(owned)?;
                let key_bytes = key.capacity() as u64;
                event_quota.adopt(key_bytes)?;
                if key == admission_key {
                    if declaration.is_some() {
                        return Err(guarded_boundary(
                            event_quota.work(),
                            wire::ErrorCode::Conflict,
                            "coordinator history repeats an idempotency key",
                        ));
                    }
                    let SchedulerEvent::Declared { spec } = event else {
                        return Err(guarded_boundary(
                            event_quota.work(),
                            wire::ErrorCode::Conflict,
                            "task admission does not identify a declaration",
                        ));
                    };
                    if spec.operation_id != operation {
                        return Err(guarded_boundary(
                            event_quota.work(),
                            wire::ErrorCode::Conflict,
                            "historical declaration identifies another operation",
                        ));
                    }
                    spec.verify_owner(owner, verifier, event_quota.work())?;
                    declaration = Some(spec);
                    drop(key);
                    event_quota.relinquish(key_bytes)?;
                    event_quota.transfer();
                    drop(event_quota);
                    quota.adopt(owned)?;
                } else {
                    if last_time.is_some_and(|previous| time <= previous) {
                        return Err(guarded_boundary(
                            event_quota.work(),
                            wire::ErrorCode::Storage,
                            "historical coordinator source identity differs",
                        ));
                    }
                    last_time = Some(time);
                    match &event {
                        SchedulerEvent::Admitted {
                            operation_id,
                            reservation,
                        } if *operation_id == operation
                            && reservation.id == candidate.reservation_id
                            && reservation.placement == candidate.placement =>
                        {
                            if admitted {
                                return Err(guarded_boundary(
                                    event_quota.work(),
                                    wire::ErrorCode::Conflict,
                                    "historical reservation has distinct admission origins",
                                ));
                            }
                            admitted = true;
                        }
                        SchedulerEvent::Started {
                            operation_id,
                            fence,
                        } if *operation_id == operation && fence == candidate => {
                            if !admitted || started {
                                return Err(guarded_boundary(
                                    event_quota.work(),
                                    wire::ErrorCode::Conflict,
                                    "historical reservation has no unique admitted start",
                                ));
                            }
                            started = true;
                        }
                        _ => {}
                    }
                    drop((key, event));
                    drop(event_quota);
                }
            }
            drop(record);
            quota.relinquish(raw_bytes)?;
        }
        if next == from {
            return Err(guarded_boundary(
                quota.work(),
                wire::ErrorCode::Storage,
                "historical coordinator prefix is incomplete",
            ));
        }
    }
    let declaration = declaration.ok_or_else(|| {
        guarded_boundary(
            quota.work(),
            wire::ErrorCode::NotFound,
            "original task declaration source",
        )
    })?;
    if !started {
        return Err(guarded_boundary(
            quota.work(),
            wire::ErrorCode::NotFound,
            "selected original reservation start",
        ));
    }
    let key_bytes = admission_key.capacity() as u64;
    drop(admission_key);
    drop(stream);
    quota.relinquish(key_bytes + path_bytes)?;
    let declaration = *declaration;
    quota.relinquish(std::mem::size_of::<OperationSpec>() as u64)?;
    quota.transfer();
    Ok((declaration, candidate))
}
