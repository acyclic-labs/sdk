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
    let key = formatted_text(51, format_args!("task-admission:{operation}"), quota.work())?;
    if quota.work().is_some() {
        quota.adopt(key.capacity() as u64)?;
    }
    let path = origins::path(operation, "declared", None, quota.work())?;
    let (declared_revision, declared_time, declared) =
        origins::read(client, operation, path, Some(&key), quota.work()).await?;
    quota.adopt(declared.logical_owned_bytes()?)?;
    let SchedulerEvent::Declared { spec } = declared else {
        return Err(guarded_boundary(
            quota.work(),
            wire::ErrorCode::Storage,
            "invalid declaration origin",
        ));
    };
    spec.verify_owner(owner, verifier, quota.work())?;
    let selector = Some((
        candidate.reservation_id.as_str(),
        candidate.placement.as_str(),
    ));
    let path = origins::path(operation, "admitted", selector, quota.work())?;
    let (admitted_revision, admitted_time, admitted) =
        origins::read(client, operation, path, None, quota.work()).await?;
    quota.adopt(admitted.logical_owned_bytes()?)?;
    let path = origins::path(operation, "started", selector, quota.work())?;
    let (started_revision, started_time, started) =
        origins::read(client, operation, path, None, quota.work()).await?;
    quota.adopt(started.logical_owned_bytes()?)?;
    if !(declared_revision < admitted_revision
        && admitted_revision < started_revision
        && declared_time < admitted_time
        && admitted_time < started_time)
    {
        return Err(guarded_boundary(
            quota.work(),
            wire::ErrorCode::Conflict,
            "reservation origins are out of order",
        ));
    }
    let owned = spec.logical_owned_bytes()?;
    drop((key, admitted, started));
    let spec = *spec;
    quota.replace(owned)?;
    quota.transfer();
    Ok((spec, candidate))
}
