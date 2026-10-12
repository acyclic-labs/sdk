use super::*;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};
use crate::filesystem::PreparationIo;

pub(super) fn require_family(
    bound: Option<&NativeMaterializationWork>, work: &NativeMaterializationWork,
) -> Result<()> {
    let bound = bound.ok_or(Error::NativeBoundary {
        code: crate::wire::ErrorCode::Unsupported,
        reason: "native provider was not bound to an original finite work family",
    })?;
    if !bound.shares_ledger(work) {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
            reason: "native provider work differs from its original binding" });
    }
    Ok(())
}

pub(super) fn require_view_family<P: StreamProvider>(
    view: &dyn NativeProcessView<P>, work: &NativeMaterializationWork,
) -> Result<()> {
    require_family(view.original_native_work(), work)?;
    view.operation_cancellation().check()
        .map_err(acyclic_fs::NativeTreeMaterializationError::from)
        .map_err(Error::from)
}

pub(super) fn copy_plan_with_io(
    plan: &TaskEffectPlan, io: &PreparationIo,
) -> Result<TaskEffectPlan> {
    let mut observer = io.history_io();
    Ok(TaskEffectPlan {
        provider: crate::store::history::HistoryIo::clone_text(&mut observer, &plan.provider)?,
        guarantee: plan.guarantee,
        effect_kind: crate::store::history::HistoryIo::clone_text(&mut observer, &plan.effect_kind)?,
        request: plan.request.clone_with_io(io)?,
        result_schema: plan.result_schema.clone_with_io(io)?,
    })
}

pub(super) fn shared_custody(
    mut custody: NativeAllocation, work: &NativeMaterializationWork,
) -> Result<Arc<Mutex<NativeAllocation>>> {
    let mut slot = match work.claim(std::mem::size_of::<Mutex<NativeAllocation>>() as u64) {
        Ok(slot) => slot,
        Err(cause) => return Err(Error::NativeWork { cause, custody: Some(custody) }),
    };
    if let Err(cause) = custody.absorb(&mut slot) {
        return Err(Error::NativeWork { cause, custody: Some(custody) });
    }
    Ok(Arc::new(Mutex::new(custody)))
}

pub(super) fn absorb_shared(
    held: &Arc<Mutex<NativeAllocation>>, mut incoming: NativeAllocation,
) -> Result<()> {
    let mut held = held.lock().map_err(|_| Error::NativeBoundary {
        code: crate::wire::ErrorCode::Storage, reason: "original provider payload custody is poisoned",
    })?;
    held.absorb(&mut incoming)
        .map_err(|cause| Error::NativeWork { cause, custody: Some(incoming) })
}

fn arc<T>(value: T, io: &PreparationIo) -> Result<Arc<T>> {
    let bytes = std::mem::size_of::<T>() as u64;
    io.retain_owned(bytes)?;
    io.allocation_attempt(bytes)?;
    Ok(Arc::new(value))
}

pub(super) async fn bind<P: StreamProvider + 'static>(
    owner: Arc<TaskJournalOwner<P>>, command: OperationId, plan: &TaskEffectPlan,
    view: Arc<dyn NativeProcessView<P>>, approval: InteractionId,
    authority: NativeProcessAuthority, work: &NativeMaterializationWork,
) -> Result<NativeProcessProvider<P>> {
    let io = PreparationIo::from_native_work(work)?;
    let plan = copy_plan_with_io(plan, &io)?;
    use crate::filesystem::native_contract::validation_error;
    require_view_family(view.as_ref(), work)?;
    owner.verify_with_native_work(false, work).await?;
    owner.require_effect_grants(&plan.provider, false, &mut Some(&mut io.history_io()))?;
    owner.validate_input_file_with_io(&plan.request, &io)?;
    owner.validate_input_file_with_io(&plan.result_schema, &io)?;
    if authority.history_limits.maximum_events == 0 || authority.history_limits.maximum_bytes == 0 {
        return Err(validation_error(Some(&io), crate::wire::ErrorCode::Invalid,
            "native effect history allowances must be finite and positive"));
    }
    if plan.effect_kind != NATIVE_PROCESS_EFFECT_KIND || plan.guarantee != EffectGuarantee::AtMostOnce {
        return Err(validation_error(Some(&io), crate::wire::ErrorCode::Invalid,
            "native process requires its one-shot at-most-once contract"));
    }
    if plan.request.descriptor().media_type() != "application/json" {
        return Err(validation_error(Some(&io), crate::wire::ErrorCode::Invalid, "native request must be JSON content"));
    }
    let bytes = authority.content.read_with_native_work(&plan.request, owner.input_limits().file_bytes, work)?.await?;
    let request = io.decode_preparation_with_code::<NativeProcessRequest>(
        bytes.as_ref(), crate::wire::ErrorCode::Invalid)?.into_retained();
    drop(bytes);
    request.view.validate_with_io(Some(&io))?;
    for mapping in &request.view.volumes {
        owner.require_volume_grant_with_native_work(&mapping.volume, crate::conversation::VolumeOperation::Read, work)?;
        if mapping.writable {
            owner.require_volume_grant_with_native_work(&mapping.volume, crate::conversation::VolumeOperation::Write, work)?;
        }
    }
    request.validate_native_paths(view.manifest(), &io)?;
    if request.view.volumes.iter().any(|mapping| &mapping.volume == authority.results.volume()) {
        return Err(validation_error(Some(&io), crate::wire::ErrorCode::Unsupported,
            "native result storage must be separate from materialized volumes"));
    }
    if u64::from(request.maximum_result_bytes) > owner.input_limits().file_bytes {
        return Err(validation_error(Some(&io), crate::wire::ErrorCode::Unauthorized,
            "native result bound exceeds retained task file allowance"));
    }
    if request.standard_input.as_ref().is_some_and(|input| input.len() as u64 > owner.input_limits().file_bytes) {
        return Err(validation_error(Some(&io), crate::wire::ErrorCode::Unauthorized,
            "native stdin exceeds retained task file allowance"));
    }
    if plan.result_schema.descriptor().media_type() != "application/schema+json" {
        return Err(validation_error(Some(&io), crate::wire::ErrorCode::Invalid,
            "effect schema must be JSON schema content"));
    }
    let schema = authority.content.read_with_native_work(&plan.result_schema, owner.input_limits().file_bytes, work)?.await?;
    let decoded = io.decode_preparation_with_code::<serde_json::Value>(schema.as_ref(), crate::wire::ErrorCode::Invalid)?;
    crate::contract::compile_json_schema(&decoded.value, "effect result", Some(&io))?;
    let result_schema = decoded.into_retained();
    drop(schema);
    for operation in [crate::conversation::VolumeOperation::Write, crate::conversation::VolumeOperation::Read] {
        owner.require_volume_grant_with_native_work(authority.results.volume(), operation, work)?;
    }
    let approval_digest = request.approval_digest_with_io(owner.task_binding().0, command, Some(&io))?;
    authority.approvals.verify_with_native_work(approval, command, approval_digest, work)?.await?;
    let invocation = Invocation {
        view, request: arc(request, &io)?, result_schema: arc(result_schema, &io)?, approval_digest,
    };
    let provider = assemble(io, owner, command, plan, Some(invocation), approval, authority, work)?;
    provider.owner.verify_with_native_work(false, work).await?;
    require_view_family(provider.invocation()?.view.as_ref(), work)?;
    Ok(provider)
}

pub(super) fn assemble<P: StreamProvider + 'static>(
    io: PreparationIo, owner: Arc<TaskJournalOwner<P>>, command: OperationId, plan: TaskEffectPlan,
    invocation: Option<Invocation<P>>, approval: InteractionId,
    authority: NativeProcessAuthority, work: &NativeMaterializationWork,
) -> Result<NativeProcessProvider<P>> {
    let effect_id = crate::effect_host::task_effect_id_with_native_work(owner.task_binding().0, command, work)?;
    let history = HistoryReader::new_with_work(
        &owner.stream(), authority.verifier.audience(), &authority.verifier, Some(&mut io.history_io()))?;
    let plan = arc(plan, &io)?;
    let authority = arc(authority, &io)?;
    let history = arc(history, &io)?;
    let supervisor = arc(NativeSupervisorControl::new(), &io)?;
    let retained = io.owned_bytes()?;
    Ok(NativeProcessProvider { owner, command, plan, invocation, approval, authority,
        supervisor, effect_id, history, _native_work: work.clone(),
        _custody: shared_custody(io.into_native_custody(retained)?, work)? })
}
