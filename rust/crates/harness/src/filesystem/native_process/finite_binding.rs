use super::*;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use crate::filesystem::PreparationIo;

const NATIVE_SERDE_LABELS: &str = concat!(
    "struct NativeProcessRequest `executable`, `argv`, `cwd`, `environment`, ",
    "`timeout_ms`, `control_timeout_ms`, `cancellation_poll_ms`, ",
    "`maximum_output_bytes`, `maximum_result_bytes`, `mcp_stdio`, `standard_input`, `view`, ",
    "struct NativeViewOptions `root`, `maximum_volumes`, `work_per_volume`, ",
    "struct NativeViewManifest `options`, `volumes`, ",
    "struct NativeVolumeMapping `volume`, `generation`, `namespace`, `path`, ",
    "`writable`, `authority_revision`, enum NativeVolumeNamespace ",
    "`materialized_directory`, `lazy_mount`, `snapshot`, ",
    "struct McpStdioRequest `initialization`, `operation`, `method`, `params`, `maximum_bytes`, ",
    "enum McpStdioMethod `tools/list`, `tools/call`, ",
    "struct ProviderRefWire `namespace`, `family`, `version`, ",
    "struct ResourceRefWire `kind`, `provider`, `key`, `version`, enum ResourceKind ",
    "`workspace`, `generation`, `artifact`, `sandbox`, `checkpoint`, `stream`, `context`, `run`, ",
    "struct VolumeRefWire `provider`, `id`, `class`, `owner`, enum VolumeClass ",
    "`project`, `agent_private`, `session_shared`, adjacently tagged enum VolumeOwner ",
    "`kind`, `id`, `project`, `agent`, `session`, ",
    "invalid request: provider identity is invalid resource key is empty ",
    "resource version is invalid typed resource kind mismatch file or volume label is invalid ",
    "struct LazySnapshotRef `id`, `workspace_id`, `authored_generation`, `source`, `overlay`, `shadows`, ",
    "struct SourceReference `identity`, `epoch`, an array of length 16 an array of length 32 ",
    "struct WorkCounters `authorityRecordsRead`, `authorityRecordsAppended`, ",
    "`authorityBytesRead`, `authorityBytesWritten`, `objectProbes`, ",
    "`backendReadOperations`, `backendWriteOperations`, `durabilityOperations`, ",
    "`pageReads`, `pageWrites`, `objectBytesRead`, `objectBytesWritten`, ",
    "`bytesHashed`, `bytesCopied`, `bytesEncoded`, `sourceBytesRead`, ",
    "`sourcePathComponents`, `sourceEntriesVisited`, `outputBytes`, `itemsExamined`, ",
    "`itemsReturned`, `allocationOperations`, `peakAllocationBytes`, `materializations`, ",
    "path string a formatted UUID string a 16 byte array UUID parsing failed: ",
    "invalid character: found `` at invalid length: expected 16 bytes, found ",
    "invalid group count: expected 5, found invalid group length in group : expected , found ",
    "non-UTF8 input invalid length: found failed to parse a UUID ",
);

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
    let allowance = (plan.provider.capacity() as u64)
        .checked_add(plan.effect_kind.capacity() as u64)
        .ok_or_else(|| io.invalid("native binding plan ownership overflow"))?;
    io.retain_owned(allowance)?;
    let held = io.guard_owned(allowance)?;
    let provider = crate::resources::copy_metadata_string(&plan.provider, io)?;
    let effect_kind = crate::resources::copy_metadata_string(&plan.effect_kind, io)?;
    let actual = (provider.capacity() as u64).checked_add(effect_kind.capacity() as u64)
        .ok_or_else(|| io.invalid("native binding plan ownership overflow"))?;
    io.release(allowance.checked_sub(actual)
        .ok_or_else(|| io.invalid("native plan copy exceeded source capacity"))?)?;
    held.into_retained();
    Ok(TaskEffectPlan {
        provider, guarantee: plan.guarantee, effect_kind,
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
    held: &Option<Arc<Mutex<NativeAllocation>>>, mut incoming: NativeAllocation,
) -> Result<()> {
    let held = held.as_ref().ok_or(Error::NativeBoundary {
        code: crate::wire::ErrorCode::Storage, reason: "original provider payload custody is unavailable",
    })?;
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
    let slot = NativeReceiptCodecSlot::new(work)?;
    let io = PreparationIo::from_native_work(work)?;
    let plan = copy_plan_with_io(plan, &io)?;
    let input = plan.request.descriptor().byte_length().max(plan.result_schema.descriptor().byte_length());
    let typed = crate::contract::audited_serde_diagnostic_bytes(input, NATIVE_SERDE_LABELS.len() as u64)?;
    let diagnostic = io.reserve_decode(typed.max(crate::contract::sdk_json_diagnostic_bytes())
        .max(crate::contract::lexical_json_diagnostic_bytes()))?;
    let result = bind_body(owner, command, plan, view, approval, authority, work, &io).await;
    match result {
        Ok(mut provider) => {
            drop(diagnostic);
            let retained = io.owned_bytes()?;
            provider._custody = Some(shared_custody(io.into_native_custody(retained)?, work)?);
            Ok(provider)
        }
        Err(source) => {
            diagnostic.into_retained();
            let retained = io.owned_bytes()?;
            Err(slot.retain(source, Some(io.into_native_custody(retained)?)))
        }
    }
}

async fn bind_body<P: StreamProvider + 'static>(
    owner: Arc<TaskJournalOwner<P>>, command: OperationId, plan: TaskEffectPlan,
    view: Arc<dyn NativeProcessView<P>>, approval: InteractionId,
    authority: NativeProcessAuthority, work: &NativeMaterializationWork, io: &PreparationIo,
) -> Result<NativeProcessProvider<P>> {
    use crate::filesystem::native_contract::validation_error;
    require_view_family(view.as_ref(), work)?;
    owner.verify_with_native_work(false, work).await?;
    owner.require_effect_grants_with_native_work(&plan.provider, false, work)?;
    owner.validate_input_file_with_native_work(&plan.request, work)?;
    owner.validate_input_file_with_native_work(&plan.result_schema, work)?;
    if authority.history_limits.maximum_events == 0 || authority.history_limits.maximum_bytes == 0 {
        return Err(validation_error(Some(io), crate::wire::ErrorCode::Invalid,
            "native effect history allowances must be finite and positive"));
    }
    if plan.effect_kind != NATIVE_PROCESS_EFFECT_KIND || plan.guarantee != EffectGuarantee::AtMostOnce {
        return Err(validation_error(Some(io), crate::wire::ErrorCode::Invalid,
            "native process requires its one-shot at-most-once contract"));
    }
    if plan.request.descriptor().media_type() != "application/json" {
        return Err(validation_error(Some(io), crate::wire::ErrorCode::Invalid, "native request must be JSON content"));
    }
    let bytes = authority.content.read_with_native_work(&plan.request, owner.input_limits().file_bytes, work)?.await?;
    let request = io.decode_preparation_with_code::<NativeProcessRequest>(
        bytes.as_ref(), crate::wire::ErrorCode::Invalid)?.into_retained();
    drop(bytes);
    request.view.validate_with_io(Some(io))?;
    for mapping in &request.view.volumes {
        owner.require_volume_grant_with_native_work(&mapping.volume, crate::conversation::VolumeOperation::Read, work)?;
        if mapping.writable {
            owner.require_volume_grant_with_native_work(&mapping.volume, crate::conversation::VolumeOperation::Write, work)?;
        }
    }
    request.validate_native_paths(view.manifest(), Some(io))?;
    if request.view.volumes.iter().any(|mapping| &mapping.volume == authority.results.volume()) {
        return Err(validation_error(Some(io), crate::wire::ErrorCode::Unsupported,
            "native result storage must be separate from materialized volumes"));
    }
    if u64::from(request.maximum_result_bytes) > owner.input_limits().file_bytes {
        return Err(validation_error(Some(io), crate::wire::ErrorCode::Unauthorized,
            "native result bound exceeds retained task file allowance"));
    }
    if request.standard_input.as_ref().is_some_and(|input| input.len() as u64 > owner.input_limits().file_bytes) {
        return Err(validation_error(Some(io), crate::wire::ErrorCode::Unauthorized,
            "native stdin exceeds retained task file allowance"));
    }
    if plan.result_schema.descriptor().media_type() != "application/schema+json" {
        return Err(validation_error(Some(io), crate::wire::ErrorCode::Invalid,
            "effect schema must be JSON schema content"));
    }
    let schema = authority.content.read_with_native_work(&plan.result_schema, owner.input_limits().file_bytes, work)?.await?;
    let decoded = io.decode_preparation_with_code::<serde_json::Value>(schema.as_ref(), crate::wire::ErrorCode::Invalid)?;
    crate::contract::compile_json_schema_with_io(&decoded.value, "effect result", Some(io))?;
    let result_schema = decoded.into_retained();
    drop(schema);
    for operation in [crate::conversation::VolumeOperation::Write, crate::conversation::VolumeOperation::Read] {
        owner.require_volume_grant_with_native_work(authority.results.volume(), operation, work)?;
    }
    let approval_digest = request.approval_digest_with_io(owner.task_binding().0, command, Some(io))?;
    let effect_id = crate::effect_host::task_effect_id_with_native_work(owner.task_binding().0, command, work)?;
    let (history, history_custody) = HistoryReader::new_with_native_work(
        &owner.stream(), authority.verifier.audience(), &authority.verifier, work)?;
    io.absorb_native_custody(history_custody)?;
    authority.approvals.verify_with_native_work(approval, command, approval_digest, work)?.await?;
    let provider = NativeProcessProvider {
        owner, command, plan: arc(plan, io)?,
        admission: NativeAdmission::Executable { request: arc(request, io)?, result_schema: arc(result_schema, io)?, approval_digest },
        view: Some(view), approval, authority: arc(authority, io)?, supervisor: arc(NativeSupervisorControl::new(), io)?,
        effect_id, history: arc(history, io)?, _native_work: Some(work.clone()), _custody: None,
    };
    provider.owner.verify_with_native_work(false, work).await?;
    require_view_family(provider.view.as_ref().expect("executable binding retains its view").as_ref(), work)?;
    Ok(provider)
}
