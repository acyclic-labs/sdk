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

enum ContentPayload {
    Ordinary(Vec<u8>),
    Original(acyclic_stream::OwnedBytes),
}

impl AsRef<[u8]> for ContentPayload {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Ordinary(bytes) => bytes,
            Self::Original(bytes) => bytes.as_ref(),
        }
    }
}

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

async fn content(
    authority: &NativeProcessAuthority, reference: &crate::conversation::FileRef,
    maximum: u64, work: Option<&NativeMaterializationWork>,
) -> Result<ContentPayload> {
    match work {
        Some(work) => {
            let bytes = authority.content.read_with_native_work(reference, maximum, work)?.await?;
            Ok(ContentPayload::Original(bytes))
        }
        None => Ok(ContentPayload::Ordinary(authority.content.read(reference).await?)),
    }
}

fn arc<T>(value: T, io: Option<&PreparationIo>) -> Result<Arc<T>> {
    if let Some(io) = io {
        let bytes = std::mem::size_of::<T>() as u64;
        io.retain_owned(bytes)?;
        io.allocation_attempt(bytes)?;
    }
    Ok(Arc::new(value))
}

pub(super) async fn bind_original<P: StreamProvider + 'static>(
    owner: Arc<TaskJournalOwner<P>>, command: OperationId, plan: Cow<'_, TaskEffectPlan>,
    view: Option<Arc<dyn NativeProcessView<P>>>, approval: InteractionId,
    authority: NativeProcessAuthority, work: Option<&NativeMaterializationWork>,
) -> Result<NativeProcessProvider<P>> {
    let codec_slot = work.map(NativeReceiptCodecSlot::new).transpose()?;
    let io = work.map(PreparationIo::from_native_work).transpose()?;
    let plan = match plan {
        Cow::Owned(plan) => plan,
        Cow::Borrowed(plan) => copy_plan_with_io(plan, io.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason: "borrowed native plan requires original preparation IO",
        })?)?,
    };
    let diagnostic = io.as_ref().map(|io| {
        let input = plan.request.descriptor().byte_length()
            .max(plan.result_schema.descriptor().byte_length());
        let typed = crate::contract::audited_serde_diagnostic_bytes(input, NATIVE_SERDE_LABELS.len() as u64)?;
        io.reserve_decode(typed.max(crate::contract::sdk_json_diagnostic_bytes())
            .max(crate::contract::lexical_json_diagnostic_bytes()))
    }).transpose()?;
    let result = bind_body(owner, command, plan, view, approval, authority, work, io.as_ref()).await;
    match result {
        Ok(mut provider) => {
            drop(diagnostic);
            if let Some(io) = io {
                let retained = io.owned_bytes()?;
                let custody = io.into_native_custody(retained)?;
                provider._custody = Some(shared_custody(custody,
                    work.expect("native preparation IO carries its original work"))?);
            }
            Ok(provider)
        }
        Err(source) => {
            if let Some(diagnostic) = diagnostic { diagnostic.into_retained(); }
            let custody = match io {
                Some(io) => { let retained = io.owned_bytes()?; Some(io.into_native_custody(retained)?) }
                None => None,
            };
            Err(match codec_slot {
                Some(slot) => super::finite_dispatch::root_error(slot.retain(source, custody)),
                None => source,
            })
        }
    }
}

async fn bind_body<P: StreamProvider + 'static>(
    owner: Arc<TaskJournalOwner<P>>, command: OperationId, plan: TaskEffectPlan,
    view: Option<Arc<dyn NativeProcessView<P>>>, approval: InteractionId,
    authority: NativeProcessAuthority, work: Option<&NativeMaterializationWork>,
    io: Option<&PreparationIo>,
) -> Result<NativeProcessProvider<P>> {
    use crate::filesystem::native_contract::validation_error;
    let allow_cancelled = view.is_none();
    match work {
        Some(work) => {
            if let Some(view) = &view { require_view_family(view.as_ref(), work)?; }
            owner.verify_with_native_work(allow_cancelled, work).await?;
            owner.require_effect_grants_with_native_work(&plan.provider, false, work)?;
            owner.validate_input_file_with_native_work(&plan.request, work)?;
            owner.validate_input_file_with_native_work(&plan.result_schema, work)?;
        }
        None => {
            owner.verify(allow_cancelled).await?;
            owner.require_effect_grants(&plan.provider, false)?;
            owner.validate_input_file(&plan.request)?;
            owner.validate_input_file(&plan.result_schema)?;
        }
    }
    if authority.history_limits.maximum_events == 0 || authority.history_limits.maximum_bytes == 0 {
        return Err(validation_error(io, crate::wire::ErrorCode::Invalid,
            "native effect history allowances must be finite and positive"));
    }
    if plan.effect_kind != NATIVE_PROCESS_EFFECT_KIND || plan.guarantee != EffectGuarantee::AtMostOnce {
        return Err(validation_error(io, crate::wire::ErrorCode::Invalid,
            "native process requires its one-shot at-most-once contract"));
    }
    if plan.request.descriptor().media_type() != "application/json" {
        return Err(validation_error(io, crate::wire::ErrorCode::Invalid, "native request must be JSON content"));
    }
    let bytes = content(&authority, &plan.request, owner.input_limits().file_bytes, work).await?;
    if io.is_none() { plan.request.descriptor().verify(bytes.as_ref())?; }
    let request = match io {
        Some(io) => io.decode_preparation_with_code::<NativeProcessRequest>(
            bytes.as_ref(), crate::wire::ErrorCode::Invalid)?.into_retained(),
        None => crate::contract::json_from_slice(bytes.as_ref())
            .map_err(|error| Error::Invalid(error.to_string()))?,
    };
    drop(bytes);
    request.view.validate_with_io(io)?;
    for mapping in &request.view.volumes {
        match work {
            Some(work) => {
                owner.require_volume_grant_with_native_work(&mapping.volume, crate::conversation::VolumeOperation::Read, work)?;
                if mapping.writable { owner.require_volume_grant_with_native_work(&mapping.volume,
                    crate::conversation::VolumeOperation::Write, work)?; }
            }
            None => {
                owner.require_volume_grant(&mapping.volume, crate::conversation::VolumeOperation::Read)?;
                if mapping.writable { owner.require_volume_grant(&mapping.volume,
                    crate::conversation::VolumeOperation::Write)?; }
            }
        }
    }
    if let Some(view) = &view { request.validate_native_paths_with_io(view.manifest(), io)?; }
    else { request.validate_with_io(io)?; }
    if request.view.volumes.iter().any(|mapping| &mapping.volume == authority.results.volume()) {
        return Err(validation_error(io, crate::wire::ErrorCode::Unsupported,
            "native result storage must be separate from materialized volumes"));
    }
    if u64::from(request.maximum_result_bytes) > owner.input_limits().file_bytes {
        return Err(validation_error(io, crate::wire::ErrorCode::Unauthorized,
            "native result bound exceeds retained task file allowance"));
    }
    if request.standard_input.as_ref().is_some_and(|input| input.len() as u64 > owner.input_limits().file_bytes) {
        return Err(validation_error(io, crate::wire::ErrorCode::Unauthorized,
            "native stdin exceeds retained task file allowance"));
    }
    if io.is_some() && plan.result_schema.descriptor().media_type() != "application/schema+json" {
        return Err(validation_error(io, crate::wire::ErrorCode::Invalid,
            "effect schema must be JSON schema content"));
    }
    let schema = content(&authority, &plan.result_schema, owner.input_limits().file_bytes, work).await?;
    let result_schema = match io {
        Some(io) => {
            let decoded = io.decode_preparation_with_code::<serde_json::Value>(
                schema.as_ref(), crate::wire::ErrorCode::Invalid)?;
            crate::contract::compile_json_schema(&decoded.value, "effect result", Some(io))?;
            decoded.into_retained()
        }
        None => crate::effects::validate_schema_bytes(&plan.result_schema, schema.as_ref())?,
    };
    drop(schema);
    for operation in [crate::conversation::VolumeOperation::Write, crate::conversation::VolumeOperation::Read] {
        match work {
            Some(work) => owner.require_volume_grant_with_native_work(authority.results.volume(), operation, work)?,
            None => owner.require_volume_grant(authority.results.volume(), operation)?,
        }
    }
    let approval_digest = request.approval_digest_with_io(owner.task_binding().0, command, io)?;
    let effect_id = match work {
        Some(work) => crate::effect_host::task_effect_id_with_native_work(owner.task_binding().0, command, work)?,
        None => task_effect_id(owner.task_binding().0, command)?,
    };
    let history = HistoryReader::new(&owner.stream(), authority.verifier.audience(), authority.verifier.clone())?;
    match work {
        Some(work) => authority.approvals.verify_with_native_work(approval, command, approval_digest, work)?.await?,
        None => authority.approvals.verify(approval, command, approval_digest).await?,
    }
    let provider = NativeProcessProvider {
        owner, command, plan: arc(plan, io)?,
        admission: NativeAdmission::Executable { request: arc(request, io)?, result_schema: arc(result_schema, io)?, approval_digest },
        view, approval, authority: arc(authority, io)?, supervisor: arc(NativeSupervisorControl::new(), io)?,
        effect_id, history: arc(history, io)?, _native_work: work.cloned(), _custody: None,
    };
    match work {
        Some(work) => provider.owner.verify_with_native_work(allow_cancelled, work).await?,
        None => provider.owner.verify(allow_cancelled).await?,
    }
    if let (Some(work), Some(view)) = (work, &provider.view) { require_view_family(view.as_ref(), work)?; }
    Ok(provider)
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_fs::{CancellationToken, WorkBudget};

    #[test]
    fn ledger_membership_is_zero_work_and_does_not_grant_uncancelled_authority() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let alias = work.clone();
        let before = work.receipt();
        cancellation.cancel();
        assert!(require_family(Some(&alias), &work).is_ok());
        assert_eq!(work.receipt(), before);
        assert!(work.cancellation().is_cancelled());
    }

    #[test]
    fn foreign_binding_is_rejected_without_adoption_allocation_or_counter_changes() {
        let cancellation = CancellationToken::new();
        let original = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let foreign = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let original_before = original.receipt();
        let foreign_before = foreign.receipt();
        assert!(matches!(require_family(Some(&original), &foreign),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, .. })));
        assert!(matches!(require_family(None, &foreign),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, .. })));
        assert_eq!(original.receipt(), original_before);
        assert_eq!(foreign.receipt(), foreign_before);
    }

    #[test]
    fn cached_schema_and_shared_original_guard_survive_creator_drop_together() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let io = PreparationIo::from_native_work(&work).unwrap();
        let input = br#"{"type":"object"}"#;
        let decoded = io.decode_preparation_with_code::<serde_json::Value>(
            input, crate::wire::ErrorCode::Invalid).unwrap();
        let schema = arc(decoded.into_retained(), Some(&io)).unwrap();
        let bytes = io.owned_bytes().unwrap();
        let keeper = shared_custody(io.into_native_custody(bytes).unwrap(), &work).unwrap();
        let weak_schema = Arc::downgrade(&schema);
        let original = (schema, keeper);
        let supervised = (Arc::clone(&original.0), Arc::clone(&original.1));
        let before = work.receipt();
        drop(original);
        cancellation.cancel();
        assert!(weak_schema.upgrade().is_some());
        assert!(matches!(supervised.1.lock(), Ok(custody) if custody.work().shares_ledger(&work)));
        assert_eq!(work.receipt(), before);
        assert!(work.live_allocation_bytes() > NativeMaterializationWork::accounting_state_bytes());
        drop(supervised);
        assert!(weak_schema.upgrade().is_none());
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
    }

    #[test]
    fn native_unknown_field_diagnostic_retains_original_typed_cause_and_full_input_witness() {
        let field = "unknown_native_field".repeat(1024);
        let input = format!("{{\"{field}\":0}}").into_bytes();
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let slot = NativeReceiptCodecSlot::new(&work).unwrap();
        let io = PreparationIo::from_native_work(&work).unwrap();
        let bytes = crate::contract::audited_serde_diagnostic_bytes(
            input.len() as u64, NATIVE_SERDE_LABELS.len() as u64).unwrap();
        let diagnostic = io.reserve_decode(bytes).unwrap();
        let source = io.decode_preparation_with_code::<NativeProcessRequest>(
            &input, crate::wire::ErrorCode::Invalid).err().unwrap();
        diagnostic.into_retained();
        let retained = io.owned_bytes().unwrap();
        let custody = io.into_native_custody(retained).unwrap();
        let alias = work.retain_owned(0).unwrap();
        let before = work.receipt();
        cancellation.cancel();
        let error = super::super::finite_dispatch::root_error(slot.retain(source, Some(custody)));
        let Error::NativeCodec { code, source, custody: Some(box_custody) } = error else {
            panic!("native decoder must retain the original prepaid codec envelope");
        };
        assert_eq!(code, crate::wire::ErrorCode::Invalid);
        let cause = source.source().unwrap().downcast_ref::<Error>().unwrap();
        let Error::NativeDecode { code, source: decode, custody: Some(inner) } = cause else {
            panic!("the original typed native decode cause must not be flattened");
        };
        assert_eq!(*code, crate::wire::ErrorCode::Invalid);
        assert!(decode.is_data());
        assert!(inner.shares_ledger(&alias));
        let envelope = source.downcast_ref::<NativeReceiptCodecCause>().unwrap();
        let custody = envelope._custody.as_ref().unwrap();
        assert!(box_custody.shares_ledger(&alias));
        assert!(custody.bytes() >= bytes);
        assert!(custody.shares_ledger(&alias));
        assert_eq!(work.receipt(), before);
    }
}
