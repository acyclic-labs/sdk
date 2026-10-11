//! One launch/observation reader and authenticator for ordinary and native work.

use super::*;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use crate::contract::{JsonBounds, JsonCodecCharge};
use std::mem::size_of;

pub(super) struct ReadReceipt {
    pub(super) launch: LaunchReceipt<'static>,
    pub(super) observed: Option<NativeObservation>,
    // All decoded payloads precede their original accounting guard.
    pub(super) custody: Option<NativeAllocation>,
}


fn check(work: Option<&NativeMaterializationWork>) -> std::result::Result<(), NativeReceiptError> {
    if let Some(work) = work { work.remaining()?; }
    Ok(())
}

fn examine(work: Option<&NativeMaterializationWork>, operation: impl FnOnce() -> bool) -> std::result::Result<bool, NativeReceiptError> {
    if let Some(work) = work { work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?; }
    Ok(operation())
}

fn add(left: u64, right: u64) -> std::result::Result<u64, NativeReceiptError> {
    left.checked_add(right).ok_or(NativeReceiptError::Record("native receipt capacity overflows"))
}

fn multiply(left: u64, right: u64) -> std::result::Result<u64, NativeReceiptError> {
    left.checked_mul(right).ok_or(NativeReceiptError::Record("native receipt admission overflows"))
}

fn mapping_bytes(value: &super::super::NativeVolumeMapping) -> std::result::Result<u64, NativeReceiptError> {
    add(add(value.volume.logical_owned_bytes()?, value.generation.logical_owned_bytes()?)?, value.path.capacity() as u64)
}

fn file_bytes(value: &crate::conversation::FileRef) -> std::result::Result<u64, NativeReceiptError> {
    value.logical_owned_bytes().map_err(NativeReceiptError::Owner)
}

fn status_bytes(value: &EffectStatus) -> std::result::Result<u64, NativeReceiptError> {
    match value {
        EffectStatus::Succeeded { result } => file_bytes(result),
        EffectStatus::Failed { message } => Ok(message.capacity() as u64),
        EffectStatus::Planned | EffectStatus::Dispatched | EffectStatus::Indeterminate => Ok(0),
    }
}

pub(super) fn effect_bytes(value: &EffectObservation) -> std::result::Result<u64, NativeReceiptError> {
    add(value.provider.capacity() as u64, status_bytes(&value.status)?)
}

fn plan_bytes(value: &TaskEffectPlan) -> std::result::Result<u64, NativeReceiptError> {
    add(add(value.provider.capacity() as u64, value.effect_kind.capacity() as u64)?, add(file_bytes(&value.request)?, file_bytes(&value.result_schema)?)?)
}

fn observation_bytes(value: &NativeObservation) -> std::result::Result<u64, NativeReceiptError> {
    observation_parts_bytes(&value.effect, value.diagnostic.as_ref(),
        value.publication_failure.as_ref(), value.invocation_failure.as_ref())
}

fn observation_parts_bytes(
    effect: &EffectObservation, diagnostic: Option<&crate::conversation::FileRef>,
    publication_failure: Option<&NativePublicationFailure>, invocation_failure: Option<&String>,
) -> std::result::Result<u64, NativeReceiptError> {
    let mut bytes = effect_bytes(effect)?;
    if let Some(value) = diagnostic { bytes = add(bytes, file_bytes(value)?)?; }
    if let Some(value) = invocation_failure { bytes = add(bytes, value.capacity() as u64)?; }
    if let Some(value) = publication_failure {
        bytes = add(bytes, value.message.capacity() as u64)?;
        bytes = add(bytes, multiply(value.completed.capacity() as u64, size_of::<super::super::NativeVolumeMapping>() as u64)?)?;
        if let Some(value) = &value.destination { bytes = add(bytes, mapping_bytes(value)?)?; }
        for value in &value.completed { bytes = add(bytes, mapping_bytes(value)?)?; }
    }
    Ok(bytes)
}

pub(super) fn receipt_bytes(value: &Receipt<'static>) -> std::result::Result<u64, NativeReceiptError> {
    match value {
        Receipt::Launch(value) => {
            let mut bytes = size_of::<LaunchReceipt<'static>>() as u64;
            bytes = add(bytes, value.dispatch.provider.capacity() as u64)?;
            bytes = add(bytes, value.dispatch.effect_kind.capacity() as u64)?;
            bytes = add(bytes, file_bytes(&value.dispatch.request)?)?;
            bytes = add(bytes, plan_bytes(&value.plan)?)?;
            bytes = add(bytes, value.conversation.id.capacity() as u64)?;
            bytes = add(bytes, value.fence.reservation_id.capacity() as u64)?;
            add(bytes, value.fence.placement.capacity() as u64)
        }
        Receipt::Observed(value) => add(size_of::<NativeObservation>() as u64, observation_bytes(value)?),
    }
}

// serde 1.0.229: Receipt (internally tagged), EffectStatus (internally tagged),
// and VolumeOwner (adjacently tagged) form at most three Content layers. The
// remaining reference converters move their wire fields; there is no Value
// variant or from_value pass in this closed DTO. FileDescriptorWire's digest
// Vec<u8> and NativePublicationFailure.completed are its typed collections.
const CONTENT_LAYERS: u64 = 3;
const RECEIPT_LABELS: &str = concat!(
    "launch observed dispatch plan conversation task command fence approval approval_digest ",
    "provider effect_id attempt_id request_digest effect_kind request guarantee ",
    "result_schema kind id reservation_id placement state planned dispatched succeeded failed indeterminate result message ",
    "diagnostic publication_failure invocation_failure work destination completed volume generation namespace path writable authority_revision ",
    "materialized_directory lazy_mount snapshot source authored_generation overlay shadows ",
    "sha256 byte_length media_type display_name version class owner project agent session ",
    "authorityRecordsRead authorityRecordsAppended authorityBytesRead authorityBytesWritten ",
    "objectProbes backendReadOperations backendWriteOperations durabilityOperations ",
    "pageReads pageWrites objectBytesRead objectBytesWritten bytesHashed bytesCopied bytesEncoded ",
    "sourceBytesRead sourcePathComponents sourceEntriesVisited outputBytes itemsExamined itemsReturned ",
    "allocationOperations peakAllocationBytes materializations ",
    "native receipt capacity overflows native receipt admission overflows SHA-256 digest must contain exactly 32 bytes"
);

pub(super) fn decode(
    bytes: &[u8], work: Option<&NativeMaterializationWork>,
    custody: &mut Option<NativeAllocation>, _marker: OperationId,
) -> std::result::Result<Receipt<'static>, NativeReceiptError> {
    let Some(work) = work else {
        return crate::contract::json_from_slice(bytes)
            .map_err(|source| NativeReceiptError::Decode { source, custody: None });
    };
    let remaining = work.remaining()?;
    let bounds = JsonBounds { maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
        maximum_items: remaining.items_examined, maximum_depth: 256 };
    let mut items = 0u64;
    let guard = custody.as_mut().ok_or(NativeReceiptError::Record("native receipt custody is missing"))?;
    let previous = guard.bytes();
    guard.resize_retained(add(previous, crate::contract::lexical_json_diagnostic_bytes())?)?;
    crate::contract::admit_json(bytes, bounds, &mut |charge| {
        let result = match charge {
            JsonCodecCharge::Work { items: visited, bytes: encoded } => {
                items = match items.checked_add(visited) {
                    Some(items) => items,
                    None => return Err(Error::NativeWork {
                        cause: acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow),
                        custody: None,
                    }),
                };
                work.charge(WorkCounters { items_examined: visited, bytes_encoded: encoded, ..WorkCounters::default() })
            }
            JsonCodecCharge::AllocationOperation { .. } => work.charge(WorkCounters {
                allocation_operations: 1, ..WorkCounters::default()
            }),
            JsonCodecCharge::CopyAdmission { bytes } => work.admit(WorkCounters {
                bytes_copied: bytes, ..WorkCounters::default()
            }),
            JsonCodecCharge::Read { .. } => work.remaining().map(|_| ()),
            _ => work.remaining().map(|_| ()),
        };
        result.map_err(|cause| Error::NativeWork { cause, custody: None })
    }).map_err(|source| NativeReceiptError::Codec { source, custody: custody.take() })?;
    custody.as_mut().ok_or(NativeReceiptError::Record("native receipt custody is missing"))?
        .substitute(previous)?;
    let slots = multiply(items, 10)?;
    let content_slot = size_of::<(serde::__private229::de::Content<'static>, serde::__private229::de::Content<'static>)>()
        .max(size_of::<serde::__private229::de::Content<'static>>()) as u64;
    let typed_slot = size_of::<super::super::NativeVolumeMapping>().max(size_of::<u8>()) as u64;
    let mut maximum = multiply(multiply(slots, content_slot)?, CONTENT_LAYERS)?;
    maximum = add(maximum, multiply(slots, typed_slot)?)?;
    maximum = add(maximum, size_of::<LaunchReceipt<'static>>().max(size_of::<NativeObservation>()) as u64)?;
    maximum = add(maximum, multiply(bytes.len() as u64, CONTENT_LAYERS + 1)?)?;
    maximum = add(maximum, multiply((bytes.len() as u64).max(8), 2)?)?;
    maximum = add(maximum, crate::contract::audited_serde_diagnostic_bytes(bytes.len() as u64, RECEIPT_LABELS.len() as u64)?)?;
    let guard = custody.as_mut().ok_or(NativeReceiptError::Record("native receipt custody is missing"))?;
    guard.resize_retained(add(previous, maximum)?)?;
    let value: Receipt<'static> = crate::contract::json_from_slice(bytes)
        .map_err(|source| NativeReceiptError::Decode { source, custody: custody.take() })?;
    let retained = receipt_bytes(&value)?;
    if retained > maximum { return Err(NativeReceiptError::Record("native receipt exceeds its audited decode admission")); }
    custody.as_mut().ok_or(NativeReceiptError::Record("native receipt custody is missing"))?
        .resize_retained(add(previous, retained)?)?;
    Ok(value)
}

pub(super) async fn read<P: StreamProvider>(
    owner: &TaskJournalOwner<P>, path: &StreamPath, work: Option<&NativeMaterializationWork>,
    marker: OperationId,
) -> std::result::Result<Option<ReadReceipt>, NativeReceiptError> {
    // Guards precede the physical records/decoded payloads on every unwind path.
    let mut custody = work.map(|work| work.retain_owned(0)).transpose()?;
    let mut raw_custody = None;
    check(work)?;
    let stream = owner.stream().stream(path.as_str()).map_err(NativeReceiptError::Backend)?;
    if let Some(work) = work { work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?; }
    let bounds = match stream.bounds().await {
        Ok(bounds) => bounds,
        Err(StreamError::NotFound) => return Ok(None),
        Err(error) => return Err(NativeReceiptError::Backend(error)),
    };
    check(work)?;
    if !(1..=2).contains(&bounds.tail) { return Err(NativeReceiptError::Record("invalid native receipt length")); }
    let limit = bounds.tail as u32;
    if let Some(work) = work {
        raw_custody = Some(work.retain_owned(multiply(bounds.tail, acyclic_stream::MAX_RECORD_BYTES as u64)?)?);
        work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
    }
    let mut records = stream.read(0, limit).await?;
    check(work)?;
    let first = records.try_next().await?.ok_or(NativeReceiptError::Record("missing native launch receipt"))?;
    receive(&first, work)?;
    if first.sequence != 0 { return Err(NativeReceiptError::Record("native receipt starts after launch")); }
    let Receipt::Launch(launch) = decode(&first.value, work, &mut custody, marker)? else {
        return Err(NativeReceiptError::Record("native launch receipt required"));
    };
    drop(first);
    if let Some(raw) = raw_custody.as_mut() { raw.release(acyclic_stream::MAX_RECORD_BYTES as u64)?; }
    check(work)?;
    let observed = match records.try_next().await? {
        Some(record) => {
            receive(&record, work)?;
            let Receipt::Observed(observed) = decode(&record.value, work, &mut custody, marker)? else {
                return Err(NativeReceiptError::Record("native observed receipt required"));
            };
            if !examine(work, || record.sequence == 1)?
                || !examine(work, || observed.effect.provider == launch.dispatch.provider)?
                || !examine(work, || observed.effect.effect_id == launch.dispatch.effect_id)?
                || !examine(work, || observed.effect.attempt_id == launch.dispatch.attempt_id)?
                || !examine(work, || observed.effect.request_digest == launch.dispatch.request_digest)?
                || !examine(work, || observed.effect.guarantee == launch.dispatch.guarantee)?
                || !examine(work, || observed.diagnostic.is_none() || observed.effect.status == EffectStatus::Indeterminate)?
                || !examine(work, || !matches!(observed.effect.status, EffectStatus::Planned | EffectStatus::Dispatched))? {
                return Err(NativeReceiptError::Record("native observation identity is invalid"));
            }
            drop(record);
            if let Some(raw) = raw_custody.as_mut() { raw.resize_retained(0)?; }
            Some(*observed)
        }
        None => None,
    };
    check(work)?;
    if bounds.tail != 1 + u64::from(observed.is_some()) || records.try_next().await?.is_some() {
        return Err(NativeReceiptError::Record("native receipt sequence is invalid"));
    }
    check(work)?;
    Ok(Some(ReadReceipt { launch: *launch, observed, custody }))
}

fn receive(record: &acyclic_stream::Record, work: Option<&NativeMaterializationWork>) -> std::result::Result<(), NativeReceiptError> {
    if let Some(work) = work {
        work.spent(WorkCounters { authority_records_read: 1, authority_bytes_read: record.value.len() as u64,
            ..WorkCounters::default() })?;
    }
    if record.value.len() > acyclic_stream::MAX_RECORD_BYTES { return Err(NativeReceiptError::Record("native receipt exceeds Stream limit")); }
    check(work)
}

fn effect_identity(
    task: crate::TaskId, command: OperationId, work: &NativeMaterializationWork,
) -> std::result::Result<crate::EffectId, NativeReceiptError> {
    let remaining = work.remaining()?;
    let mut custody = work.retain_owned(0)?;
    let mut opaque_custody: Option<NativeAllocation> = None;
    let encoded = crate::contract::canonical_json_bytes_bounded(
        &("harness/v2/task-effect", task, command),
        JsonBounds { maximum_bytes: remaining.bytes_encoded,
            maximum_items: remaining.items_examined, maximum_depth: 256 },
        &mut |charge| {
            let result = (|| match charge {
                JsonCodecCharge::Work { bytes, items } => work.charge(WorkCounters {
                    bytes_encoded: bytes, items_examined: items, ..WorkCounters::default()
                }),
                JsonCodecCharge::Copy { bytes } => work.charge(WorkCounters {
                    bytes_copied: bytes, ..WorkCounters::default()
                }),
                JsonCodecCharge::CopyAdmission { bytes } => work.admit(WorkCounters {
                    bytes_copied: bytes, ..WorkCounters::default()
                }),
                JsonCodecCharge::Retain { bytes } => {
                    let previous = custody.bytes();
                    match previous.checked_add(bytes) {
                        Some(retained) => {
                            if let Some(mut opaque) = opaque_custody.take() {
                                custody.absorb(&mut opaque)?;
                            }
                            custody.resize_retained(retained)
                        }
                        None => Err(acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow)),
                    }
                },
                JsonCodecCharge::AllocationOperation { .. } => work.charge(WorkCounters {
                    allocation_operations: 1, ..WorkCounters::default()
                }),
                JsonCodecCharge::Release { bytes } => custody.release(bytes),
                JsonCodecCharge::Read { .. } => work.remaining().map(|_| ()),
                JsonCodecCharge::Admission { maximum_owned_bytes } => {
                    let mut reserved = work.retain_owned(maximum_owned_bytes)?;
                    if let Some(opaque) = opaque_custody.as_mut() {
                        opaque.absorb(&mut reserved)
                    } else {
                        opaque_custody = Some(reserved);
                        Ok(())
                    }
                },
            })();
            result.map_err(|cause| Error::NativeWork { cause, custody: None })
        },
    );
    let bytes = match encoded {
        Ok(bytes) => bytes,
        Err(source) => {
            if let Some(mut opaque) = opaque_custody.take() {
                custody.absorb(&mut opaque)?;
            }
            return Err(NativeReceiptError::Codec { source, custody: Some(custody) });
        }
    };
    work.charge(WorkCounters { bytes_hashed: bytes.len() as u64, ..WorkCounters::default() })?;
    let hash = blake3::hash(&bytes);
    let mut id = [0; 16];
    id.copy_from_slice(&hash.as_bytes()[..16]);
    drop(bytes);
    Ok(crate::EffectId::from_bytes(id))
}

/// Authenticates one protected launch against the original qualified effect reader.
///
/// Admission, approval and namespace checks remain with the caller; both cold
/// observation and detached lost-ack recovery use this exact archive comparison.
pub(super) async fn authenticate_core_launch<P: StreamProvider>(
    reader: &HistoryReader<P>,
    launch: &LaunchReceipt<'_>,
    authority: &NativeProcessAuthority,
    work: Option<&NativeMaterializationWork>,
) -> std::result::Result<(), NativeReceiptError> {
    let mut native_io = work.map(crate::store::history::native_work_io).transpose()?;
    let result = async {
        let io = native_io.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo);
        let cursor = reader.pin_with_work(0, io).await?;
        reader.effect_with_work(&cursor, launch.dispatch.effect_id, authority.history_limits,
            native_io.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo)).await
    }.await;
    let state = match result {
        Ok(Some(state)) => state,
        Ok(None) => return Err(NativeReceiptError::Record("native receipt has no retained core effect")),
        Err(error) => return Err(NativeReceiptError::Owner(error)),
    };
    if !examine(work, || state.request_digest == launch.dispatch.request_digest)?
        || !examine(work, || state.request == launch.plan.request)?
        || !examine(work, || state.provider == launch.plan.provider)?
        || !examine(work, || state.effect_kind == launch.plan.effect_kind)?
        || !examine(work, || state.guarantee == launch.plan.guarantee)?
        || !examine(work, || state.result_schema == launch.plan.result_schema)?
        || !examine(work, || state.attempts.last() == Some(&launch.dispatch.attempt_id))? {
        return Err(NativeReceiptError::Record("native receipt differs from its retained core effect"));
    }
    drop(state);
    drop(native_io);
    check(work)
}

impl<P: StreamProvider + 'static> NativeProcessProvider<P> {
    /// Reopens only an original protected receipt. Terminal tasks remain
    /// read-only: this mode has no view, dispatch, reservation, or new lease.
    ///
    /// Original approval is checked against the protected accepted launch
    /// when observing an attempt, not recreated from its digest or task ID.
    pub async fn recover_with_native_work(
        owner: Arc<TaskJournalOwner<P>>, command: OperationId, plan: TaskEffectPlan,
        approval: InteractionId, authority: NativeProcessAuthority,
        work: &NativeMaterializationWork,
    ) -> std::result::Result<Self, NativeReceiptError> {
        crate::NativeFutureBundle::new(work, || async move {
            Ok::<_, Error>((async move {
            let (outcome, outcome_custody) = owner.outcome_with_native_work(work).await?;
            if outcome.is_none() { owner.verify_with_native_work(true, work).await?; }
            owner.require_volume_grant_with_native_work(plan.request.volume(),
                crate::conversation::VolumeOperation::Read, work)?;
            owner.require_volume_grant_with_native_work(plan.result_schema.volume(),
                crate::conversation::VolumeOperation::Read, work)?;
            if authority.history_limits.maximum_events == 0 || authority.history_limits.maximum_bytes == 0 {
                return Err(NativeReceiptError::Record("native effect history allowances must be finite and positive"));
            }
            let mut io = crate::store::history::native_work_io(work)?;
            let history = HistoryReader::new_with_work(&owner.stream(),
                authority.verifier.audience(), &authority.verifier, Some(&mut io))?;
            let effect_id = effect_identity(owner.task_binding().0, command, work)?;
            let mut custody = io.into_custody();
            // Caller-owned authority and plan buffers move unchanged. Admit
            // each new service control immediately before its actual allocation.
            let mut authority_custody = work.claim(size_of::<NativeProcessAuthority>() as u64)?;
            custody.absorb(&mut authority_custody)?;
            let authority = Arc::new(authority);
            let mut supervisor_custody = work.claim(size_of::<NativeSupervisorControl>() as u64)?;
            custody.absorb(&mut supervisor_custody)?;
            let supervisor = Arc::new(NativeSupervisorControl::new());
            let mut history_custody = work.claim(size_of::<HistoryReader<P>>() as u64)?;
            custody.absorb(&mut history_custody)?;
            let history = Arc::new(history);
            let mut plan_custody = work.claim(size_of::<TaskEffectPlan>() as u64)?;
            custody.absorb(&mut plan_custody)?;
            let plan = Arc::new(plan);
            let (after, after_custody) = owner.outcome_with_native_work(work).await?;
            if after.is_none() { owner.verify_with_native_work(true, work).await?; }
            drop(after); drop(after_custody);
            check(Some(work))?;
            drop(outcome); drop(outcome_custody);
            Ok(Self { owner, command, plan, admission: NativeAdmission::ReceiptOnly,
                view: None, approval, authority, supervisor, effect_id, history,
                _native_work: Some(work.clone()),
                _custody: Some(finite_binding::shared_custody(custody, work)?) })
            }).await)
        })?.await?
    }
    /// Observe the original attempt on the caller's original native work ledger.
    /// Previous capture/publication work remains receipt data, never query work.
    pub async fn receipt_with_native_work(
        &self, attempt: crate::EffectAttemptId, work: &NativeMaterializationWork,
    ) -> std::result::Result<Option<NativeProcessReceipt>, NativeReceiptError> {
        crate::NativeFutureBundle::new(work, || async move {
            Ok::<_, Error>(self.receipt_original(attempt, Some(work)).await)
        })?.await?
    }

    pub(super) async fn receipt_original(
        &self, attempt: crate::EffectAttemptId, work: Option<&NativeMaterializationWork>,
    ) -> std::result::Result<Option<NativeProcessReceipt>, NativeReceiptError> {
        let outcome_custody = if let Some(work) = work {
            let (outcome, custody) = self.owner.outcome_with_native_work(work).await?;
            if outcome.is_none() { self.owner.verify_with_native_work(true, work).await?; }
            Some((outcome, custody))
        } else { self.owner.verify(true).await?; None };
        if let Some(work) = work {
            self.owner.require_volume_grant_with_native_work(self.plan.request.volume(), crate::conversation::VolumeOperation::Read, work)?;
        }
        let mut path_custody = None;
        if let Some(work) = work { path_custody = Some(work.claim(("harness/v2/native-process/".len() + 36) as u64)?); }
        let path = receipt_path(attempt)?;
        let Some(ReadReceipt { launch, observed, mut custody }) = read(&self.owner, &path, work, self.command).await? else { return Ok(None); };
        if !examine(work, || launch.task == self.owner.task_binding().0)?
            || !examine(work, || launch.command == self.command)?
            || !examine(work, || launch.plan.as_ref() == self.plan.as_ref())?
            || !examine(work, || launch.conversation.as_ref() == self.authority.verifier.audience())?
            || !examine(work, || launch.dispatch.effect_id == self.effect_id)?
            || !examine(work, || launch.dispatch.attempt_id == attempt)?
            || !examine(work, || launch.dispatch.provider == self.plan.provider)?
            || !examine(work, || launch.dispatch.request == self.plan.request)?
            || !examine(work, || launch.dispatch.effect_kind == self.plan.effect_kind)?
            || !examine(work, || launch.dispatch.guarantee == self.plan.guarantee)?
            || !examine(work, || launch.approval == self.approval)?
            || !examine(work, || match &self.admission {
                NativeAdmission::Executable { approval_digest, .. } => launch.approval_digest == *approval_digest,
                NativeAdmission::ReceiptOnly => true,
            })? {
            return Err(NativeReceiptError::Record("native receipt belongs to a different admission"));
        }
        authenticate_core_launch(&self.history, &launch, &self.authority, work).await?;
        if let Some(work) = work {
            let (after, after_custody) = self.owner.outcome_with_native_work(work).await?;
            if after.is_none() { self.owner.verify_with_native_work(true, work).await?; }
            drop(after); drop(after_custody);
        }
        check(work)?;
        let LaunchReceipt { dispatch, plan, conversation, task, command, fence,
            approval: _, approval_digest: _ } = launch;
        drop(plan);
        drop(conversation);
        let original_fence = fence.into_owned();
        let was_observed = observed.is_some();
        let (effect, diagnostic, publication_failure, invocation_failure, completed_work) = match observed {
            Some(observed) => {
                drop(dispatch);
                (observed.effect, observed.diagnostic, observed.publication_failure,
                    observed.invocation_failure, observed.work)
            }
            None => {
                let EffectDispatch { provider, effect_id, attempt_id, effect_kind, request,
                    guarantee, request_digest } = dispatch.into_owned();
                drop(effect_kind);
                drop(request);
                (EffectObservation { provider, effect_id, attempt_id, request_digest, guarantee,
                    status: EffectStatus::Indeterminate }, None, None, None, WorkCounters::default())
            }
        };
        let mut result = NativeProcessReceipt {
            task, command, original_fence, effect, observed: was_observed,
            diagnostic, publication_failure, invocation_failure, work: completed_work, _custody: None,
        };
        let retained = add(
            add(result.original_fence.reservation_id.capacity() as u64,
                result.original_fence.placement.capacity() as u64)?,
            observation_parts_bytes(&result.effect, result.diagnostic.as_ref(),
                result.publication_failure.as_ref(), result.invocation_failure.as_ref())?,
        )?;
        if let Some(custody) = custody.as_mut() { custody.resize_retained(retained)?; }
        result._custody = custody;
        drop(path); drop(path_custody); drop(outcome_custody);
        Ok(Some(result))
    }
}

pub(super) fn ordinary_error(error: NativeReceiptError) -> Error {
    match error {
        NativeReceiptError::Owner(error) => error,
        NativeReceiptError::Backend(error) => error.into(),
        error => Error::Storage(error.to_string()),
    }
}
