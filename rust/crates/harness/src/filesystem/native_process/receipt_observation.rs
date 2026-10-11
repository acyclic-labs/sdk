//! One launch/observation reader and authenticator for ordinary and native work.

use super::*;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use crate::contract::{JsonBounds, JsonCodecCharge};
use std::mem::size_of;
use futures::StreamExt as _;

enum ReceiptRecord {
    Ordinary(acyclic_stream::Record),
    Original(acyclic_stream::NativeObservedRecord),
}

impl ReceiptRecord {
    fn record(&self) -> &acyclic_stream::Record {
        match self {
            Self::Ordinary(record) => record,
            Self::Original(record) => record.value(),
        }
    }
}

enum ReceiptRecords<'a> {
    Ordinary(acyclic_stream::RecordStream),
    Original {
        records: acyclic_stream::NativeStreamOutput<acyclic_stream::NativeRecordStream>,
        work: &'a NativeMaterializationWork,
    },
}

impl ReceiptRecords<'_> {
    async fn next(&mut self) -> std::result::Result<Option<ReceiptRecord>, Error> {
        match self {
            Self::Ordinary(records) => Ok(records.try_next().await?.map(ReceiptRecord::Ordinary)),
            Self::Original { records, work } => match records.value_mut().next().await {
                None => Ok(None),
                Some(record) => Ok(Some(ReceiptRecord::Original(
                    record.map_err(|error| Error::from(work.runtime_error(error)))??,
                ))),
            },
        }
    }
}

pub(super) fn receipt_path_with_native_work(
    attempt: crate::EffectAttemptId,
    work: &NativeMaterializationWork,
) -> std::result::Result<StreamPath, Error> {
    const PREFIX: &[u8] = b"harness/v2/native-process/";
    let mut bytes = [0_u8; PREFIX.len() + 36];
    let completed = WorkCounters {
        bytes_copied: PREFIX.len() as u64,
        bytes_encoded: 36,
        items_examined: bytes.len() as u64,
        ..WorkCounters::UNCHARGED
    };
    work.admit(completed)?;
    bytes[..PREFIX.len()].copy_from_slice(PREFIX);
    uuid::Uuid::from_bytes(attempt.into_bytes())
        .hyphenated().encode_lower(&mut bytes[PREFIX.len()..]);
    let path = std::str::from_utf8(&bytes)
        .map_err(|_| receipt_error("native receipt UUID encoding is invalid"))?;
    work.spent(completed)?;
    StreamPath::new_native(path, &work.runtime_meter_owner())
        .map_err(|error| Error::from(work.runtime_error(error)))?
        .map_err(receipt_backend)
}

pub(super) struct ReadReceipt {
    pub(super) launch: LaunchReceipt<'static>,
    pub(super) observed: Option<NativeObservation>,
    pub(super) custody: Option<NativeAllocation>,
}

fn check(work: Option<&NativeMaterializationWork>) -> std::result::Result<(), Error> {
    if let Some(work) = work { work.remaining()?; }
    Ok(())
}

fn examine(work: Option<&NativeMaterializationWork>, operation: impl FnOnce() -> bool) -> std::result::Result<bool, Error> {
    if let Some(work) = work { work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?; }
    Ok(operation())
}

fn add(left: u64, right: u64) -> std::result::Result<u64, Error> {
    left.checked_add(right).ok_or(receipt_error("native receipt capacity overflows"))
}

fn multiply(left: u64, right: u64) -> std::result::Result<u64, Error> {
    left.checked_mul(right).ok_or(receipt_error("native receipt admission overflows"))
}

fn mapping_bytes(value: &super::super::NativeVolumeMapping) -> std::result::Result<u64, Error> {
    add(add(value.volume.logical_owned_bytes()?, value.generation.logical_owned_bytes()?)?, value.path.capacity() as u64)
}

fn file_bytes(value: &crate::conversation::FileRef) -> std::result::Result<u64, Error> {
    value.logical_owned_bytes()
}

fn status_bytes(value: &EffectStatus) -> std::result::Result<u64, Error> {
    match value {
        EffectStatus::Succeeded { result } => file_bytes(result),
        EffectStatus::Failed { message } => Ok(message.capacity() as u64),
        EffectStatus::Planned | EffectStatus::Dispatched | EffectStatus::Indeterminate => Ok(0),
    }
}

pub(super) fn effect_bytes(value: &EffectObservation) -> std::result::Result<u64, Error> {
    add(value.provider.capacity() as u64, status_bytes(&value.status)?)
}

fn plan_bytes(value: &TaskEffectPlan) -> std::result::Result<u64, Error> {
    add(add(value.provider.capacity() as u64, value.effect_kind.capacity() as u64)?, add(file_bytes(&value.request)?, file_bytes(&value.result_schema)?)?)
}

fn observation_bytes(value: &NativeObservation) -> std::result::Result<u64, Error> {
    observation_parts_bytes(&value.effect, value.diagnostic.as_ref(),
        value.publication_failure.as_ref(), value.invocation_failure.as_ref(), &value.published)
}

fn observation_parts_bytes(
    effect: &EffectObservation, diagnostic: Option<&crate::conversation::FileRef>,
    publication_failure: Option<&NativePublicationFailure>, invocation_failure: Option<&String>,
    published: &Vec<NativeProcessPublishedVolume>,
) -> std::result::Result<u64, Error> {
    let mut bytes = effect_bytes(effect)?;
    if let Some(value) = diagnostic { bytes = add(bytes, file_bytes(value)?)?; }
    if let Some(value) = invocation_failure { bytes = add(bytes, value.capacity() as u64)?; }
    if let Some(value) = publication_failure {
        bytes = add(bytes, value.message.capacity() as u64)?;
        bytes = add(bytes, multiply(value.completed.capacity() as u64, size_of::<super::super::NativeVolumeMapping>() as u64)?)?;
        if let Some(value) = &value.destination { bytes = add(bytes, mapping_bytes(value)?)?; }
        for value in &value.completed { bytes = add(bytes, mapping_bytes(value)?)?; }
    }
    bytes = add(bytes, multiply(published.capacity() as u64, size_of::<NativeProcessPublishedVolume>() as u64)?)?;
    Ok(bytes)
}

pub(super) fn receipt_bytes(value: &Receipt<'static>) -> std::result::Result<u64, Error> {
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

const CONTENT_LAYERS: u64 = 3;
const RECEIPT_LABELS: &str = concat!(
    "launch observed dispatch plan conversation task command fence approval approval_digest ",
    "provider effect_id attempt_id request_digest effect_kind request guarantee ",
    "result_schema kind id reservation_id placement state planned dispatched succeeded failed indeterminate result message ",
    "diagnostic publication_failure invocation_failure work destination completed volume generation namespace path writable authority_revision ",
    "materialized_directory lazy_mount snapshot source authored_generation overlay shadows ",
    "published index operation context Authored Lazy workspace_id config profile concurrency lifecycle case_sensitivity unicode symbolic_links hard_links sparse_files limits ",
    "portable posix windows browser exclusive_writer optimistic serialized_authority ephemeral durable sensitive profile_folded preserve require_nfc identity epoch ",
    "maximum_path_bytes maximum_component_bytes maximum_path_depth maximum_object_bytes maximum_mutations_per_batch maximum_paths_per_batch maximum_checkout_dependencies maximum_directory_page_entries maximum_page_height maximum_read_bytes maximum_files_per_generation maximum_objects_per_generation maximum_generation_bytes ",
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
) -> std::result::Result<Receipt<'static>, Error> {
    let Some(work) = work else {
        return crate::contract::json_from_slice(bytes)
            .map_err(|source| Error::NativeDecode { code: crate::wire::ErrorCode::Storage, source, custody: None });
    };
    let remaining = work.remaining()?;
    let codec_slot = NativeReceiptCodecSlot::new(work)?;
    let bounds = JsonBounds { maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
        maximum_items: remaining.items_examined, maximum_depth: 256 };
    let mut items = 0u64;
    let guard = custody.as_mut().ok_or(receipt_error("native receipt custody is missing"))?;
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
        result.map_err(Error::from)
    }).map_err(|source| codec_slot.retain(source, custody.take()))?;
    custody.as_mut().ok_or(receipt_error("native receipt custody is missing"))?
        .substitute(previous)?;
    let slots = multiply(items, 10)?;
    let content_slot = size_of::<(serde::__private229::de::Content<'static>, serde::__private229::de::Content<'static>)>()
        .max(size_of::<serde::__private229::de::Content<'static>>()) as u64;
    let typed_slot = size_of::<super::super::NativeVolumeMapping>()
        .max(size_of::<NativeProcessPublishedVolume>()).max(size_of::<u8>()) as u64;
    let mut maximum = multiply(multiply(slots, content_slot)?, CONTENT_LAYERS)?;
    maximum = add(maximum, multiply(slots, typed_slot)?)?;
    maximum = add(maximum, size_of::<LaunchReceipt<'static>>().max(size_of::<NativeObservation>()) as u64)?;
    maximum = add(maximum, multiply(bytes.len() as u64, CONTENT_LAYERS + 1)?)?;
    maximum = add(maximum, multiply((bytes.len() as u64).max(8), 2)?)?;
    maximum = add(maximum, crate::contract::audited_serde_diagnostic_bytes(bytes.len() as u64, RECEIPT_LABELS.len() as u64)?)?;
    let guard = custody.as_mut().ok_or(receipt_error("native receipt custody is missing"))?;
    guard.resize_retained(add(previous, maximum)?)?;
    let value: Receipt<'static> = crate::contract::json_from_slice(bytes)
        .map_err(|source| Error::NativeDecode { code: crate::wire::ErrorCode::Storage, source, custody: custody.take() })?;
    let retained = receipt_bytes(&value)?;
    if retained > maximum { return Err(receipt_error("native receipt exceeds its audited decode admission")); }
    custody.as_mut().ok_or(receipt_error("native receipt custody is missing"))?
        .resize_retained(add(previous, retained)?)?;
    Ok(value)
}

pub(super) async fn read<P: StreamProvider>(
    owner: &TaskJournalOwner<P>, path: &StreamPath, work: Option<&NativeMaterializationWork>,
    marker: OperationId,
) -> std::result::Result<Option<ReadReceipt>, Error> {
    let mut custody = work.map(|work| work.retain_owned(0)).transpose()?;
    let client = owner.stream();
    check(work)?;
    let native_meter = work.map(NativeMaterializationWork::runtime_meter_owner);
    let bounds = if let (Some(original), Some(meter)) = (work, &native_meter) {
        match client.native_bounds(path.clone(), meter.clone()).await
            .map_err(|error| Error::from(original.runtime_error(error)))? {
            Ok(bounds) => *bounds.value(),
            Err(StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(receipt_backend(error)),
        }
    } else {
        let stream = client.stream(path.as_str()).map_err(receipt_backend)?;
        match stream.bounds().await {
            Ok(bounds) => bounds,
            Err(StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(receipt_backend(error)),
        }
    };
    check(work)?;
    if !(1..=2).contains(&bounds.tail) { return Err(receipt_error("invalid native receipt length")); }
    let limit = bounds.tail as u32;
    let mut records = if let (Some(original), Some(meter)) = (work, native_meter) {
        let records = client.native_read(acyclic_stream::ReadRequest {
            path: path.clone(), from: 0, limit,
        }, meter).await.map_err(|error| Error::from(original.runtime_error(error)))??;
        ReceiptRecords::Original { records, work: original }
    } else {
        let stream = client.stream(path.as_str()).map_err(receipt_backend)?;
        ReceiptRecords::Ordinary(stream.read(0, limit).await?)
    };
    check(work)?;
    let first = records.next().await?.ok_or(receipt_error("missing native launch receipt"))?;
    receive(first.record(), work)?;
    if first.record().sequence != 0 { return Err(receipt_error("native receipt starts after launch")); }
    let Receipt::Launch(launch) = decode(&first.record().value, work, &mut custody, marker)? else {
        return Err(receipt_error("native launch receipt required"));
    };
    drop(first);
    check(work)?;
    let observed = match records.next().await? {
        Some(record) => {
            receive(record.record(), work)?;
            let Receipt::Observed(observed) = decode(&record.record().value, work, &mut custody, marker)? else {
                return Err(receipt_error("native observed receipt required"));
            };
            if !examine(work, || record.record().sequence == 1)?
                || !examine(work, || observed.effect.provider == launch.dispatch.provider)?
                || !examine(work, || observed.effect.effect_id == launch.dispatch.effect_id)?
                || !examine(work, || observed.effect.attempt_id == launch.dispatch.attempt_id)?
                || !examine(work, || observed.effect.request_digest == launch.dispatch.request_digest)?
                || !examine(work, || observed.effect.guarantee == launch.dispatch.guarantee)?
                || !examine(work, || observed.diagnostic.is_none() || observed.effect.status == EffectStatus::Indeterminate)?
                || !examine(work, || !matches!(observed.effect.status, EffectStatus::Planned | EffectStatus::Dispatched))? {
                return Err(receipt_error("native observation identity is invalid"));
            }
            drop(record);
            Some(*observed)
        }
        None => None,
    };
    check(work)?;
    if bounds.tail != 1 + u64::from(observed.is_some()) || records.next().await?.is_some() {
        return Err(receipt_error("native receipt sequence is invalid"));
    }
    check(work)?;
    Ok(Some(ReadReceipt { launch: *launch, observed, custody }))
}

fn receive(record: &acyclic_stream::Record, work: Option<&NativeMaterializationWork>) -> std::result::Result<(), Error> {
    if let Some(work) = work {
        work.spent(WorkCounters { authority_records_read: 1, authority_bytes_read: record.value.len() as u64,
            ..WorkCounters::default() })?;
    }
    if record.value.len() > acyclic_stream::MAX_RECORD_BYTES { return Err(receipt_error("native receipt exceeds Stream limit")); }
    check(work)
}

/// Authenticates one protected launch against the original qualified effect reader.
pub(super) async fn authenticate_core_launch<P: StreamProvider>(
    reader: &HistoryReader<P>,
    launch: &LaunchReceipt<'_>,
    authority: &NativeProcessAuthority,
    work: Option<&NativeMaterializationWork>,
) -> std::result::Result<(), Error> {
    let mut native_io = work.map(crate::store::history::native_work_io).transpose()?;
    let result = async {
        let io = native_io.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo);
        let cursor = reader.pin_with_work(0, io).await?;
        reader.effect_with_work(&cursor, launch.dispatch.effect_id, authority.history_limits,
            native_io.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo)).await
    }.await;
    let state = match result {
        Ok(Some(state)) => state,
        Ok(None) => return Err(receipt_error("native receipt has no retained core effect")),
        Err(error) => return Err(error),
    };
    if !examine(work, || state.request_digest == launch.dispatch.request_digest)?
        || !examine(work, || state.request == launch.plan.request)?
        || !examine(work, || state.provider == launch.plan.provider)?
        || !examine(work, || state.effect_kind == launch.plan.effect_kind)?
        || !examine(work, || state.guarantee == launch.plan.guarantee)?
        || !examine(work, || state.result_schema == launch.plan.result_schema)?
        || !examine(work, || state.attempts.last() == Some(&launch.dispatch.attempt_id))? {
        return Err(receipt_error("native receipt differs from its retained core effect"));
    }
    drop(state);
    drop(native_io);
    check(work)
}

pub(super) enum OriginalNativeRequest {
    Cached(Arc<NativeProcessRequest>),
    Pinned { request: NativeProcessRequest, _custody: NativeAllocation },
}

impl OriginalNativeRequest {
    pub(super) fn request(&self) -> &NativeProcessRequest {
        match self {
            Self::Cached(request) => request,
            Self::Pinned { request, .. } => request,
        }
    }
}

impl<P: StreamProvider + 'static> NativeProcessProvider<P> {
    pub(super) async fn original_request_with_native_work(
        &self, work: &NativeMaterializationWork,
    ) -> Result<OriginalNativeRequest> {
        match &self.admission {
            NativeAdmission::Executable { request, .. } =>
                Ok(OriginalNativeRequest::Cached(Arc::clone(request))),
            NativeAdmission::ReceiptOnly => {
                self.owner.require_volume_grant_with_native_work(self.plan.request.volume(),
                    crate::conversation::VolumeOperation::Read, work)?;
                let bytes = self.authority.content.read_with_native_work(
                    &self.plan.request, self.owner.input_limits().file_bytes, work,
                )?.await?;
                let (request, custody) = NativeProcessRequest::decode_with_native_work(bytes.as_ref(), work)?;
                drop(bytes);
                Ok(OriginalNativeRequest::Pinned { request, _custody: custody })
            }
        }
    }

    pub(super) async fn approved_original_receipt_with_native_work(
        &self, attempt: crate::EffectAttemptId, request: &NativeProcessRequest,
        work: &NativeMaterializationWork,
    ) -> Result<NativeProcessReceipt> {
        let digest = request.approval_digest_with_native_work(
            self.owner.task_binding().0, self.command, work,
        )?;
        self.authority.approvals.verify_with_native_work(self.approval, self.command, digest, work)?.await?;
        self.receipt_original(attempt, Some(work), Some(digest)).await
            ?
            .ok_or(Error::Indeterminate(self.command))
    }

    /// Reopens only an original protected receipt.
    pub async fn recover_with_native_work(
        owner: Arc<TaskJournalOwner<P>>, command: OperationId, plan: &TaskEffectPlan,
        approval: InteractionId, authority: NativeProcessAuthority,
        work: &NativeMaterializationWork,
    ) -> std::result::Result<Self, Error> {
        crate::NativeFutureBundle::new(work, || async move {
            Ok::<_, Error>((async move {
            let (outcome, outcome_custody) = owner.outcome_with_native_work(work).await?;
            if outcome.is_none() { owner.verify_with_native_work(true, work).await?; }
            owner.require_volume_grant_with_native_work(plan.request.volume(),
                crate::conversation::VolumeOperation::Read, work)?;
            owner.require_volume_grant_with_native_work(plan.result_schema.volume(),
                crate::conversation::VolumeOperation::Read, work)?;
            if authority.history_limits.maximum_events == 0 || authority.history_limits.maximum_bytes == 0 {
                return Err(receipt_error("native effect history allowances must be finite and positive"));
            }
            let mut io = crate::store::history::native_work_io(work)?;
            let history = HistoryReader::new_with_work(&owner.stream(),
                authority.verifier.audience(), &authority.verifier, Some(&mut io))?;
            let effect_id = crate::effect_host::task_effect_id_with_native_work(owner.task_binding().0, command, work)?;
            let mut custody = io.into_custody();
            let mut copied_plan_custody;
            let plan_io = crate::filesystem::PreparationIo::from_native_work(work)?;
            let plan = finite_binding::copy_plan_with_io(plan, &plan_io)?;
            let copied_plan_bytes = plan_io.owned_bytes()?;
            copied_plan_custody = plan_io.into_native_custody(copied_plan_bytes)?;
            custody.absorb(&mut copied_plan_custody)?;
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
    pub async fn receipt_with_native_work(
        &self, attempt: crate::EffectAttemptId, work: &NativeMaterializationWork,
    ) -> std::result::Result<Option<NativeProcessReceipt>, Error> {
        crate::NativeFutureBundle::new(work, || async move {
            Ok::<_, Error>(self.receipt_original(attempt, Some(work), None).await)
        })?.await?
    }

    pub(super) async fn receipt_original(
        &self, attempt: crate::EffectAttemptId, work: Option<&NativeMaterializationWork>,
        expected_approval_digest: Option<[u8; 32]>,
    ) -> std::result::Result<Option<NativeProcessReceipt>, Error> {
        let outcome_custody = if let Some(work) = work {
            let (outcome, custody) = self.owner.outcome_with_native_work(work).await?;
            if outcome.is_none() { self.owner.verify_with_native_work(true, work).await?; }
            Some((outcome, custody))
        } else { self.owner.verify(true).await?; None };
        if let Some(work) = work {
            self.owner.require_volume_grant_with_native_work(self.plan.request.volume(), crate::conversation::VolumeOperation::Read, work)?;
        }
        let path = match work {
            Some(work) => receipt_path_with_native_work(attempt, work)?,
            None => receipt_path(attempt)?,
        };
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
            return Err(receipt_error("native receipt belongs to a different admission"));
        }
        if let Some(expected) = expected_approval_digest
            && !examine(work, || launch.approval_digest == expected)?
        {
            return Err(receipt_error("native receipt differs from its original approved request digest"));
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
        let (effect, diagnostic, publication_failure, invocation_failure, published, completed_work) = match observed {
            Some(observed) => {
                drop(dispatch);
                (observed.effect, observed.diagnostic, observed.publication_failure,
                    observed.invocation_failure, observed.published, observed.work)
            }
            None => {
                let EffectDispatch { provider, effect_id, attempt_id, effect_kind, request,
                    guarantee, request_digest } = dispatch.into_owned();
                drop(effect_kind);
                drop(request);
                (EffectObservation { provider, effect_id, attempt_id, request_digest, guarantee,
                    status: EffectStatus::Indeterminate }, None, None, None, Vec::new(), WorkCounters::default())
            }
        };
        let mut result = NativeProcessReceipt {
            task, command, original_fence, effect, observed: was_observed,
            diagnostic, publication_failure, invocation_failure, published, work: completed_work, _custody: None,
        };
        let retained = add(
            add(result.original_fence.reservation_id.capacity() as u64,
                result.original_fence.placement.capacity() as u64)?,
            observation_parts_bytes(&result.effect, result.diagnostic.as_ref(),
                result.publication_failure.as_ref(), result.invocation_failure.as_ref(), &result.published)?,
        )?;
        if let Some(custody) = custody.as_mut() { custody.resize_retained(retained)?; }
        result._custody = custody;
        drop(path); drop(outcome_custody);
        Ok(Some(result))
    }
}

