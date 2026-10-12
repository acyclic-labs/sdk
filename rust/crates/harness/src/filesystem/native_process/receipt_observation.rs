//! Authenticated original launch and observation records.

use super::*;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use std::mem::size_of;

pub(super) fn receipt_path(
    attempt: crate::EffectAttemptId,
    work: &NativeMaterializationWork,
) -> Result<StreamPath> {
    const PREFIX: &[u8] = b"harness/v2/native-process/";
    let mut bytes = [0_u8; PREFIX.len() + 36];
    let completed = WorkCounters {
        bytes_copied: PREFIX.len() as u64,
        bytes_encoded: 36,
        items_examined: bytes.len() as u64,
        ..Default::default()
    };
    work.admit(completed)?;
    bytes[..PREFIX.len()].copy_from_slice(PREFIX);
    uuid::Uuid::from_bytes(attempt.into_bytes())
        .hyphenated().encode_lower(&mut bytes[PREFIX.len()..]);
    let path = std::str::from_utf8(&bytes)
        .map_err(|_| boundary(crate::wire::ErrorCode::Storage, "native receipt UUID encoding is invalid"))?;
    work.spent(completed)?;
    StreamPath::new_native(path, &work.runtime_meter())
        .map_err(|error| Error::from(work.runtime_error(error)))?
        .map_err(receipt_backend)
}

pub(super) fn add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right).ok_or(boundary(crate::wire::ErrorCode::Storage, "native receipt capacity overflows"))
}

fn multiply(left: u64, right: u64) -> Result<u64> {
    left.checked_mul(right).ok_or(boundary(crate::wire::ErrorCode::Storage, "native receipt admission overflows"))
}

fn mapping_bytes(value: &super::super::NativeVolumeMapping) -> Result<u64> {
    add(add(value.volume.logical_owned_bytes()?, value.generation.logical_owned_bytes()?)?, value.path.capacity() as u64)
}

fn status_bytes(value: &EffectStatus) -> Result<u64> {
    match value {
        EffectStatus::Succeeded { result } => result.logical_owned_bytes(),
        EffectStatus::Failed { message } => Ok(message.capacity() as u64),
        EffectStatus::Planned | EffectStatus::Dispatched | EffectStatus::Indeterminate => Ok(0),
    }
}

pub(super) fn effect_bytes(value: &EffectObservation) -> Result<u64> {
    add(value.provider.capacity() as u64, status_bytes(&value.status)?)
}

fn plan_bytes(value: &TaskEffectPlan) -> Result<u64> {
    add(add(value.provider.capacity() as u64, value.effect_kind.capacity() as u64)?, add(value.request.logical_owned_bytes()?, value.result_schema.logical_owned_bytes()?)?)
}

fn observation_parts_bytes(
    effect: &EffectObservation, diagnostic: Option<&crate::conversation::FileRef>,
    publication_failure: Option<&NativePublicationFailure>, invocation_failure: Option<&String>,
    published: &Vec<NativeProcessPublishedVolume>,
) -> Result<u64> {
    let mut bytes = effect_bytes(effect)?;
    if let Some(value) = diagnostic { bytes = add(bytes, value.logical_owned_bytes()?)?; }
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

pub(super) fn receipt_bytes(value: &Receipt) -> Result<u64> {
    match value {
        Receipt::Launch(value) => {
            let mut bytes = size_of::<LaunchReceipt>() as u64;
            bytes = add(bytes, value.dispatch.provider.capacity() as u64)?;
            bytes = add(bytes, value.dispatch.effect_kind.capacity() as u64)?;
            bytes = add(bytes, value.dispatch.request.logical_owned_bytes()?)?;
            bytes = add(bytes, plan_bytes(&value.plan)?)?;
            bytes = add(bytes, value.conversation.id.capacity() as u64)?;
            bytes = add(bytes, value.fence.reservation_id.capacity() as u64)?;
            add(bytes, value.fence.placement.capacity() as u64)
        }
        Receipt::Observed(value) => add(size_of::<NativeObservation>() as u64, observation_parts_bytes(&value.effect, value.diagnostic.as_ref(), value.publication_failure.as_ref(), value.invocation_failure.as_ref(), &value.published)?),
    }
}

// Serde diagnostic label bound for the closed receipt graph.
const RECEIPT_DIAGNOSTIC_LABEL_BYTES: u64 = 1765;

impl super::super::PreparationRecord for Receipt {
    fn maximum_slot_bytes() -> u64 {
        let typed = size_of::<LaunchReceipt>().max(size_of::<NativeObservation>())
            .max(size_of::<super::super::NativeVolumeMapping>())
            .max(size_of::<NativeProcessPublishedVolume>()) as u64;
        let content = size_of::<(serde::__private229::de::Content<'static>, serde::__private229::de::Content<'static>)>() as u64;
        typed + 30 * content
    }
    fn diagnostic_label_bytes() -> u64 { RECEIPT_DIAGNOSTIC_LABEL_BYTES }
}

fn decode(bytes: &[u8], io: &super::super::PreparationIo,
    custody: &mut NativeAllocation,
) -> Result<Receipt> {
    if bytes.len() > acyclic_stream::MAX_RECORD_BYTES { return Err(boundary(crate::wire::ErrorCode::Storage, "native receipt exceeds Stream limit")); }
    let decoded = io.decode(std::marker::PhantomData::<Receipt>, bytes, crate::wire::ErrorCode::Storage)?;
    let (receipt, mut payload) = crate::filesystem::native_decoded(decoded)?;
    payload.substitute(receipt_bytes(&receipt)?)?;
    custody.absorb(&mut payload)?;
    Ok(receipt)
}


pub(super) async fn read<P: StreamProvider>(
    owner: &TaskJournalOwner<P>, path: &StreamPath, work: &NativeMaterializationWork,
) -> Result<Option<(LaunchReceipt<'static>, Option<NativeObservation>, NativeAllocation)>> {
    let mut custody = work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
    let client = owner.stream();
    check(work)?;
    let io = super::super::PreparationIo::from_native_work(work)?;
    let mut history_io = &io;
    let mut observer = Some(&mut history_io as &mut dyn crate::store::history::HistoryIo);
    let bounds = match crate::store::operations::bounds_with_work(&client, path, &mut observer).await? {
        Ok(bounds) => bounds,
        Err(error) if matches!(crate::distributed::sdk_source(&error), StreamError::NotFound) => return Ok(None),
        Err(error) => return Err(crate::distributed::sdk_error(error, &observer)),
    };
    check(work)?;
    if !(1..=2).contains(&bounds.tail) { return Err(boundary(crate::wire::ErrorCode::Storage, "invalid native receipt length")); }
    let stream = crate::store::operations::stream(&client, path.as_str(), &mut observer)?;
    let mut records = crate::store::operations::read_records_with_work(
        &client, &stream.0, 0, bounds.tail as u32, &mut observer,
    ).await?.map_err(|error| crate::distributed::sdk_error(error, &observer))?;
    check(work)?;
    let first = records.next(&mut observer).await?.ok_or(boundary(crate::wire::ErrorCode::Storage, "missing native launch receipt"))?;
    if first.sequence != 0 { return Err(boundary(crate::wire::ErrorCode::Storage, "native receipt starts after launch")); }
    let Receipt::Launch(launch) = decode(&first.value, &io, &mut custody)? else {
        return Err(boundary(crate::wire::ErrorCode::Storage, "native launch receipt required"));
    };
    drop(first);
    check(work)?;
    let observed = match records.next(&mut observer).await? {
        Some(record) => {
            let Receipt::Observed(observed) = decode(&record.value, &io, &mut custody)? else {
                return Err(boundary(crate::wire::ErrorCode::Storage, "native observed receipt required"));
            };
            if !examine(work, || record.sequence == 1)?
                || !examine(work, || observed.effect.provider == launch.dispatch.provider)?
                || !examine(work, || observed.effect.effect_id == launch.dispatch.effect_id)?
                || !examine(work, || observed.effect.attempt_id == launch.dispatch.attempt_id)?
                || !examine(work, || observed.effect.request_digest == launch.dispatch.request_digest)?
                || !examine(work, || observed.effect.guarantee == launch.dispatch.guarantee)?
                || !examine(work, || observed.diagnostic.is_none() || observed.effect.status == EffectStatus::Indeterminate)?
                || !examine(work, || !matches!(observed.effect.status, EffectStatus::Planned | EffectStatus::Dispatched))? {
                return Err(boundary(crate::wire::ErrorCode::Storage, "native observation identity is invalid"));
            }
            drop(record);
            Some(*observed)
        }
        None => None,
    };
    check(work)?;
    if bounds.tail != 1 + u64::from(observed.is_some()) || records.next(&mut observer).await?.is_some() {
        return Err(boundary(crate::wire::ErrorCode::Storage, "native receipt sequence is invalid"));
    }
    check(work)?;
    Ok(Some((*launch, observed, custody)))
}

/// Authenticates one protected launch against the original qualified effect reader.
pub(super) async fn authenticate_core_launch<P: StreamProvider>(
    reader: &HistoryReader<P>,
    launch: &LaunchReceipt<'_>,
    authority: &NativeProcessAuthority,
    work: &NativeMaterializationWork,
) -> Result<()> {
    let io = super::super::PreparationIo::from_native_work(work)?;
    let cursor = reader.pin(0, Some(&mut &io)).await?;
    let state = reader.effect(&cursor, launch.dispatch.effect_id, authority.history_limits,
        Some(&mut &io)).await.map(|state| state.map(|(state, _)| state))?
        .ok_or(boundary(crate::wire::ErrorCode::Storage, "native receipt has no retained core effect"))?;
    if !examine(work, || state.request_digest == launch.dispatch.request_digest)?
        || !examine(work, || state.request == launch.plan.request)?
        || !examine(work, || state.provider == launch.plan.provider)?
        || !examine(work, || state.effect_kind == launch.plan.effect_kind)?
        || !examine(work, || state.guarantee == launch.plan.guarantee)?
        || !examine(work, || state.result_schema == launch.plan.result_schema)?
        || !examine(work, || state.attempts.last() == Some(&launch.dispatch.attempt_id))? {
        return Err(boundary(crate::wire::ErrorCode::Storage, "native receipt differs from its retained core effect"));
    }
    drop(state);
    drop(cursor);
    drop(io);
    check(work)
}

impl<P: StreamProvider> NativeProcessProvider<P> {
    pub(super) async fn request(
        &self, work: &NativeMaterializationWork,
    ) -> Result<(Cow<'_, NativeProcessRequest>, Option<NativeAllocation>)> {
        match &self.invocation {
            Some(invocation) => Ok((Cow::Borrowed(&invocation.request), None)),
            None => {
                self.owner.require_volume_grant_with_native_work(self.plan.request.volume(),
                    crate::conversation::VolumeOperation::Read, work)?;
                let io = crate::filesystem::PreparationIo::from_native_work(work)?;
                let bytes = self.authority.content.read(&self.plan.request, self.owner.input_limits().file_bytes, Some(&io))?.await?;
                let (request, custody) = crate::filesystem::native_decoded(io.decode(
                    std::marker::PhantomData::<NativeProcessRequest>, &bytes, crate::wire::ErrorCode::Storage)?)?;
                drop(bytes);
                Ok((Cow::Owned(request), Some(custody)))
            }
        }
    }

    pub(super) async fn approved_receipt(
        &self, attempt: crate::EffectAttemptId, request: &NativeProcessRequest,
        work: &NativeMaterializationWork,
    ) -> Result<NativeProcessReceipt> {
        let digest = request.approval_digest_with_native_work(
            self.owner.task_binding().0, self.command, work,
        )?;
        self.authority.approvals.verify_with_native_work(self.approval, self.command, digest, work)?.await?;
        self.receipt(attempt, work, Some(digest)).await?
            .ok_or(Error::Indeterminate(self.command))
    }

    /// Reopens only an original protected receipt.
    pub async fn recover(
        owner: Arc<TaskJournalOwner<P>>, command: OperationId, plan: &TaskEffectPlan,
        approval: InteractionId, authority: NativeProcessAuthority,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        crate::NativeFutureBundle::new(work, || async move {
            let (outcome, outcome_custody) = owner.outcome_with_native_work(work).await?;
            if outcome.is_none() { owner.verify_with_native_work(true, work).await?; }
            owner.require_volume_grant_with_native_work(plan.request.volume(),
                crate::conversation::VolumeOperation::Read, work)?;
            owner.require_volume_grant_with_native_work(plan.result_schema.volume(),
                crate::conversation::VolumeOperation::Read, work)?;
            if authority.history_limits.maximum_events == 0 || authority.history_limits.maximum_bytes == 0 {
                return Err(boundary(crate::wire::ErrorCode::Storage, "native effect history allowances must be finite and positive"));
            }
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            let plan = finite_binding::copy_plan_with_io(plan, &io)?;
            let provider = finite_binding::assemble(io, owner, command, plan, None, approval, authority, work)?;
            let (after, after_custody) = provider.owner.outcome_with_native_work(work).await?;
            if after.is_none() { provider.owner.verify_with_native_work(true, work).await?; }
            drop(after); drop(after_custody);
            check(work)?;
            drop(outcome); drop(outcome_custody);
            Ok(provider)
        })?.await
    }
    /// Observes the protected attempt, optionally checking its approved request digest.
    pub async fn receipt(
        &self, attempt: crate::EffectAttemptId, work: &NativeMaterializationWork,
        expected_approval_digest: Option<[u8; 32]>,
    ) -> Result<Option<NativeProcessReceipt>> {
        crate::NativeFutureBundle::new(work, || async move {
            let outcome_custody = self.owner.outcome_with_native_work(work).await?;
            if outcome_custody.0.is_none() { self.owner.verify_with_native_work(true, work).await?; }
            self.owner.require_volume_grant_with_native_work(self.plan.request.volume(), crate::conversation::VolumeOperation::Read, work)?;
            let path = receipt_path(attempt, work)?;
            let Some((launch, observed, custody)) = read(&self.owner, &path, work).await? else { return Ok(None); };
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
                || !examine(work, || self.invocation.as_ref().is_none_or(|invocation|
                    launch.approval_digest == invocation.approval_digest))? {
                return Err(boundary(crate::wire::ErrorCode::Storage, "native receipt belongs to a different admission"));
            }
            if let Some(expected) = expected_approval_digest
                && !examine(work, || launch.approval_digest == expected)?
            {
                return Err(boundary(crate::wire::ErrorCode::Storage, "native receipt differs from its original approved request digest"));
            }
            authenticate_core_launch(&self.history, &launch, &self.authority, work).await?;
            {

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
                diagnostic, publication_failure, invocation_failure, published, work: completed_work, _custody: custody,
            };
            let retained = add(
                add(result.original_fence.reservation_id.capacity() as u64,
                    result.original_fence.placement.capacity() as u64)?,
                observation_parts_bytes(&result.effect, result.diagnostic.as_ref(),
                    result.publication_failure.as_ref(), result.invocation_failure.as_ref(), &result.published)?,
            )?;
            result._custody.resize_retained(retained)?;
            drop(path); drop(outcome_custody);
            Ok(Some(result))
        })?.await
    }
}
