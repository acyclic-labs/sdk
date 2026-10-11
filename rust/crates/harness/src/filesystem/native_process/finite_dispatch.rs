//! The original launch supervisor, with optional original native work custody.

use super::*;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use crate::contract::JsonBounds;
use std::future::Future;

type NativeResult<T> = std::result::Result<T, Error>;

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ReceiptWrite<'a, 'b> {
    Launch(&'a LaunchReceipt<'b>),
    Observed(&'a ObservedWrite<'a>),
}

#[derive(Serialize)]
struct EffectWrite<'a> {
    provider: &'a str,
    effect_id: crate::EffectId,
    attempt_id: crate::EffectAttemptId,
    request_digest: [u8; 32],
    guarantee: EffectGuarantee,
    status: &'a EffectStatus,
}

#[derive(Serialize)]
struct ObservedWrite<'a> {
    effect: EffectWrite<'a>,
    diagnostic: &'a Option<crate::conversation::FileRef>,
    publication_failure: &'a Option<NativePublicationFailure>,
    invocation_failure: &'a Option<String>,
    published: &'a [NativeProcessPublishedVolume],
    work: &'a WorkCounters,
}

pub(super) fn dispatch_payload(
    dispatch: EffectDispatch, work: Option<&NativeMaterializationWork>,
) -> NativeResult<(EffectDispatch, Option<NativeAllocation>)> {
    let custody = match work {
        Some(work) => {
            let bytes = checked_add(checked_add(dispatch.provider.capacity() as u64,
                dispatch.effect_kind.capacity() as u64)?, dispatch.request.logical_owned_bytes()?)?;
            Some(work.retain_owned(bytes)?)
        }
        None => None,
    };
    Ok((dispatch, custody))
}

pub(super) fn into_observed_effect(
    observed: NativeObservation, mut custody: Option<NativeAllocation>,
) -> NativeResult<(EffectObservation, Option<NativeAllocation>)> {
    let NativeObservation {
        effect, diagnostic, publication_failure, invocation_failure, published, work: _,
    } = observed;
    drop(diagnostic);
    drop(publication_failure);
    drop(invocation_failure);
    drop(published);
    if let Some(custody) = custody.as_mut() {
        custody.substitute(receipt_observation::effect_bytes(&effect)?)?;
    }
    Ok((effect, custody))
}

fn check(work: Option<&NativeMaterializationWork>) -> NativeResult<()> {
    if let Some(work) = work { work.remaining()?; }
    Ok(())
}

pub(super) fn require_finite(work: &NativeMaterializationWork) -> Result<()> {
    let remaining = work.remaining()
        .map_err(Error::from)?;
    if !remaining.has_finite_allowances() {
        return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "native dispatch/reconciliation requires finite original work",
        });
    }
    Ok(())
}

fn examine(work: Option<&NativeMaterializationWork>, compare: impl FnOnce() -> bool) -> NativeResult<bool> {
    if let Some(work) = work {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
    }
    Ok(compare())
}

fn checked_add(left: u64, right: u64) -> NativeResult<u64> {
    left.checked_add(right).ok_or(receipt_error("native dispatch custody overflows"))
}

fn effect_result(receipt: NativeProcessReceipt, finite: bool)
    -> NativeResult<(EffectObservation, Option<NativeAllocation>)>
{
    if finite {
        receipt.into_effect().map(|(effect, guard)| (effect, Some(guard)))
    } else { Ok((receipt.effect, None)) }
}

type SupervisorFuture<'a, T> = futures::future::Either<
    BoxProviderFuture<'a, Result<T>>, crate::NativeFutureBundle<'a, T>>;

fn supervisor_future_original<'a, T: 'a, F, B>(build: B, work: Option<&NativeMaterializationWork>)
    -> NativeResult<SupervisorFuture<'a, T>>
where F: Future<Output = T> + Send + 'a, B: FnOnce() -> F + Send + 'a {
    match work {
        Some(work) => crate::NativeFutureBundle::new(work, || async move { Ok(build().await) })
            .map(futures::future::Either::Right),
        None => Ok(futures::future::Either::Left(Box::pin(async move { Ok(build().await) }))),
    }
}

fn observed_matches(
    observed: Option<&NativeObservation>, receipt: &ObservedWrite<'_>,
    work: Option<&NativeMaterializationWork>,
) -> NativeResult<bool> {
    let Some(observed) = observed else { return Ok(false); };
    Ok(examine(work, || observed.effect.provider == receipt.effect.provider
            && observed.effect.effect_id == receipt.effect.effect_id
            && observed.effect.attempt_id == receipt.effect.attempt_id
            && observed.effect.request_digest == receipt.effect.request_digest
            && observed.effect.guarantee == receipt.effect.guarantee
            && &observed.effect.status == receipt.effect.status)?
        && examine(work, || &observed.diagnostic == receipt.diagnostic)?
        && examine(work, || &observed.publication_failure == receipt.publication_failure)?
        && examine(work, || &observed.invocation_failure == receipt.invocation_failure)?
        && examine(work, || &observed.work == receipt.work)?
        && published_matches(&observed.published, receipt.published, work)?)
}

fn published_matches(
    observed: &[NativeProcessPublishedVolume], expected: &[NativeProcessPublishedVolume],
    work: Option<&NativeMaterializationWork>,
) -> NativeResult<bool> {
    if !examine(work, || observed.len() == expected.len())? { return Ok(false); }
    for (actual, expected) in observed.iter().zip(expected) {
        if !examine(work, || actual == expected)? { return Ok(false); }
    }
    Ok(true)
}

fn require_same_launch(
    retained: &LaunchReceipt<'_>, requested: &LaunchReceipt<'_>,
    work: Option<&NativeMaterializationWork>,
) -> NativeResult<()> {
    if !examine(work, || retained.dispatch == requested.dispatch)?
        || !examine(work, || retained.plan == requested.plan)?
        || !examine(work, || retained.conversation == requested.conversation)?
        || !examine(work, || retained.task == requested.task)?
        || !examine(work, || retained.command == requested.command)?
        || !examine(work, || retained.approval == requested.approval)?
        || !examine(work, || retained.approval_digest == requested.approval_digest)? {
        return Err(receipt_error("native launch identity was reused"));
    }
    Ok(())
}

impl<P: StreamProvider + 'static> NativeProcessProvider<P> {
    pub(super) async fn require_dispatch_original(
        &self, dispatch: &EffectDispatch, work: Option<&NativeMaterializationWork>,
    ) -> NativeResult<()> {
        check(work)?;
        match work {
            Some(work) => self.owner.verify_with_native_work(false, work).await?,
            None => { self.owner.verify(false).await?; }
        }
        let mut io = work.map(crate::store::history::native_work_io).transpose()?;
        let cursor = self.history.pin_with_work(0,
            io.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo)).await?;
        let state = self.history.effect_with_work(&cursor, self.effect_id,
            self.authority.history_limits,
            io.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo)).await?
            .ok_or(receipt_error("native command has no committed effect admission"))?;
        if !examine(work, || state.status == EffectStatus::Dispatched)?
            || !examine(work, || state.attempts.last() == Some(&dispatch.attempt_id))?
            || !examine(work, || dispatch.effect_id == self.effect_id)?
            || !examine(work, || state.request_digest == dispatch.request_digest)?
            || !examine(work, || state.provider == dispatch.provider)?
            || !examine(work, || state.effect_kind == dispatch.effect_kind)?
            || !examine(work, || state.request == dispatch.request)?
            || !examine(work, || state.guarantee == dispatch.guarantee)?
            || !examine(work, || state.provider == self.plan.provider)?
            || !examine(work, || state.effect_kind == self.plan.effect_kind)?
            || !examine(work, || state.guarantee == self.plan.guarantee)?
            || !examine(work, || state.request == self.plan.request)?
            || !examine(work, || state.result_schema == self.plan.result_schema)?
        {
            return Err(receipt_error("native dispatch differs from retained command"));
        }
        drop(state);
        drop(cursor);
        drop(io);
        let view = self.view.as_ref().ok_or(receipt_error(
            "recovered native provider can only reconcile receipts"))?;
        let (request, _, approval_digest) = self.executable_admission()?;
        if !examine(work, || view.manifest() == &request.view)? {
            return Err(receipt_error("native dispatch view changed"));
        }
        view.operation_cancellation().check()
            .map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
        match work {
            Some(work) => {
                self.authority.approvals.verify_with_native_work(
                    self.approval, self.command, approval_digest, work)?.await?;
                for mapping in &request.view.volumes {
                    self.owner.require_volume_grant_with_native_work(&mapping.volume,
                        crate::conversation::VolumeOperation::Read, work)?;
                    if mapping.writable {
                        self.owner.require_volume_grant_with_native_work(&mapping.volume,
                            crate::conversation::VolumeOperation::Write, work)?;
                    }
                }
                self.owner.verify_with_native_work(false, work).await?;
            }
            None => {
                request.validate_native_paths(view.manifest(), None)?;
                self.authority.approvals.verify(self.approval, self.command, approval_digest).await?;
                require_view_grants(&self.owner, &request.view)?;
                self.owner.verify(false).await?;
            }
        }
        check(work)
    }

    pub(super) async fn launch_original(
        &self, payload: (EffectDispatch, Option<NativeAllocation>), work: Option<&NativeMaterializationWork>,
    ) -> NativeResult<(EffectObservation, Option<NativeAllocation>)> {
        let dispatch = &payload.0;
        self.require_dispatch_original(dispatch, work).await?;
        if let Some(receipt) = self.receipt_original(dispatch.attempt_id, work, None).await? {
            return effect_result(receipt, work.is_some());
        }
        let (task, fence) = self.owner.task_binding();
        let path = match work {
            Some(work) => super::receipt_observation::receipt_path_with_native_work(
                dispatch.attempt_id, work)?,
            None => receipt_path(dispatch.attempt_id)?,
        };
        let launch = LaunchReceipt {
            dispatch: Cow::Borrowed(&dispatch), plan: Cow::Borrowed(&self.plan),
            conversation: Cow::Borrowed(self.authority.verifier.audience()), task,
            command: self.command, fence: Cow::Borrowed(fence), approval: self.approval,
            approval_digest: self.executable_admission()?.2,
        };
        if !append_receipt_original(&self.owner, &path, 0,
            &ReceiptWrite::Launch(&launch), JournalWrite::Fresh, work).await? {
            let receipt = self.receipt_original(dispatch.attempt_id, work, None).await?
                .ok_or(receipt_error("native launch receipt changed"))?;
            return effect_result(receipt, work.is_some());
        }
        drop(launch);
        let (dispatch, custody) = payload;
        let owner = Arc::clone(&self.owner);
        let view = Arc::clone(self.view.as_ref().ok_or(receipt_error(
            "native dispatch requires a prepared view"))?);
        let authority = Arc::clone(&self.authority);
        let (request, schema, approval_digest) = self.executable_admission()?;
        let request = Arc::clone(request);
        let schema = Arc::clone(schema);
        let supervisor = Arc::clone(&self.supervisor);
        let command = self.command;
        let plan = Arc::clone(&self.plan);
        let history = Arc::clone(&self.history);
        let approval = self.approval;
        let attempt = dispatch.attempt_id;
        let original_work = work.cloned();
        let provider_custody = self._custody.clone();
        let future = supervisor_future_original(|| async move {
            let _provider_custody = provider_custody;
            let _completion = NativeSupervisorCompletion(Arc::clone(&supervisor));
            let work = original_work.as_ref();
            let (execution, mut execution_custody) = execute(&owner, view.as_ref(), &authority,
                &request, &dispatch, &schema, &supervisor, work)?.await?;
            let mut custody = custody;
            if let Some(guard) = custody.as_mut() {
                guard.absorb(execution_custody.as_mut().ok_or(receipt_error(
                    "finite native execution lost original payload custody"))?)?;
            }
            let observed = ObservedWrite {
                effect: EffectWrite { provider: &dispatch.provider, effect_id: dispatch.effect_id,
                    attempt_id: dispatch.attempt_id, request_digest: dispatch.request_digest,
                    guarantee: dispatch.guarantee, status: &execution.status },
                diagnostic: &execution.diagnostic, publication_failure: &execution.publication_failure,
                invocation_failure: &execution.invocation_failure, work: &execution.work,
                published: &execution.published,
            };
            if !append_receipt_original(&owner, &path, 1,
                &ReceiptWrite::Observed(&observed), JournalWrite::Settlement, work).await? {
                let retained = receipt_observation::read(&owner, &path, work, command).await?
                    .ok_or(receipt_error("native observed receipt is missing"))?;
                let (task, fence) = owner.task_binding();
                let expected = LaunchReceipt {
                    dispatch: Cow::Borrowed(&dispatch), plan: Cow::Borrowed(plan.as_ref()),
                    conversation: Cow::Borrowed(authority.verifier.audience()),
                    task, command, fence: Cow::Borrowed(fence), approval, approval_digest,
                };
                require_same_launch(&retained.launch, &expected, work)?;
                receipt_observation::authenticate_core_launch(history.as_ref(), &retained.launch,
                    &authority, work).await?;
                if !observed_matches(retained.observed.as_ref(), &observed, work)? {
                    return Err(receipt_error("native observed outcome was not retained"));
                }
            }
            let (_, phase) = supervisor.snapshot(attempt)?;
            if matches!(phase, NativeCapturePhase::Pending | NativeCapturePhase::Drained) {
                supervisor.record_capture(NativeCapturePhase::Recorded)
                    .map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
            }
            drop(path);
            let EffectDispatch { provider, effect_id, attempt_id, request_digest, guarantee,
                effect_kind, request } = dispatch;
            drop(effect_kind);
            drop(request);
            let result = into_observed_effect(NativeObservation {
                    effect: EffectObservation { provider, effect_id, attempt_id, request_digest,
                        guarantee, status: execution.status },
                    diagnostic: execution.diagnostic,
                    publication_failure: execution.publication_failure,
                    invocation_failure: execution.invocation_failure, work: execution.work,
                    published: execution.published,
                }, custody);
            drop(execution_custody);
            result
        }, work)?;
        self.supervisor.begin(attempt)?;
        tokio::spawn(future).await
            .map_err(|source| Error::NativeSupervisor { source, custody: None })?
            ?
    }
}

async fn append_receipt_original<P: StreamProvider>(
    owner: &TaskJournalOwner<P>, path: &StreamPath, tail: u64,
    receipt: &ReceiptWrite<'_, '_>, write: JournalWrite,
    work: Option<&NativeMaterializationWork>,
) -> NativeResult<bool> {
    let codec_slot = work.map(NativeReceiptCodecSlot::new).transpose()?;
    let diagnostic_bytes = work.map_or(0, |_| crate::contract::sdk_json_diagnostic_bytes());
    let mut custody = work.map(|work| work.retain_owned(diagnostic_bytes)).transpose()?;
    let bytes = match work {
        Some(work) => {
            let codec_slot = codec_slot.ok_or(receipt_error(
                "native receipt codec slot is missing"))?;
            let remaining = work.remaining()?;
            let guard = custody.as_mut().ok_or(receipt_error("native receipt encode custody is missing"))?;
            crate::contract::canonical_json_bytes_bounded(receipt,
                JsonBounds { maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
                    maximum_items: remaining.items_examined, maximum_depth: 256 },
                &mut |charge| {
                    crate::filesystem::native_execution::native_codec_charge_typed(charge, work, guard)
                        .map_err(Error::from)
                }).map_err(|source| codec_slot.retain(source, custody.take()))?
        }
        None => crate::contract::canonical_json_bytes(receipt)?,
    };
    if let Some(guard) = custody.as_mut() {
        guard.release(diagnostic_bytes)?;
    }
    if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(receipt_error("native receipt exceeds Stream limit"));
    }
    let mut key_custody = None;
    if let Some(work) = work {
        work.charge(WorkCounters { bytes_hashed: bytes.len() as u64, ..WorkCounters::default() })?;
    }
    let hash = blake3::hash(&bytes);
    let mut key_bytes = Vec::new();
    if let Some(work) = work {
        work.admit(WorkCounters { bytes_copied: 32, ..WorkCounters::default() })?;
        key_custody = Some(work.claim(32)?);
    }
    key_bytes.try_reserve_exact(32)
        .map_err(acyclic_fs::NativeTreeMaterializationError::AllocationFailed)?;
    if let Some(work) = work {
        work.charge(WorkCounters { bytes_copied: 32, ..WorkCounters::default() })?;
    }
    key_bytes.extend_from_slice(hash.as_bytes());
    let key = acyclic_stream::IdempotencyKey::new(Bytes::from(key_bytes))?;
    let target = match work {
        Some(_) => path.clone(),
        None => {
            let mut target = String::new();
            target.try_reserve_exact(path.as_str().len())
                .map_err(acyclic_fs::NativeTreeMaterializationError::AllocationFailed)?;
            target.push_str(path.as_str());
            StreamPath::new(target)?
        }
    };
    let bytes = Bytes::from(bytes);
    let result = match work {
        Some(work) => owner.append_with_native_work(target, tail, &key, bytes.clone(), write, work).await,
        None => owner.append(target, tail, &key, bytes.clone(), write).await,
    };
    drop(bytes);
    drop(key);
    drop(key_custody);
    drop(custody);
    result
}

