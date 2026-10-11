//! The original launch supervisor, with optional original native work custody.

use super::*;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use crate::contract::{JsonBounds, JsonCodecCharge};
use std::{future::Future, pin::Pin, task::{Context, Poll}};

type NativeResult<T> = std::result::Result<T, NativeReceiptError>;

// Serialize borrowed payloads. The protected wire contract remains Receipt;
// neither launch nor observed payload is cloned just to encode that contract.
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
    work: &'a WorkCounters,
}

pub(super) struct DispatchPayload {
    pub(super) dispatch: EffectDispatch,
    _custody: Option<NativeAllocation>,
}

pub(super) struct ObservedPayload {
    pub(super) observed: NativeObservation,
    pub(super) _custody: Option<NativeAllocation>,
}

struct SupervisorPayload {
    dispatch: EffectDispatch,
    path: StreamPath,
    _custody: Option<NativeAllocation>,
}

struct ExecutedPayload {
    dispatch: EffectDispatch,
    execution: NativeExecution,
    path: StreamPath,
    _custody: Option<NativeAllocation>,
}

// Capture this whole value, never independent Arc fields: the shared original
// guard must outlive every cached payload even if the registry/provider drops.
struct SupervisorGraph<P: StreamProvider> {
    owner: Arc<TaskJournalOwner<P>>,
    view: Arc<dyn NativeProcessView<P>>,
    authority: Arc<NativeProcessAuthority>,
    request: Arc<NativeProcessRequest>,
    schema: Arc<serde_json::Value>,
    supervisor: Arc<NativeSupervisorControl>,
    plan: Arc<TaskEffectPlan>,
    history: Arc<HistoryReader<P>>,
    _custody: Option<Arc<Mutex<NativeAllocation>>>,
}

pub(super) fn dispatch_payload(
    dispatch: EffectDispatch, work: Option<&NativeMaterializationWork>,
) -> NativeResult<DispatchPayload> {
    let custody = match work {
        Some(work) => {
            let bytes = checked_add(checked_add(dispatch.provider.capacity() as u64,
                dispatch.effect_kind.capacity() as u64)?, dispatch.request.logical_owned_bytes()?)?;
            Some(work.retain_owned(bytes)?)
        }
        None => None,
    };
    Ok(DispatchPayload { dispatch, _custody: custody })
}

pub(super) fn into_observed_effect(
    payload: ObservedPayload,
) -> NativeResult<(EffectObservation, Option<NativeAllocation>)> {
    let ObservedPayload { observed, mut _custody } = payload;
    let NativeObservation { effect, diagnostic, publication_failure, invocation_failure, work: _ } = observed;
    drop(diagnostic);
    drop(publication_failure);
    drop(invocation_failure);
    if let Some(custody) = _custody.as_mut() {
        custody.substitute(receipt_observation::effect_bytes(&effect)?)?;
    }
    Ok((effect, _custody))
}

fn check(work: Option<&NativeMaterializationWork>) -> NativeResult<()> {
    if let Some(work) = work { work.remaining()?; }
    Ok(())
}

pub(super) fn require_finite(work: &NativeMaterializationWork) -> Result<()> {
    let remaining = work.remaining()
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    if !remaining.has_finite_allowances() {
        return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "native dispatch/reconciliation requires finite original work",
        });
    }
    Ok(())
}

pub(super) fn root_error(error: NativeReceiptError) -> Error {
    match error {
        NativeReceiptError::Owner(source) => source,
        NativeReceiptError::Native(cause) => Error::NativeWork { cause, custody: None },
        NativeReceiptError::Record(reason) => Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage, reason,
        },
        NativeReceiptError::Backend(source) => Error::NativeStream {
            code: crate::wire::ErrorCode::Storage, source, custody: None,
        },
        NativeReceiptError::Supervisor(source) => Error::NativeSupervisor { source, custody: None },
        NativeReceiptError::Decode { source, custody } => Error::NativeDecode {
            code: crate::wire::ErrorCode::Storage, source, custody,
        },
        NativeReceiptError::Codec { code, source, custody } =>
            Error::NativeCodec { code, source, custody },
    }
}

fn examine(work: Option<&NativeMaterializationWork>, compare: impl FnOnce() -> bool) -> NativeResult<bool> {
    if let Some(work) = work {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
    }
    Ok(compare())
}

fn checked_add(left: u64, right: u64) -> NativeResult<u64> {
    left.checked_add(right).ok_or(NativeReceiptError::Record("native dispatch custody overflows"))
}

fn effect_result(receipt: NativeProcessReceipt, finite: bool)
    -> NativeResult<(EffectObservation, Option<NativeAllocation>)>
{
    if finite {
        receipt.into_effect_with_native_custody().map(|(effect, guard)| (effect, Some(guard)))
    } else { Ok((receipt.effect, None)) }
}

enum SupervisorFuture<'a, T> {
    Ordinary(BoxProviderFuture<'a, T>),
    Original(crate::NativeFutureBundle<'a, T>),
}

impl<T> Future for SupervisorFuture<'_, T> {
    type Output = Result<T>;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            Self::Ordinary(future) => future.as_mut().poll(context).map(Ok),
            Self::Original(future) => Pin::new(future).poll(context),
        }
    }
}

fn supervisor_future_original<'a, T: 'a, F, B>(build: B, work: Option<&NativeMaterializationWork>)
    -> NativeResult<SupervisorFuture<'a, T>>
where F: Future<Output = T> + Send + 'a, B: FnOnce() -> F + Send + 'a {
    match work {
        Some(work) => crate::NativeFutureBundle::new(work, || async move { Ok(build().await) })
            .map(SupervisorFuture::Original).map_err(NativeReceiptError::Owner),
        None => Ok(SupervisorFuture::Ordinary(Box::pin(build()))),
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
        && examine(work, || &observed.work == receipt.work)?)
}

fn require_same_launch(
    retained: &LaunchReceipt<'_>, requested: &LaunchReceipt<'_>,
    work: Option<&NativeMaterializationWork>,
) -> NativeResult<()> {
    // A legitimate recovery can hold a later fence. The original recorded
    // fence is evidence, never permission to redispatch under a new lease.
    if !examine(work, || retained.dispatch == requested.dispatch)?
        || !examine(work, || retained.plan == requested.plan)?
        || !examine(work, || retained.conversation == requested.conversation)?
        || !examine(work, || retained.task == requested.task)?
        || !examine(work, || retained.command == requested.command)?
        || !examine(work, || retained.approval == requested.approval)?
        || !examine(work, || retained.approval_digest == requested.approval_digest)? {
        return Err(NativeReceiptError::Record("native launch identity was reused"));
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
        // The cached reader is the original authenticated Core archive reader.
        // Its native adapter borrows the caller's ledger; no history reader or
        // fresh materialization allowance is constructed for dispatch.
        let mut io = work.map(crate::store::history::native_work_io).transpose()?;
        let cursor = self.history.pin_with_work(0,
            io.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo)).await?;
        let state = self.history.effect_with_work(&cursor, self.effect_id,
            self.authority.history_limits,
            io.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo)).await?
            .ok_or(NativeReceiptError::Record("native command has no committed effect admission"))?;
        // Compare borrowed fields rather than cloning EffectDispatch::from_state.
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
            return Err(NativeReceiptError::Record("native dispatch differs from retained command"));
        }
        drop(state);
        drop(cursor);
        drop(io);
        let view = self.view.as_ref().ok_or(NativeReceiptError::Record(
            "recovered native provider can only reconcile receipts"))?;
        let (request, _, approval_digest) = self.executable_admission()?;
        // These immutable request paths were checked at bind. Dispatch rechecks
        // the original selection without a second Windows case-folding copy.
        if !examine(work, || view.manifest() == &request.view)? {
            return Err(NativeReceiptError::Record("native dispatch view changed"));
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
                request.validate_native_paths(view.manifest())?;
                self.authority.approvals.verify(self.approval, self.command, approval_digest).await?;
                require_view_grants(&self.owner, &request.view)?;
                self.owner.verify(false).await?;
            }
        }
        check(work)
    }

    pub(super) async fn launch_original(
        &self, payload: DispatchPayload, work: Option<&NativeMaterializationWork>,
    ) -> NativeResult<(EffectObservation, Option<NativeAllocation>)> {
        let dispatch = &payload.dispatch;
        self.require_dispatch_original(dispatch, work).await?;
        if let Some(receipt) = self.receipt_original(dispatch.attempt_id, work).await? {
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
            let receipt = self.receipt_original(dispatch.attempt_id, work).await?
                .ok_or(NativeReceiptError::Record("native launch receipt changed"))?;
            return effect_result(receipt, work.is_some());
        }
        drop(launch);
        // A retained launch is never replayed. All subsequent admission or
        // supervisor errors leave the very same launch-only uncertain slot.
        let payload = SupervisorPayload {
            dispatch: payload.dispatch, path,
            _custody: payload._custody,
        };
        let owner = Arc::clone(&self.owner);
        let view = Arc::clone(self.view.as_ref().ok_or(NativeReceiptError::Record(
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
        let attempt = payload.dispatch.attempt_id;
        // Clone only the original shared ledger handle, retaining its alias in
        // the supervisor after either the caller or the provider is dropped.
        let original_work = work.cloned();
        let graph = SupervisorGraph {
            owner, view, authority, request, schema, supervisor, plan, history,
            _custody: self._custody.clone(),
        };
        let future = supervisor_future_original(|| async move {
            let graph = graph;
            let owner = &graph.owner;
            let view = &graph.view;
            let authority = &graph.authority;
            let request = &graph.request;
            let schema = &graph.schema;
            let supervisor = &graph.supervisor;
            let plan = &graph.plan;
            let history = &graph.history;
            let _completion = NativeSupervisorCompletion(Arc::clone(supervisor));
            let work = original_work.as_ref();
            // Capture the complete payload, not individual fields: on every
            // early return its original guards are dropped after its values.
            let mut payload = payload;
            let (execution, execution_custody) = execute(&owner, view.as_ref(), &authority,
                &request, &payload.dispatch, &schema, &supervisor, work)?.await?;
            if let Some(guard) = payload._custody.as_mut() {
                let mut execution_custody = execution_custody.ok_or(NativeReceiptError::Record(
                    "finite native execution lost original payload custody"))?;
                guard.absorb(&mut execution_custody)?;
            }
            let SupervisorPayload { dispatch, path, _custody } = payload;
            let payload = ExecutedPayload { dispatch, execution, path, _custody };
            let dispatch = &payload.dispatch;
            let execution = &payload.execution;
            let observed = ObservedWrite {
                effect: EffectWrite { provider: &dispatch.provider, effect_id: dispatch.effect_id,
                    attempt_id: dispatch.attempt_id, request_digest: dispatch.request_digest,
                    guarantee: dispatch.guarantee, status: &execution.status },
                diagnostic: &execution.diagnostic, publication_failure: &execution.publication_failure,
                invocation_failure: &execution.invocation_failure, work: &execution.work,
            };
            if !append_receipt_original(&owner, &payload.path, 1,
                &ReceiptWrite::Observed(&observed), JournalWrite::Settlement, work).await? {
                let retained = receipt_observation::read(&owner, &payload.path, work, command).await?
                    .ok_or(NativeReceiptError::Record("native observed receipt is missing"))?;
                let (task, fence) = owner.task_binding();
                let expected = LaunchReceipt {
                    dispatch: Cow::Borrowed(dispatch), plan: Cow::Borrowed(plan.as_ref()),
                    conversation: Cow::Borrowed(authority.verifier.audience()),
                    task, command, fence: Cow::Borrowed(fence), approval, approval_digest,
                };
                require_same_launch(&retained.launch, &expected, work)?;
                receipt_observation::authenticate_core_launch(history.as_ref(), &retained.launch,
                    &authority, work).await?;
                if !observed_matches(retained.observed.as_ref(), &observed, work)? {
                    return Err(NativeReceiptError::Record("native observed outcome was not retained"));
                }
            }
            // Recorded also requires physical quiescence. A durable uncertain
            // outcome must not erase the original unresolved process phase.
            let (_, phase) = supervisor.snapshot(attempt)?;
            if matches!(phase, NativeCapturePhase::Pending | NativeCapturePhase::Drained) {
                supervisor.record_capture(NativeCapturePhase::Recorded)
                    .map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
            }
            let ExecutedPayload { dispatch, execution, path, _custody } = payload;
            drop(path);
            let EffectDispatch { provider, effect_id, attempt_id, request_digest, guarantee,
                effect_kind, request } = dispatch;
            drop(effect_kind);
            drop(request);
            into_observed_effect(ObservedPayload {
                observed: NativeObservation {
                    effect: EffectObservation { provider, effect_id, attempt_id, request_digest,
                        guarantee, status: execution.status },
                    diagnostic: execution.diagnostic,
                    publication_failure: execution.publication_failure,
                    invocation_failure: execution.invocation_failure, work: execution.work,
                },
                _custody,
            })
        }, work)?;
        self.supervisor.begin(attempt)?;
        tokio::spawn(future).await
            .map_err(|source| NativeReceiptError::Supervisor(source))?
            .map_err(NativeReceiptError::Owner)?
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
            let codec_slot = codec_slot.ok_or(NativeReceiptError::Record(
                "native receipt codec slot is missing"))?;
            let remaining = work.remaining()?;
            let guard = custody.as_mut().ok_or(NativeReceiptError::Record("native receipt encode custody is missing"))?;
            crate::contract::canonical_json_bytes_bounded(receipt,
                JsonBounds { maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
                    maximum_items: remaining.items_examined, maximum_depth: 256 },
                &mut |charge| {
                    let result = match charge {
                        JsonCodecCharge::Work { bytes, items } => work.charge(WorkCounters {
                            bytes_encoded: bytes, items_examined: items, ..WorkCounters::default() }),
                        JsonCodecCharge::CopyAdmission { bytes } => work.admit(WorkCounters {
                            bytes_copied: bytes, ..WorkCounters::default() }),
                        JsonCodecCharge::Copy { bytes } => work.charge(WorkCounters {
                            bytes_copied: bytes, ..WorkCounters::default() }),
                        JsonCodecCharge::AllocationOperation { .. } => work.charge(WorkCounters {
                            allocation_operations: 1, ..WorkCounters::default() }),
                        JsonCodecCharge::Read { .. } => work.remaining().map(|_| ()),
                        JsonCodecCharge::Admission { maximum_owned_bytes } => work.admit_retained(maximum_owned_bytes),
                        JsonCodecCharge::Retain { bytes } => guard.bytes().checked_add(bytes)
                            .ok_or(acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow))
                            .and_then(|bytes| guard.resize_retained(bytes)),
                        JsonCodecCharge::Release { bytes } => guard.release(bytes),
                    };
                    result.map_err(|cause| Error::NativeWork { cause, custody: None })
                }).map_err(|source| codec_slot.retain(source, custody.take()))?
        }
        None => crate::contract::canonical_json_bytes(receipt)?,
    };
    if let Some(guard) = custody.as_mut() {
        // Encoding succeeded: no SDK diagnostic payload was created.
        guard.release(diagnostic_bytes)?;
    }
    if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(NativeReceiptError::Record("native receipt exceeds Stream limit"));
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
        // StreamPath aliases its intrinsically guarded original payload.
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
    result.map_err(NativeReceiptError::Owner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_fs::{CancellationToken, WorkBudget};
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn intrinsic_receipt_path_alias_retains_original_custody_through_cancellation() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let attempt = crate::EffectAttemptId::from_bytes([7; 16]);
        let before = work.live_allocation_bytes();
        let path = receipt_observation::receipt_path_with_native_work(attempt, &work).unwrap();
        assert_eq!(path.as_str(), receipt_path(attempt).unwrap().as_str());
        assert!(work.live_allocation_bytes() > before);
        let alias = path.clone();
        drop(path);
        let retained = work.live_allocation_bytes();
        let prefix = work.receipt();
        cancellation.cancel();
        assert_eq!(work.live_allocation_bytes(), retained);
        assert_eq!(work.receipt(), prefix);
        assert!(work.cancellation().is_cancelled());
        drop(alias);
        assert_eq!(work.live_allocation_bytes(), before);
    }

    #[test]
    fn native_receipt_path_denial_precedes_encoding_without_changing_ordinary_policy() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget {
            items_examined: 0, ..WorkBudget::UNBOUNDED
        }, &cancellation).unwrap();
        let attempt = crate::EffectAttemptId::from_bytes([7; 16]);
        let before = work.receipt();
        assert!(receipt_observation::receipt_path_with_native_work(attempt, &work).is_err());
        assert_eq!(work.receipt(), before);
        assert_eq!(receipt_path(attempt).unwrap().as_str(),
            "harness/v2/native-process/07070707-0707-0707-0707-070707070707");
    }

    #[test]
    fn finite_launch_frame_denial_precedes_construction_and_preserves_prefix() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget {
            allocation_operations: 1, ..WorkBudget::UNBOUNDED
        }, &cancellation).unwrap();
        let before = work.receipt();
        let constructed = AtomicBool::new(false);
        let denied = supervisor_future_original(|| {
            constructed.store(true, Ordering::SeqCst);
            std::future::ready(())
        }, Some(&work));
        assert!(matches!(denied, Err(NativeReceiptError::Owner(Error::NativeWork { .. }))));
        assert!(!constructed.load(Ordering::SeqCst));
        assert_eq!(work.receipt(), before);
        assert_eq!(work.receipt().authority_records_appended, 0);
        assert_eq!(work.receipt().backend_write_operations, 0);
    }

    #[test]
    fn original_cancellation_denies_supervisor_frame_without_resetting_work() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let before = work.receipt();
        cancellation.cancel();
        let constructed = AtomicBool::new(false);
        let denied = supervisor_future_original(|| {
            constructed.store(true, Ordering::SeqCst);
            std::future::ready(())
        }, Some(&work));
        assert!(matches!(denied, Err(NativeReceiptError::Owner(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
        }))));
        assert!(!constructed.load(Ordering::SeqCst));
        assert_eq!(work.receipt(), before);
        assert!(work.cancellation().is_cancelled());
    }

    fn receipt(work: &NativeMaterializationWork) -> NativeProcessReceipt {
        let effect = EffectObservation {
            provider: "actual-native-provider".into(),
            effect_id: crate::EffectId::from_bytes([1; 16]),
            attempt_id: crate::EffectAttemptId::from_bytes([2; 16]),
            request_digest: [3; 32], guarantee: EffectGuarantee::AtMostOnce,
            status: EffectStatus::Indeterminate,
        };
        let original_fence = crate::scheduler::LeaseFence {
            reservation_id: "original-reservation".into(), placement: "original-worker".into(),
        };
        let bytes = receipt_observation::effect_bytes(&effect).unwrap()
            + original_fence.reservation_id.capacity() as u64 + original_fence.placement.capacity() as u64;
        NativeProcessReceipt {
            task: crate::TaskId::from_bytes([4; 16]), command: OperationId::from_bytes([5; 16]),
            original_fence, effect, observed: true, diagnostic: None, publication_failure: None,
            invocation_failure: None, work: WorkCounters::default(),
            _custody: Some(work.retain_owned(bytes).unwrap()),
        }
    }

    fn borrowed(observed: &NativeObservation) -> ObservedWrite<'_> {
        ObservedWrite {
            effect: EffectWrite { provider: &observed.effect.provider,
                effect_id: observed.effect.effect_id, attempt_id: observed.effect.attempt_id,
                request_digest: observed.effect.request_digest, guarantee: observed.effect.guarantee,
                status: &observed.effect.status },
            diagnostic: &observed.diagnostic, publication_failure: &observed.publication_failure,
            invocation_failure: &observed.invocation_failure, work: &observed.work,
        }
    }

    #[test]
    fn borrowed_observed_encoder_preserves_the_original_protected_wire_record() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let receipt = receipt(&work);
        let expected = NativeObservation {
            effect: receipt.effect.clone(), diagnostic: None, publication_failure: None,
            invocation_failure: None, work: receipt.work,
        };
        let expected = borrowed(&expected);
        let bytes = crate::contract::canonical_json_bytes(&ReceiptWrite::Observed(&expected)).unwrap();
        let decoded: Receipt<'static> = crate::contract::json_from_slice(&bytes).unwrap();
        let Receipt::Observed(observed) = decoded else { panic!("original observed wire contract"); };
        assert!(observed_matches(Some(&observed), &expected, None).unwrap());
    }

    #[test]
    fn lost_ack_comparison_borrows_the_original_receipt_and_rejects_a_different_slot() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let receipt = receipt(&work);
        let mut observed = NativeObservation {
            effect: receipt.effect.clone(), diagnostic: None, publication_failure: None,
            invocation_failure: None, work: WorkCounters::default(),
        };
        let expected = NativeObservation {
            effect: receipt.effect.clone(), diagnostic: None, publication_failure: None,
            invocation_failure: None, work: receipt.work,
        };
        let expected = borrowed(&expected);
        let before = work.receipt();
        assert!(observed_matches(Some(&observed), &expected, Some(&work)).unwrap());
        assert_eq!(work.receipt().items_examined - before.items_examined, 5);
        assert_eq!(work.receipt().bytes_copied, before.bytes_copied);
        observed.effect.attempt_id = crate::EffectAttemptId::from_bytes([9; 16]);
        assert!(!observed_matches(Some(&observed), &expected, Some(&work)).unwrap());
        assert!(!observed_matches(None, &expected, Some(&work)).unwrap());
        assert_eq!(work.receipt().authority_records_appended, 0);
    }

    #[test]
    fn complete_effect_handoff_keeps_original_guard_after_original_cancellation() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let receipt = receipt(&work);
        let alias = work.retain_owned(0).unwrap();
        cancellation.cancel();
        let before = work.receipt();
        let (effect, guard) = receipt.into_effect_with_native_custody().unwrap();
        assert!(guard.shares_ledger(&alias));
        assert_eq!(guard.bytes(), receipt_observation::effect_bytes(&effect).unwrap());
        assert_eq!(work.receipt(), before);
        assert!(guard.work().cancellation().is_cancelled());
        drop(effect);
        drop(guard);
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
    }

    #[tokio::test]
    async fn original_supervisor_keeps_ledger_alias_when_caller_join_future_is_dropped() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let guard = work.retain_owned(7).unwrap();
        let original = work.clone();
        let control = Arc::new(NativeSupervisorControl::new());
        let attempt = crate::EffectAttemptId::from_bytes([7; 16]);
        control.begin(attempt).unwrap();
        let supervisor = Arc::clone(&control);
        let (resume, resumed) = tokio::sync::oneshot::channel();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let future = supervisor_future_original(|| async move {
            let _completion = NativeSupervisorCompletion(supervisor);
            resumed.await.unwrap();
            original.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() }).unwrap();
            finish.send(guard).unwrap();
        }, Some(&work)).unwrap();
        let caller = tokio::spawn(future);
        let before = work.receipt();
        drop(caller);
        drop(work);
        resume.send(()).unwrap();
        let retained = finished.await.unwrap();
        assert_eq!(retained.bytes(), 7);
        assert_eq!(retained.work().receipt().items_examined, before.items_examined + 1);
        cancellation.cancel();
        assert!(retained.work().cancellation().is_cancelled());
        // The notification comes from the original completion guard, not a new
        // supervisor or a reset invocation after dropping the caller handle.
        while !control.snapshot(attempt).unwrap().0 {
            tokio::task::yield_now().await;
        }
    }

    #[test]
    fn original_codec_diagnostic_moves_with_existing_custody_after_cancellation() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let slot = NativeReceiptCodecSlot::new(&work).unwrap();
        let guard = work.retain_owned(crate::contract::sdk_json_diagnostic_bytes()).unwrap();
        let alias = work.retain_owned(0).unwrap();
        let message = "original codec failure".to_owned();
        let pointer = message.as_ptr();
        let before = work.receipt();
        cancellation.cancel();
        let error = root_error(slot.retain(Error::Invalid(message), Some(guard)));
        let Error::NativeCodec { code, source, custody: Some(box_custody) } = error else {
            panic!("original prepaid diagnostic carrier");
        };
        let Error::Invalid(message) = source.source().unwrap().downcast_ref::<Error>().unwrap() else {
            panic!("the original dynamic cause must remain intact");
        };
        let cause = source.downcast_ref::<NativeReceiptCodecCause>().unwrap();
        let custody = cause._custody.as_ref().unwrap();
        assert_eq!(code, crate::wire::ErrorCode::Invalid);
        assert_eq!(message.as_ptr(), pointer);
        assert_eq!(message, "original codec failure");
        assert!(custody.bytes() >= message.capacity() as u64);
        assert!(custody.shares_ledger(&alias));
        assert!(box_custody.shares_ledger(&alias));
        assert_eq!(work.receipt(), before);
    }

    #[test]
    fn original_typed_decode_cause_keeps_code_and_admitted_guard_after_cancellation() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let slot = NativeReceiptCodecSlot::new(&work).unwrap();
        let bytes = crate::contract::audited_serde_diagnostic_bytes(1, "serde_json::Value".len() as u64).unwrap();
        let custody = work.retain_owned(bytes).unwrap();
        let alias = work.retain_owned(0).unwrap();
        let source = serde_json::from_slice::<serde_json::Value>(b"[").unwrap_err();
        let before = work.receipt();
        cancellation.cancel();
        let error = root_error(slot.retain(
            Error::NativeDecode { code: crate::wire::ErrorCode::Invalid, source, custody: None },
            Some(custody),
        ));
        let Error::NativeCodec { code, source, custody: Some(box_custody) } = error else {
            panic!("original prepaid typed cause and diagnostic custody");
        };
        let Error::NativeDecode { code: inner_code, source: decode, custody: None } =
            source.source().unwrap().downcast_ref::<Error>().unwrap() else {
                panic!("the original typed decode cause must remain intact");
            };
        assert_eq!(code, crate::wire::ErrorCode::Invalid);
        assert_eq!(*inner_code, crate::wire::ErrorCode::Invalid);
        assert!(decode.is_eof());
        let cause = source.downcast_ref::<NativeReceiptCodecCause>().unwrap();
        let custody = cause._custody.as_ref().unwrap();
        assert!(box_custody.shares_ledger(&alias));
        assert_eq!(custody.bytes(), bytes);
        assert!(custody.shares_ledger(&alias));
        assert_eq!(work.receipt(), before);
    }

    #[test]
    fn guarded_native_cause_keeps_all_prepaid_custody_without_new_claim() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let slot = NativeReceiptCodecSlot::new(&work).unwrap();
        let outer = work.retain_owned(8).unwrap();
        let message = "original cause".to_owned();
        let bytes = message.capacity() as u64;
        let pointer = message.as_ptr();
        let inner = work.retain_owned(bytes).unwrap();
        let before = work.receipt();
        cancellation.cancel();
        let error = root_error(slot.retain(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Allocation(message),
            custody: Some(inner),
        }, Some(outer)));
        let Error::NativeCodec { source, custody: Some(box_custody), .. } = error else {
            panic!("original prepaid native cause and custody");
        };
        let Error::NativeWork { cause, custody: Some(custody) } =
            source.source().unwrap().downcast_ref::<Error>().unwrap() else {
                panic!("the original guarded native cause must remain intact");
            };
        let envelope = source.downcast_ref::<NativeReceiptCodecCause>().unwrap();
        assert_eq!(envelope._custody.as_ref().unwrap().bytes(), 8);
        assert!(box_custody.work().shares_ledger(&work));
        let acyclic_fs::NativeTreeMaterializationError::Allocation(message) = cause else {
            panic!("original allocation cause");
        };
        assert_eq!(message.as_ptr(), pointer);
        assert_eq!(custody.bytes(), bytes);
        assert_eq!(work.receipt(), before);
        assert!(custody.work().cancellation().is_cancelled());
    }

    #[test]
    fn foreign_native_cause_keeps_its_actual_guard_after_codec_buffers_physically_drop() {
        let cancellation = CancellationToken::new();
        let original = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let foreign = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let slot = NativeReceiptCodecSlot::new(&original).unwrap();
        let buffer = vec![0u8; 8];
        let outer = original.retain_owned(buffer.capacity() as u64).unwrap();
        let message = "foreign original cause".to_owned();
        let bytes = message.capacity() as u64;
        let pointer = message.as_ptr();
        let inner = foreign.retain_owned(bytes).unwrap();
        let original_alias = original.retain_owned(0).unwrap();
        // This is the closed codec constructor's inventory: temporary SDK
        // buffers are gone, while the independently guarded cause still lives.
        drop(buffer);
        let error = root_error(slot.retain(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Allocation(message),
            custody: Some(inner),
        }, Some(outer)));
        let Error::NativeCodec { source, custody: Some(box_custody), .. } = &error else {
            panic!("original prepaid cause guard retained");
        };
        let Error::NativeWork { cause, custody: Some(custody) } =
            source.source().unwrap().downcast_ref::<Error>().unwrap() else {
                panic!("the original foreign guarded cause must remain intact");
            };
        let envelope = source.downcast_ref::<NativeReceiptCodecCause>().unwrap();
        assert_eq!(envelope._custody.as_ref().unwrap().bytes(), 8);
        assert!(box_custody.shares_ledger(&original_alias));
        let acyclic_fs::NativeTreeMaterializationError::Allocation(message) = cause else {
            panic!("original allocation cause");
        };
        assert_eq!(message.as_ptr(), pointer);
        assert_eq!(custody.bytes(), bytes);
        assert!(!custody.shares_ledger(&original_alias));
        assert_eq!(original.live_allocation_bytes(),
            NativeMaterializationWork::accounting_state_bytes() + 8 + box_custody.bytes());
        assert_eq!(foreign.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes() + bytes);
        drop(error);
        assert_eq!(original.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        assert_eq!(foreign.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
    }
}
