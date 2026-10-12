//! The original launch supervisor and native work ledger.

use super::*;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};

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

pub(super) fn require_finite(work: &NativeMaterializationWork) -> Result<()> {
    let remaining = work.remaining()?;
    if !remaining.has_finite_allowances() {
        return Err(boundary(crate::wire::ErrorCode::Invalid, "native dispatch/reconciliation requires finite original work"));
    }
    Ok(())
}


fn observed_matches(
    observed: Option<&NativeObservation>, receipt: &ObservedWrite<'_>,
    work: &NativeMaterializationWork,
) -> Result<bool> {
    let Some(observed) = observed else { return Ok(false); };
    if !(examine(work, || observed.effect.provider == receipt.effect.provider
            && observed.effect.effect_id == receipt.effect.effect_id
            && observed.effect.attempt_id == receipt.effect.attempt_id
            && observed.effect.request_digest == receipt.effect.request_digest
            && observed.effect.guarantee == receipt.effect.guarantee
            && &observed.effect.status == receipt.effect.status)?
        && examine(work, || &observed.diagnostic == receipt.diagnostic)?
        && examine(work, || &observed.publication_failure == receipt.publication_failure)?
        && examine(work, || &observed.invocation_failure == receipt.invocation_failure)?
        && examine(work, || &observed.work == receipt.work)?
        && examine(work, || observed.published.len() == receipt.published.len())?) { return Ok(false); }
    for (actual, expected) in observed.published.iter().zip(receipt.published) {
        if !examine(work, || actual == expected)? { return Ok(false); }
    }
    Ok(true)
}

fn require_same_launch(
    retained: &LaunchReceipt<'_>, requested: &LaunchReceipt<'_>,
    work: &NativeMaterializationWork,
) -> Result<()> {
    if !examine(work, || retained.dispatch == requested.dispatch)?
        || !examine(work, || retained.plan == requested.plan)?
        || !examine(work, || retained.conversation == requested.conversation)?
        || !examine(work, || retained.task == requested.task)?
        || !examine(work, || retained.command == requested.command)?
        || !examine(work, || retained.approval == requested.approval)?
        || !examine(work, || retained.approval_digest == requested.approval_digest)? {
        return Err(boundary(crate::wire::ErrorCode::Storage, "native launch identity was reused"));
    }
    Ok(())
}

impl<P: StreamProvider> NativeProcessProvider<P> {
    pub(super) async fn validate_dispatch(
        &self, dispatch: &EffectDispatch, work: &NativeMaterializationWork,
    ) -> Result<()> {
        check(work)?;
        self.owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
        let mut io = crate::store::history::native_work_io(work)?;
        let cursor = self.history.pin(0,
            Some(&mut io)).await?;
        let state = self.history.effect(&cursor, self.effect_id,
            self.authority.history_limits,
            Some(&mut io)).await.map(|state| state.map(|(state, _)| state))?
            .ok_or(boundary(crate::wire::ErrorCode::Storage, "native command has no committed effect admission"))?;
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
            return Err(boundary(crate::wire::ErrorCode::Storage, "native dispatch differs from retained command"));
        }
        drop(state);
        drop(cursor);
        drop(io);
        let Invocation { view, request, approval_digest, .. } = self.invocation()?;
        if !examine(work, || view.manifest() == &request.view)? {
            return Err(boundary(crate::wire::ErrorCode::Storage, "native dispatch view changed"));
        }
        view.operation_cancellation().check()
            .map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
        {
                self.authority.approvals.verify_with_native_work(
                    self.approval, self.command, *approval_digest, work)?.await?;
                for mapping in &request.view.volumes {
                    self.owner.require_volume_grant(&mapping.volume, crate::conversation::VolumeOperation::Read, &mut Some(&mut crate::store::history::native_work_io(work)?))?;
                    if mapping.writable {
                        self.owner.require_volume_grant(&mapping.volume, crate::conversation::VolumeOperation::Write, &mut Some(&mut crate::store::history::native_work_io(work)?))?;
                    }
                }
                self.owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
            }
        check(work)
    }

    pub(super) async fn launch(
        &self, payload: (EffectDispatch, NativeAllocation), work: &NativeMaterializationWork,
    ) -> Result<(EffectObservation, NativeAllocation)> {
        let dispatch = &payload.0;
        self.validate_dispatch(dispatch, work).await?;
        if let Some(receipt) = self.receipt(dispatch.attempt_id, work, None).await? {
            return receipt.into_effect();
        }
        let (task, fence) = self.owner.task_binding();
        let path = super::receipt_observation::receipt_path(
                dispatch.attempt_id, work)?;
        let launch = LaunchReceipt {
            dispatch: Cow::Borrowed(&dispatch), plan: Cow::Borrowed(&self.plan),
            conversation: Cow::Borrowed(self.authority.verifier.audience()), task,
            command: self.command, fence: Cow::Borrowed(fence), approval: self.approval,
            approval_digest: self.invocation()?.approval_digest,
        };
        if !append_receipt(&self.owner, &path, 0,
            &Receipt::<_, &ObservedWrite<'_>>::Launch(&launch), JournalWrite::Fresh, work).await? {
            let receipt = self.receipt(dispatch.attempt_id, work, None).await?
                .ok_or(boundary(crate::wire::ErrorCode::Storage, "native launch receipt changed"))?;
            return receipt.into_effect();
        }
        drop(launch);
        let (dispatch, custody) = payload;
        let owner = Arc::clone(&self.owner);

        let authority = Arc::clone(&self.authority);
        let invocation = self.invocation()?;
        let view = Arc::clone(&invocation.view);
        let request = Arc::clone(&invocation.request);
        let schema = Arc::clone(&invocation.result_schema);
        let approval_digest = invocation.approval_digest;
        let supervisor = Arc::clone(&self.supervisor);
        let command = self.command;
        let plan = Arc::clone(&self.plan);
        let history = Arc::clone(&self.history);
        let approval = self.approval;
        let attempt = dispatch.attempt_id;
        let original_work = work.clone();
        let provider_custody = self._custody.clone();
        let future = crate::NativeFutureBundle::new(work, || async move {
            let _provider_custody = provider_custody;
            let _completion = NativeSupervisorCompletion(Arc::clone(&supervisor));
            let work = &original_work;
            let ((status, diagnostic, published), mut execution_custody) = execute(&owner, view.as_ref(), &authority,
                &request, &dispatch, &schema, &supervisor, work).await?;
            let mut custody = custody;
            custody.absorb(&mut execution_custody)?;
            let completed_work = work.receipt();
            let observed = ObservedWrite {
                effect: EffectWrite { provider: &dispatch.provider, effect_id: dispatch.effect_id,
                    attempt_id: dispatch.attempt_id, request_digest: dispatch.request_digest,
                    guarantee: dispatch.guarantee, status: &status },
                diagnostic: &diagnostic, publication_failure: &None,
                invocation_failure: &None, work: &completed_work,
                published: &published,
            };
            if !append_receipt(&owner, &path, 1,
                &Receipt::<&LaunchReceipt<'_>, _>::Observed(&observed), JournalWrite::Settlement, work).await? {
                let (launch, observation, _custody) = receipt_observation::read(&owner, &path, work).await?
                    .ok_or(boundary(crate::wire::ErrorCode::Storage, "native observed receipt is missing"))?;
                let (task, fence) = owner.task_binding();
                let expected = LaunchReceipt {
                    dispatch: Cow::Borrowed(&dispatch), plan: Cow::Borrowed(plan.as_ref()),
                    conversation: Cow::Borrowed(authority.verifier.audience()),
                    task, command, fence: Cow::Borrowed(fence), approval, approval_digest,
                };
                require_same_launch(&launch, &expected, work)?;
                receipt_observation::authenticate_core_launch(history.as_ref(), &launch,
                    &authority, work).await?;
                if !observed_matches(observation.as_ref(), &observed, work)? {
                    return Err(boundary(crate::wire::ErrorCode::Storage, "native observed outcome was not retained"));
                }
            }
            supervisor.snapshot(attempt)?;
            drop(path);
            let EffectDispatch { provider, effect_id, attempt_id, request_digest, guarantee,
                effect_kind, request } = dispatch;
            drop(effect_kind);
            drop(request);
            let effect = EffectObservation { provider, effect_id, attempt_id, request_digest,
                guarantee, status };
            drop((diagnostic, published));
            custody.substitute(receipt_observation::effect_bytes(&effect)?)?;

            drop(execution_custody);
            Ok((effect, custody))
        })?;
        self.supervisor.begin(attempt)?;
        tokio::spawn(future).await
            .map_err(|source| Error::NativeSupervisor { source, custody: None })?
    }
}

async fn append_receipt<P: StreamProvider, L: Serialize, O: Serialize>(
    owner: &TaskJournalOwner<P>, path: &StreamPath, tail: u64,
    receipt: &Receipt<L, O>, write: JournalWrite,
    work: &NativeMaterializationWork,
) -> Result<bool> {
    let io = super::super::PreparationIo::from_native_work(work)?;
    let bytes = io.encode(receipt, acyclic_stream::MAX_RECORD_BYTES as u64, true)?;
    work.charge(WorkCounters { bytes_hashed: bytes.len() as u64, ..WorkCounters::default() })?;
    let (key_bytes, key_custody) = work.copy_bytes(blake3::hash(&bytes).as_bytes())?;
    let key_bytes = acyclic_stream::OwnedBytes::from_vec_with_guard(
        key_bytes, key_custody, work.runtime_meter(),
    ).map_err(|failure| super::super::native_execution::native_owned_bytes_failure(failure, work))?;
    let key = acyclic_stream::IdempotencyKey::new(key_bytes)?;
    owner.append(path.clone(), tail, &key, bytes, write,
        &mut Some(&mut crate::store::history::native_work_io(work)?)).await
}
