//! Conversation-owned effect admission, dispatch and reconciliation.

use crate::contract::capability;
use crate::{
    EffectId, Error, IdempotencyKey, OperationId, Result,
    conversation::ContentResidencyVerifier,
    core::{
        Action, Authority, AuthorityIssuer, Command, EffectAttestation, EffectState, EffectStatus,
        SchemaRegistry, Scope,
    },
    effects::EffectRegistry,
    runtime::DurableEffectObserver,
    store::{
        DEFAULT_TERMINAL_EFFECTS, HistoryReadLimits, StreamAggregate,
        default_projection_read_limits,
    },
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::{StreamProvider};
use std::sync::Arc;

/// Immutable provider selection and ref-only request for one task command.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskEffectPlan {
    /// Exact registered provider identity.
    pub provider: String,
    /// Pinned delivery strength supported by that provider.
    pub guarantee: crate::core::EffectGuarantee,
    /// Versioned provider operation kind.
    pub effect_kind: String,
    /// Immutable request, readable under the retained task grants.
    pub request: crate::conversation::FileRef,
    /// Immutable result schema, readable under the same grants.
    pub result_schema: crate::conversation::FileRef,
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for TaskEffectPlan {
    fn maximum_slot_bytes() -> u64 {
        crate::filesystem::preparation_slot_bytes::<Self>()
    }
}

/// Resolves the canonical effect identity for one original task command.
#[cfg(feature = "filesystem")]
pub fn task_effect_id(
    task: crate::TaskId, command: OperationId,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<EffectId> {
    let digest = crate::contract::canonical_json_digest_with_work(
        &("harness/v2/task-effect", task, command),
        acyclic_stream::MAX_RECORD_BYTES as u64, work)?;
    if let Some(io) = work.as_deref_mut() { io.admit_copy(16)?; }
    let mut identity = [0; 16];
    identity.copy_from_slice(&digest[..16]);
    if let Some(io) = work.as_deref_mut() { io.copy_bytes(16)?; }
    Ok(EffectId::from_bytes(identity))
}

pub(crate) fn effect_error(
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    code: crate::wire::ErrorCode, reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().and_then(|io| io.original_native_work()).is_some() {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    match code {
        crate::wire::ErrorCode::NotFound => Error::NotFound(reason.into()),
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        crate::wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        _ => Error::Conflict(reason.into()),
    }
}

fn missing_effect(work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Error {
    effect_error(work, crate::wire::ErrorCode::NotFound, "effect is not in its owning conversation")
}

#[cfg(feature = "filesystem")]
fn validate_task_plan<P: StreamProvider>(
    owner: &crate::durable_host::TaskJournalOwner<P>, plan: &TaskEffectPlan, planning: bool,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
        owner.require_effect_grants(&plan.provider, planning, &mut Some(&mut crate::store::history::native_work_io(native)?))?;
        owner.validate_input_file(&plan.request, Some(&crate::filesystem::PreparationIo::from_native_work(native)?))?;
        return owner.validate_input_file(&plan.result_schema, Some(&crate::filesystem::PreparationIo::from_native_work(native)?));
    }
    owner.require_effect_grants(&plan.provider, planning, &mut None)?;
    owner.validate_input_file(&plan.request, None)?;
    owner.validate_input_file(&plan.result_schema, None)
}

#[cfg(feature = "filesystem")]
async fn verify_owner<P: StreamProvider>(
    owner: &crate::durable_host::TaskJournalOwner<P>, settlement: bool,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
        return owner.verify(settlement, &mut Some(&mut crate::store::history::native_work_io(native)?)).await;
    }
    owner.verify(settlement, &mut None).await
}

fn status_clone_bytes(status: &EffectStatus) -> Result<u64> {
    match status {
        EffectStatus::Succeeded { result } => result.logical_clone_owned_bytes(),
        EffectStatus::Failed { message } => Ok(message.len() as u64),
        _ => Ok(0),
    }
}

fn status_owned_bytes(status: &EffectStatus) -> Result<u64> {
    match status {
        EffectStatus::Succeeded { result } => result.logical_owned_bytes(),
        EffectStatus::Failed { message } => Ok(message.capacity() as u64),
        _ => Ok(0),
    }
}

fn clone_status(
    status: &EffectStatus, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<EffectStatus> {
    match status {
        EffectStatus::Succeeded { result } => Ok(EffectStatus::Succeeded {
            result: result.copy(work)?,
        }),
        EffectStatus::Failed { message } => Ok(EffectStatus::Failed {
            message: match work.as_deref_mut() {
                Some(io) => io.clone_text(message)?,
                None => message.clone(),
            },
        }),
        _ => Ok(status.clone()),
    }
}

fn attestation_owned_bytes(value: &EffectAttestation) -> Result<u64> {
    (value.provider.capacity() as u64).checked_add(status_owned_bytes(&value.status)?)
        .ok_or_else(|| Error::Invalid("effect attestation ownership overflows".into()))
}

#[cfg(feature = "filesystem")]
fn action_owned_bytes(action: &Action) -> Result<u64> {
    match action {
        Action::PlanEffect { provider, effect_kind, request, result_schema, .. } => {
            (provider.capacity() as u64).checked_add(effect_kind.capacity() as u64)
                .and_then(|bytes| bytes.checked_add(request.logical_owned_bytes().ok()?))
                .and_then(|bytes| bytes.checked_add(result_schema.logical_owned_bytes().ok()?))
                .ok_or_else(|| Error::Invalid("effect action ownership overflows".into()))
        },
        Action::MarkEffectDispatched { .. } => Ok(0),
        Action::ResolveEffect { observation } => attestation_owned_bytes(observation),
        _ => Err(Error::Invalid("effect host action is not an effect action".into())),
    }
}

#[cfg(feature = "filesystem")]
enum TaskEffectAction<'a> {
    Plan(&'a TaskEffectPlan),
    Dispatch(crate::EffectAttemptId),
}

#[cfg(feature = "filesystem")]
fn prepare_action_with_work(
    effect_id: EffectId, action: &TaskEffectAction<'_>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Action> {
    Ok(match action {
        TaskEffectAction::Plan(plan) => {
            let provider = match work.as_deref_mut() {
                Some(io) => io.clone_text(&plan.provider)?, None => plan.provider.clone(),
            };
            let effect_kind = match work.as_deref_mut() {
                Some(io) => io.clone_text(&plan.effect_kind)?, None => plan.effect_kind.clone(),
            };
            Action::PlanEffect {
                effect_id, provider, guarantee: plan.guarantee, effect_kind,
                request: plan.request.copy(work)?,
                result_schema: plan.result_schema.copy(work)?,
            }
        },
        TaskEffectAction::Dispatch(attempt_id) => Action::MarkEffectDispatched {
            effect_id, attempt_id: *attempt_id,
        },
    })
}

fn effect_key<T: std::fmt::Display>(
    prefix: &str, identity: T, phase: Option<&str>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(IdempotencyKey, u64)> {
    use std::fmt::Write as _;
    let capacity = prefix.len() + 36 + phase.map_or(0, |phase| phase.len() + 1);
    let mut text = String::new();
    if let Some(io) = work.as_deref_mut() {
        io.admit_copy(capacity as u64)?;
        io.reserve_text(&mut text, capacity)?;
    } else {
        text.try_reserve_exact(capacity)
            .map_err(|_| Error::Invalid("effect identity allocation failed".into()))?;
    }
    write!(&mut text, "{prefix}{identity}")
        .map_err(|_| effect_error(work, crate::wire::ErrorCode::Conflict, "effect identity formatting failed"))?;
    if let Some(phase) = phase {
        text.push(':');
        text.push_str(phase);
    }
    if let Some(io) = work.as_deref_mut() { io.copy_bytes(text.len() as u64)?; }
    let retained = text.capacity() as u64;
    IdempotencyKey::new(text).map(|key| (key, retained))
}

enum EffectStateRef<'a> {
    Resident(&'a EffectState),
    Archived {
        effect: EffectState,
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        _custody: Option<acyclic_fs::NativeAllocation>,
    },
}

impl std::ops::Deref for EffectStateRef<'_> {
    type Target = EffectState;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Resident(effect) => effect,
            Self::Archived { effect, .. } => effect,
        }
    }
}

/// Resolves a pinned provider attempt into its owning conversation history.
pub struct ConversationEffectHost<P> {
    stream: std::sync::Arc<P>,
    authority: Authority,
    issuer: AuthorityIssuer,
    scope: Scope,
    schemas: SchemaRegistry,
    content: Arc<dyn ContentResidencyVerifier>,
    providers: EffectRegistry,
    effect_history_limits: HistoryReadLimits,
    terminal_effect_limit: usize,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_metadata: Option<acyclic_fs::NativeAllocation>,
}

impl<P: StreamProvider> ConversationEffectHost<P> {
    async fn effect_for_host<'a>(
        &self, aggregate: &'a StreamAggregate<P>, effect_id: EffectId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<EffectStateRef<'a>> {
        if let Some(effect) = aggregate.reducer().effect(effect_id) {
            return Ok(EffectStateRef::Resident(effect));
        }
        let (effect, retained) = aggregate.effect(
            effect_id, self.effect_history_limits, work,
        ).await?.ok_or_else(|| missing_effect(work))?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let custody = match work.as_deref_mut() {
            Some(io) if io.original_native_work().is_some() => Some(io.take_native_custody(retained)?),
            _ => None,
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = retained;
        Ok(EffectStateRef::Archived {
            effect,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _custody: custody,
        })
    }

    /// Binds one conversation, its signed effect-run scope and provider catalog.
    #[allow(clippy::too_many_arguments, reason = "conversation binding and its original custody")]
    pub fn new(
        stream: std::sync::Arc<P>,
        authority: Authority,
        issuer: AuthorityIssuer,
        scope: Scope,
        schemas: SchemaRegistry,
        content: Arc<dyn ContentResidencyVerifier>,
        providers: EffectRegistry,
        custody: crate::NativeCustody,
        work: crate::NativeWork<'_>,
    ) -> Result<Self> {
        #[allow(unused_mut)]
        let mut host = Self {
            stream,
            authority,
            issuer,
            scope,
            schemas,
            content,
            providers,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_metadata: custody,
            effect_history_limits: default_projection_read_limits(),
            terminal_effect_limit: DEFAULT_TERMINAL_EFFECTS,
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            host.require_native_metadata(work)?;
            host.providers.require_native_metadata(work)?;
            let mut slot = work.claim(std::mem::size_of::<Self>() as u64, 0, acyclic_fs::WorkCounters::UNCHARGED)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            host.native_metadata.as_mut().expect("validated native host")
                .absorb(&mut slot)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            let mut io = crate::store::history::native_work_io(work)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            host.validate_binding(&mut Some(&mut io))?;
            return Ok(host);
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if host.native_metadata.is_some() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native effect metadata requires original work" });
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = custody;
        let _ = work;
        host.validate_binding(&mut None)?;
        Ok(host)
    }

    /// Configures bounded original-effect lookup for archived result/retry reads.
    pub fn with_effect_history_read_limits(mut self, limits: HistoryReadLimits) -> Result<Self> {
        if limits.maximum_events == 0 || limits.maximum_bytes == 0 {
            return Err(Error::Invalid(
                "effect history bounds must be positive".into(),
            ));
        }
        self.effect_history_limits = limits;
        Ok(self)
    }

    /// Configures only the resident success/failure cache; active uncertainty stays resident and every archived observation keeps its original proof.
    pub fn with_resident_terminal_effect_limit(mut self, maximum: usize) -> Result<Self> {
        if maximum == 0 {
            return Err(Error::Invalid(
                "terminal effect cache limit must be positive".into(),
            ));
        }
        self.terminal_effect_limit = maximum;
        Ok(self)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn require_native_metadata(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        if self.native_metadata.as_ref().is_some_and(|guard| guard.work().shares_ledger(work)) {
            Ok(())
        } else {
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "effect host does not retain the original native metadata",
            })
        }
    }

    fn validate_binding(&self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if self.authority.kind != crate::core::AggregateKind::Conversation {
            return Err(effect_error(work, crate::wire::ErrorCode::Invalid,
                "effect host requires a conversation aggregate"));
        }
        let verifier = self.issuer.verifier(work)?;
        verifier.verify_audience(&self.authority, work)?;
        verifier.verify(&self.scope, work)?;
        if !self.scope.capabilities().contains(capability::EFFECT_RUN) {
            return Err(effect_error(work, crate::wire::ErrorCode::Unauthorized,
                "effect host scope lacks effect:run"));
        }
        Ok(())
    }

    /// Clones the original metadata through its SDK source producers.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn clone_with_native_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Self> {
        use crate::store::history::HistoryIo as _;
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let authority = io.clone_authority(&self.authority)?;
        let issuer = self.issuer.copy(&mut Some(&mut io))?;
        let scope = self.scope.copy(&mut Some(&mut io))?;
        let schemas = self.schemas.copy(&mut Some(&mut io))?;
        let providers = self.providers.clone_with_native_work(work)?;
        let mut host = Self::new(
            self.stream.clone(), authority, issuer, scope, schemas,
            Arc::clone(&self.content), providers, Some(io.into_custody()), Some(work),
        )?;
        host.effect_history_limits = self.effect_history_limits;
        host.terminal_effect_limit = self.terminal_effect_limit;
        Ok(host)
    }

    /// Funds the SDK-owned Arc body before its one allocation attempt.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn into_arc_with_native_work(
        mut self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        use crate::store::history::HistoryIo as _;
        self.require_native_metadata(work)?;
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let bytes = std::mem::size_of::<Self>() as u64;
        io.admit_allocation(bytes)?;
        io.retain_owned(bytes)?;
        let mut custody = io.into_custody();
        self.native_metadata.as_mut().expect("validated native host")
            .absorb(&mut custody)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(Arc::new(self))
    }

    async fn aggregate(
        &self, stream: &std::sync::Arc<P>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(StreamAggregate<P>, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(original) = work.as_deref().and_then(|io| io.original_native_work()) {
            let mut io = crate::store::history::native_work_io(original)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            use crate::store::history::HistoryIo as _;
            let authority = io.clone_authority(&self.authority)?;
            let verifier = self.issuer.verifier(&mut Some(&mut io))?;
            let aggregate = StreamAggregate::open(
                stream, authority, verifier, self.schemas.clone(), None, default_projection_read_limits(), self.terminal_effect_limit, None, &mut Some(&mut io)).await?.with_content_verifier(Arc::clone(&self.content))
                .with_effect_history_read_limits(self.effect_history_limits)?;
            return Ok((aggregate, Some(io.into_custody())));
        }
        let authority = match work.as_deref_mut() {
            Some(io) => io.clone_authority(&self.authority)?, None => self.authority.clone(),
        };
        let verifier = self.issuer.verifier(work)?;
        let aggregate = StreamAggregate::open(
            stream, authority, verifier, self.schemas.clone(), None, default_projection_read_limits(), self.terminal_effect_limit, None, work).await?.with_content_verifier(Arc::clone(&self.content))
            .with_effect_history_read_limits(self.effect_history_limits)?;
        Ok((aggregate, None))
    }

    /// Runs one task command through the original conversation effect lifecycle.
    #[cfg(feature = "filesystem")]
    pub async fn run_task_effect(
        &self, owner: &crate::durable_host::TaskJournalOwner<P>,
        command_id: OperationId, plan: TaskEffectPlan,
    ) -> Result<EffectStatus> {
        self.run_task_effect_with_work(owner, command_id, &plan, &mut None).await
    }

    /// Runs that same lifecycle, retaining the actual returned status metadata.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn run_task_effect_with_native_work(
        &self, owner: &crate::durable_host::TaskJournalOwner<P>,
        command_id: OperationId, plan: &TaskEffectPlan,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(EffectStatus, acyclic_fs::NativeAllocation)> {
        self.require_native_metadata(work)?;
        self.providers.require_native_metadata(work)?;
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let status = self.run_task_effect_with_work(
            owner, command_id, plan, &mut Some(&mut io)).await?;
        use crate::store::history::HistoryIo as _;
        io.return_output(status_clone_bytes(&status)?, 1)?;
        let custody = io.take_native_custody(status_owned_bytes(&status)?)?;
        Ok((status, custody))
    }

    #[cfg(feature = "filesystem")]
    async fn run_task_effect_with_work(
        &self, owner: &crate::durable_host::TaskJournalOwner<P>,
        command_id: OperationId, plan: &TaskEffectPlan,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<EffectStatus> {
        use crate::{EffectAttemptId, core::ApplyResult};
        validate_task_plan(owner, plan, true, work)?;
        let effect_id = task_effect_id(owner.task_binding().0, command_id, work)?;
        self.task_action(owner, effect_id, "plan", TaskEffectAction::Plan(plan), work).await?;
        let stream = owner.stream();
        let aggregate = self.aggregate(&stream, work).await?;
        let effect = self.effect_for_host(&aggregate.0, effect_id, work).await?;
        if !matches!(effect.status,
            EffectStatus::Planned | EffectStatus::Dispatched | EffectStatus::Indeterminate)
        {
            return clone_status(&effect.status, work);
        }
        self.providers.resolve(&effect, work)?;
        let dispatched = if effect.status == EffectStatus::Planned {
            matches!(self.task_action(owner, effect_id, "dispatch",
                TaskEffectAction::Dispatch(EffectAttemptId::from_bytes(effect_id.into_bytes())),
                work).await?, ApplyResult::Applied { .. })
        } else { false };
        drop(effect);
        drop(aggregate);
        let aggregate = self.aggregate(&stream, work).await?;
        let effect = self.effect_for_host(&aggregate.0, effect_id, work).await?;
        if !matches!(effect.status, EffectStatus::Dispatched | EffectStatus::Indeterminate) {
            verify_owner(owner, true, work).await?;
            return clone_status(&effect.status, work);
        }
        let attestation = if dispatched {
            verify_owner(owner, false, work).await?;
            Some(self.providers.dispatch_state_and_attest(
                &self.issuer, effect_id, &effect, work).await?)
        } else {
            self.providers.reconcile_and_attest(
                &self.issuer, effect_id, &effect, work).await?
        };
        match attestation {
            Some(attestation) => self.settle(
                effect_id, &effect, attestation, Some(owner), work).await,
            None => Ok(EffectStatus::Indeterminate),
        }
    }

    /// Observes the exact retained command, including after task cancellation.
    #[cfg(feature = "filesystem")]
    pub async fn reconcile_task_effect(
        &self, owner: &crate::durable_host::TaskJournalOwner<P>,
        command_id: OperationId, plan: &TaskEffectPlan,
    ) -> Result<EffectStatus> {
        self.reconcile_task_effect_with_work(owner, command_id, plan, &mut None).await
    }

    /// Reconciles without redispatching and retains actual returned metadata.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn reconcile_task_effect_with_native_work(
        &self, owner: &crate::durable_host::TaskJournalOwner<P>,
        command_id: OperationId, plan: &TaskEffectPlan,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(EffectStatus, acyclic_fs::NativeAllocation)> {
        self.require_native_metadata(work)?;
        self.providers.require_native_metadata(work)?;
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let status = self.reconcile_task_effect_with_work(
            owner, command_id, plan, &mut Some(&mut io)).await?;
        use crate::store::history::HistoryIo as _;
        io.return_output(status_clone_bytes(&status)?, 1)?;
        let custody = io.take_native_custody(status_owned_bytes(&status)?)?;
        Ok((status, custody))
    }

    #[cfg(feature = "filesystem")]
    async fn reconcile_task_effect_with_work(
        &self, owner: &crate::durable_host::TaskJournalOwner<P>,
        command_id: OperationId, plan: &TaskEffectPlan,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<EffectStatus> {
        verify_owner(owner, true, work).await?;
        validate_task_plan(owner, plan, false, work)?;
        let effect_id = task_effect_id(owner.task_binding().0, command_id, work)?;
        let stream = owner.stream();
        let aggregate = self.aggregate(&stream, work).await?;
        let effect = self.effect_for_host(&aggregate.0, effect_id, work).await?;
        if effect.provider != plan.provider || effect.guarantee != plan.guarantee
            || effect.effect_kind != plan.effect_kind || effect.request != plan.request
            || effect.result_schema != plan.result_schema
        {
            return Err(effect_error(work, crate::wire::ErrorCode::Conflict,
                "task effect differs from retained command"));
        }
        if !matches!(effect.status, EffectStatus::Dispatched | EffectStatus::Indeterminate) {
            return clone_status(&effect.status, work);
        }
        match self.providers.reconcile_and_attest(
            &self.issuer, effect_id, &effect, work).await?
        {
            Some(attestation) => self.settle(
                effect_id, &effect, attestation, Some(owner), work).await,
            None => Ok(EffectStatus::Indeterminate),
        }
    }

    #[cfg(feature = "filesystem")]
    async fn task_action(
        &self, owner: &crate::durable_host::TaskJournalOwner<P>,
        effect_id: EffectId, phase: &str, action: TaskEffectAction<'_>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::core::ApplyResult> {
        let digest = crate::contract::canonical_json_digest_with_work(
            &("task-effect", effect_id, phase), acyclic_stream::MAX_RECORD_BYTES as u64, work)?;
        if let Some(io) = work.as_deref_mut() { io.admit_copy(16)?; }
        let mut identity = [0; 16];
        identity.copy_from_slice(&digest[..16]);
        if let Some(io) = work.as_deref_mut() { io.copy_bytes(16)?; }
        let operation_id = OperationId::from_bytes(identity);
        loop {
            let stream = owner.stream();
            let mut aggregate = self.aggregate(&stream, work).await?;
            let expected_revision = aggregate.0.reducer().operation_revision(operation_id)
                .map_or(aggregate.0.reducer().revision(), |revision| revision - 1);
            let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
            let scope = self.scope.copy(custody.work())?;
            custody.adopt(scope.logical_owned_bytes()?)?;
            let copied_action = prepare_action_with_work(effect_id, &action, custody.work())?;
            custody.adopt(action_owned_bytes(&copied_action)?)?;
            let (key, key_bytes) = effect_key("task-effect:", effect_id, Some(phase), custody.work())?;
            custody.adopt(key_bytes)?;
            let command = Command {
                operation_id, idempotency_key: key, expected_revision,
                scope, causal_parent: None, action: copied_action,
            };
            match aggregate.0.execute(command, Some(owner), custody.work()).await {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                Err(error) if error.is_native_failure() => return Err(error),
                Err(error) if error.code() == crate::wire::ErrorCode::Conflict => {
                    if aggregate.0.reducer().operation_revision(operation_id).is_some() {
                        return Err(effect_error(custody.work(), crate::wire::ErrorCode::Conflict,
                            "task effect command changed after admission"));
                    }
                    verify_owner(owner, false, custody.work()).await?;
                }
                result => return result,
            }
        }
    }

    async fn settle(
        &self, effect_id: EffectId, effect: &EffectState, attestation: EffectAttestation,
        #[cfg(feature = "filesystem")] owner: Option<&crate::durable_host::TaskJournalOwner<P>>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<EffectStatus> {
        let mut retained = crate::wire_codec::CodecQuota::new(work, 0)?;
        let attestation = attestation;
        retained.adopt(attestation_owned_bytes(&attestation)?)?;
        let digest = crate::contract::canonical_json_digest_with_work(
            &(effect_id, &attestation), acyclic_stream::MAX_RECORD_BYTES as u64, retained.work())?;
        if let Some(io) = retained.work().as_deref_mut() { io.admit_copy(16)?; }
        let mut identity = [0; 16];
        identity.copy_from_slice(&digest[..16]);
        if let Some(io) = retained.work().as_deref_mut() { io.copy_bytes(16)?; }
        let operation_id = OperationId::from_bytes(identity);
        loop {
            #[cfg(feature = "filesystem")]
            if let Some(owner) = owner { verify_owner(owner, true, retained.work()).await?; }
            #[cfg(feature = "filesystem")]
            let stream = owner.map_or_else(|| self.stream.clone(),
                crate::durable_host::TaskJournalOwner::stream);
            #[cfg(not(feature = "filesystem"))]
            let stream = self.stream.clone();
            let mut aggregate = self.aggregate(&stream, retained.work()).await?;
            let current = self.effect_for_host(&aggregate.0, effect_id, retained.work()).await?;
            if current.status == attestation.status {
                return clone_status(&attestation.status, retained.work());
            }
            if current.attempts != effect.attempts
                || !matches!(current.status, EffectStatus::Dispatched | EffectStatus::Indeterminate)
            {
                return Err(effect_error(retained.work(), crate::wire::ErrorCode::Conflict,
                    "effect attempt changed during reconciliation"));
            }
            drop(current);
            let mut command_custody = crate::wire_codec::CodecQuota::new(retained.work(), 0)?;
            let scope = self.scope.copy(command_custody.work())?;
            command_custody.adopt(scope.logical_owned_bytes()?)?;
            let observation = attestation.copy(command_custody.work())?.0;
            command_custody.adopt(attestation_owned_bytes(&observation)?)?;
            let (key, key_bytes) = effect_key("effect-reconcile:", operation_id, None, command_custody.work())?;
            command_custody.adopt(key_bytes)?;
            let command = Command {
                operation_id, idempotency_key: key,
                expected_revision: aggregate.0.reducer().revision(),
                scope, causal_parent: None, action: Action::ResolveEffect { observation },
            };
            #[cfg(feature = "filesystem")]
            let result = match owner {
                Some(owner) => aggregate.0.execute(
                    command, Some(owner), command_custody.work()).await,
                None => {
                    if command_custody.work().is_some() {
                        return Err(effect_error(command_custody.work(),
                            crate::wire::ErrorCode::Unsupported,
                            "observed effect settlement requires its task owner"));
                    }
                    aggregate.0.execute(command, None, command_custody.work()).await
                },
            };
            #[cfg(not(feature = "filesystem"))]
            let result = {
                if command_custody.work().is_some() {
                    return Err(effect_error(command_custody.work(),
                        crate::wire::ErrorCode::Unsupported,
                        "native effect settlement requires filesystem task ownership"));
                }
                aggregate.0.execute(command, command_custody.work()).await
            };
            drop(command_custody);
            match result {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                Err(error) if error.is_native_failure() => return Err(error),
                Ok(_) => return clone_status(&attestation.status, retained.work()),
                Err(error) if error.code() == crate::wire::ErrorCode::Conflict => continue,
                Err(error) => return Err(error),
            }
        }
    }
}

impl<P: StreamProvider> DurableEffectObserver for ConversationEffectHost<P> {
    fn reconcile<'a>(&'a self, effect_id: EffectId) -> BoxFuture<'a, Result<EffectStatus>> {
        Box::pin(async move {
            let aggregate = self.aggregate(&self.stream, &mut None).await?;
            let effect = self.effect_for_host(&aggregate.0, effect_id, &mut None).await?;
            if !matches!(
                effect.status,
                EffectStatus::Dispatched | EffectStatus::Indeterminate
            ) {
                return clone_status(&effect.status, &mut None);
            }
            let Some(attestation) = self
                .providers
                .reconcile_and_attest(&self.issuer, effect_id, &effect, &mut None)
                .await?
            else {
                return Ok(EffectStatus::Indeterminate);
            };
            self.settle(
                effect_id,
                &effect,
                attestation,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await
        })
    }
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod native_metadata_controls {
    use super::*;
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    use acyclic_stream::MemoryStream;

    fn budget() -> WorkBudget {
        WorkBudget {
            bytes_copied: 1_048_576, bytes_encoded: 1_048_576,
            bytes_hashed: 1_048_576, items_examined: 1024,
            allocation_operations: 1024, peak_allocation_bytes: 1_048_576,
            ..WorkBudget::default()
        }
    }

    fn ordinary_host() -> Result<ConversationEffectHost<MemoryStream>> {
        let authority = Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: "native-effect-metadata-control".into(),
        };
        let issuer = AuthorityIssuer::new("original-effect-issuer", [41; 32], authority.clone());
        let scope = issuer.issue("original-effect-scope", &crate::Capabilities::new([capability::EFFECT_RUN]), Some(crate::AgentId::from_bytes([42; 16])), None, &mut None).expect("scope proof");
        let files = crate::filesystem::FilesystemHost::new(
            acyclic_fs::Fs::memory(),
            &crate::resources::ProviderRef::new("effect-metadata-control".into(), "filesystem".into(), "2".into(), &mut None)?, None,
        )?;
        let content = crate::filesystem::FilesystemContentVerifier::new(files, &issuer.verifier(&mut None).expect("verifier copy"), &scope, crate::conversation::Limits::default().file_bytes, None)?;
        ConversationEffectHost::new(
            Arc::new(MemoryStream::default()),
            authority, issuer, scope, SchemaRegistry::new(), content, EffectRegistry::new(None)?, None, None,
        )
    }

    #[test]
    fn native_host_metadata_survives_shared_clone_until_physical_drop() -> Result<()> {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("finite original metadata ledger");
        let baseline = work.remaining().expect("original allowance").peak_allocation_bytes;
        let ordinary = ordinary_host()?;
        let host = ordinary.clone_with_native_work(&work)?;
        assert_eq!(host.authority, ordinary.authority);
        assert_eq!(host.scope, ordinary.scope);
        assert!(host.providers.require_native_metadata(&work).is_ok());
        assert!(host.providers.clone().require_native_metadata(&work).is_err());
        let shared = host.into_arc_with_native_work(&work)?;
        let alias = Arc::clone(&shared);
        assert!(work.remaining().expect("retained host").peak_allocation_bytes < baseline);
        drop(shared);
        assert!(work.remaining().expect("retained alias").peak_allocation_bytes < baseline);
        drop(alias);
        assert_eq!(work.remaining().expect("dropped payload").peak_allocation_bytes, baseline);
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert_eq!(work.receipt().backend_write_operations, 0);
        Ok(())
    }

    #[test]
    fn native_catalog_denies_metadata_before_allocation() {
        let cancellation = CancellationToken::new();
        let mut denied = budget();
        denied.peak_allocation_bytes = NativeMaterializationWork::accounting_state_bytes()
            + std::mem::size_of::<EffectRegistry>() as u64 - 1;
        let work = NativeMaterializationWork::new(denied, &cancellation)
            .expect("only meter backing is admitted");
        let before = work.receipt().allocation_operations;
        assert!(matches!(EffectRegistry::new(Some(&work)), Err(Error::NativeWork { .. })));
        assert_eq!(work.receipt().allocation_operations, before);

        let work = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original cancellable ledger");
        cancellation.cancel();
        let before = work.receipt().allocation_operations;
        assert!(matches!(EffectRegistry::new(Some(&work)), Err(Error::NativeWork { .. })));
        assert_eq!(work.receipt().allocation_operations, before);
    }

    #[test]
    fn native_host_rejects_real_foreign_scope_and_cancelled_metadata_source() -> Result<()> {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original scope ledger");
        let baseline = work.remaining().expect("original allowance").peak_allocation_bytes;
        let mut host = ordinary_host()?;
        let foreign = AuthorityIssuer::new("foreign-effect-issuer", [43; 32], host.authority.clone());
        host.scope = foreign.issue("foreign-effect-scope", &crate::Capabilities::new([capability::EFFECT_RUN]), Some(crate::AgentId::from_bytes([42; 16])), None, &mut None).expect("scope proof");
        assert!(host.clone_with_native_work(&work).is_err());
        assert_eq!(work.remaining().expect("rejected clone dropped").peak_allocation_bytes, baseline);
        cancellation.cancel();
        let before = work.receipt().allocation_operations;
        assert!(matches!(host.clone_with_native_work(&work), Err(Error::NativeWork { .. })));
        assert_eq!(work.receipt().allocation_operations, before);
        Ok(())
    }

    #[test]
    fn native_host_refuses_a_foreign_arc_ledger_before_allocation() -> Result<()> {
        let cancellation = CancellationToken::new();
        let original = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original ledger");
        let foreign = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("different original ledger");
        let baseline = original.remaining().expect("original allowance").peak_allocation_bytes;
        let host = ordinary_host()?.clone_with_native_work(&original)?;
        let before = foreign.receipt().allocation_operations;
        assert!(matches!(host.into_arc_with_native_work(&foreign), Err(Error::NativeBoundary { .. })));
        assert_eq!(foreign.receipt().allocation_operations, before);
        assert_eq!(original.remaining().expect("rejected host dropped").peak_allocation_bytes, baseline);
        Ok(())
    }
}
