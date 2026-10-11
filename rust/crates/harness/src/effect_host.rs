//! Conversation-owned effect admission, dispatch and reconciliation.
//!
//! Native companions borrow the original ledger through admission, history,
//! provider work and settlement. Returned status metadata owns its custody;
//! reconciliation never turns an uncertain durable attempt into another dispatch.

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
use acyclic_stream::{StreamClient, StreamProvider};
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

/// Resolves the canonical effect identity for one original task command.
/// This identity alone grants no dispatch or observation authority.
#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for TaskEffectPlan {
    fn maximum_slot_bytes() -> u64 {
        std::mem::size_of::<Self>().max(std::mem::size_of::<serde_json::Value>()) as u64
    }
}

#[cfg(feature = "filesystem")]
pub fn task_effect_id(task: crate::TaskId, command: OperationId) -> Result<EffectId> {
    task_effect_id_with_work(task, command, &mut None)
}

#[cfg(feature = "filesystem")]
fn task_effect_id_with_work(
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

/// Derives the original canonical task effect identity on the caller's ledger.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn task_effect_id_with_native_work(
    task: crate::TaskId, command: OperationId,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<EffectId> {
    let mut io = crate::store::history::native_work_io(work)
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    task_effect_id_with_work(task, command, &mut Some(&mut io))
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
        owner.require_effect_grants_with_native_work(&plan.provider, planning, native)?;
        owner.validate_input_file_with_native_work(&plan.request, native)?;
        return owner.validate_input_file_with_native_work(&plan.result_schema, native);
    }
    owner.require_effect_grants(&plan.provider, planning)?;
    owner.validate_input_file(&plan.request)?;
    owner.validate_input_file(&plan.result_schema)
}

#[cfg(feature = "filesystem")]
async fn verify_owner<P: StreamProvider>(
    owner: &crate::durable_host::TaskJournalOwner<P>, settlement: bool,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
        return owner.verify_with_native_work(settlement, native).await;
    }
    owner.verify(settlement).await
}

/// Uses the canonical physical field-copy engine on the original observer.
pub(crate) fn clone_file_with_work(
    file: &crate::conversation::FileRef,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<crate::conversation::FileRef> {
    file.clone_with_work(work)
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

fn clone_status_with_work(
    status: &EffectStatus, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<EffectStatus> {
    match status {
        EffectStatus::Succeeded { result } => Ok(EffectStatus::Succeeded {
            result: clone_file_with_work(result, work)?,
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

fn clone_attestation_with_work(
    value: &EffectAttestation, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<EffectAttestation> {
    value.clone_with_work(work)
}

fn clone_scope_with_work(
    scope: &Scope, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Scope> {
    scope.clone_with_work(work)
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
                request: clone_file_with_work(&plan.request, work)?,
                result_schema: clone_file_with_work(&plan.result_schema, work)?,
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

/// Payload fields precede their actual original-ledger lifetime guard.
struct ObservedEffectAggregate<P> {
    aggregate: StreamAggregate<P>,
    history_limits: HistoryReadLimits,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _custody: Option<acyclic_fs::NativeAllocation>,
}

impl<P> std::ops::Deref for ObservedEffectAggregate<P> {
    type Target = StreamAggregate<P>;
    fn deref(&self) -> &Self::Target { &self.aggregate }
}

impl<P> std::ops::DerefMut for ObservedEffectAggregate<P> {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.aggregate }
}

struct ObservedArchivedEffect {
    effect: EffectState,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _custody: Option<acyclic_fs::NativeAllocation>,
}

enum EffectStateRef<'a> {
    Resident(&'a EffectState),
    Archived(ObservedArchivedEffect),
}

impl std::ops::Deref for EffectStateRef<'_> {
    type Target = EffectState;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Resident(effect) => effect,
            Self::Archived(retained) => &retained.effect,
        }
    }
}

impl<P: StreamProvider> ObservedEffectAggregate<P> {
    async fn effect_for_host(
        &self, effect_id: EffectId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<EffectStateRef<'_>> {
        if let Some(effect) = self.reducer().effect(effect_id) {
            return Ok(EffectStateRef::Resident(effect));
        }
        let (effect, retained) = self.aggregate.effect_with_work(
            effect_id, self.history_limits, work,
        ).await?.ok_or_else(|| missing_effect(work))?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let custody = match work.as_deref_mut() {
            Some(io) if io.original_native_work().is_some() => Some(io.take_native_custody(retained)?),
            _ => None,
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = retained;
        Ok(EffectStateRef::Archived(ObservedArchivedEffect {
            effect,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _custody: custody,
        }))
    }
}

/// Resolves a pinned provider attempt into its owning conversation history.
/// Callers must separately authenticate which tasks may ask about that history.
pub struct ConversationEffectHost<P> {
    stream: StreamClient<P>,
    authority: Authority,
    issuer: AuthorityIssuer,
    scope: Scope,
    schemas: SchemaRegistry,
    content: Arc<dyn ContentResidencyVerifier>,
    providers: EffectRegistry,
    effect_history_limits: HistoryReadLimits,
    terminal_effect_limit: usize,
}

impl<P: StreamProvider> ConversationEffectHost<P> {
    /// Binds one conversation, its signed effect-run scope and provider catalog.
    pub fn new(
        stream: StreamClient<P>,
        authority: Authority,
        issuer: AuthorityIssuer,
        scope: Scope,
        schemas: SchemaRegistry,
        content: Arc<dyn ContentResidencyVerifier>,
        providers: EffectRegistry,
    ) -> Result<Self> {
        if authority.kind != crate::core::AggregateKind::Conversation {
            return Err(Error::Invalid(
                "effect host requires a conversation aggregate".into(),
            ));
        }
        let verifier = issuer.verifier();
        verifier.verify_audience(&authority)?;
        verifier.verify(&scope)?;
        if !scope.capabilities().contains(capability::EFFECT_RUN) {
            return Err(Error::Unauthorized(
                "effect host scope lacks effect:run".into(),
            ));
        }
        Ok(Self {
            stream,
            authority,
            issuer,
            scope,
            schemas,
            content,
            providers,
            effect_history_limits: default_projection_read_limits(),
            terminal_effect_limit: DEFAULT_TERMINAL_EFFECTS,
        })
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

    /// Configures only the resident success/failure cache; active uncertainty
    /// stays resident and every archived observation keeps its original proof.
    pub fn with_resident_terminal_effect_limit(mut self, maximum: usize) -> Result<Self> {
        if maximum == 0 {
            return Err(Error::Invalid(
                "terminal effect cache limit must be positive".into(),
            ));
        }
        self.terminal_effect_limit = maximum;
        Ok(self)
    }

    async fn aggregate(&self) -> Result<ObservedEffectAggregate<P>> {
        self.aggregate_on_with_work(&self.stream, &mut None).await
    }

    async fn aggregate_on_with_work(
        &self, stream: &StreamClient<P>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<ObservedEffectAggregate<P>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(original) = work.as_deref().and_then(|io| io.original_native_work()) {
            let mut io = crate::store::history::native_work_io(original)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            use crate::store::history::HistoryIo as _;
            let authority = io.clone_authority(&self.authority)?;
            let verifier = self.issuer.verifier_with_work(&mut Some(&mut io))?;
            let aggregate = StreamAggregate::open_inner(
                stream, authority, verifier, self.schemas.clone(), None,
                default_projection_read_limits(), self.terminal_effect_limit, None,
                &mut Some(&mut io),
            ).await?.with_content_verifier(Arc::clone(&self.content))
                .with_effect_history_read_limits(self.effect_history_limits)?;
            return Ok(ObservedEffectAggregate {
                aggregate, history_limits: self.effect_history_limits,
                _custody: Some(io.into_custody()),
            });
        }
        let authority = match work.as_deref_mut() {
            Some(io) => io.clone_authority(&self.authority)?, None => self.authority.clone(),
        };
        let verifier = self.issuer.verifier_with_work(work)?;
        let aggregate = StreamAggregate::open_inner(
            stream, authority, verifier, self.schemas.clone(), None,
            default_projection_read_limits(), self.terminal_effect_limit, None, work,
        ).await?.with_content_verifier(Arc::clone(&self.content))
            .with_effect_history_read_limits(self.effect_history_limits)?;
        Ok(ObservedEffectAggregate {
            aggregate,
            history_limits: self.effect_history_limits,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _custody: None,
        })
    }

    /// Checks the original provider's native support before allocating a transport future.
    /// This pure preflight does not replace the owner and signed-scope authorization.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn require_native_task_effect_plan(
        &self, plan: &TaskEffectPlan, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.require_native_metadata(work)?;
        self.providers.resolve_fields_with_native_work(
            &plan.provider, &plan.effect_kind, plan.guarantee, work)?;
        Ok(())
    }

    /// Runs one task command through the original conversation effect lifecycle.
    /// Only the durable dispatch winner invokes the provider; later calls reconcile.
    #[cfg(feature = "filesystem")]
    pub async fn run_task_effect(
        &self, owner: &crate::durable_host::TaskJournalOwner<P>,
        command_id: OperationId, plan: TaskEffectPlan,
    ) -> Result<EffectStatus> {
        self.run_task_effect_with_work(owner, command_id, &plan, &mut None).await
    }

    /// Runs that same lifecycle, retaining the actual returned status metadata.
    /// The caller retains the immutable plan and its custody through this await.
    /// The original command engine copies its fields only when preparing the command.
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
        let effect_id = task_effect_id_with_work(owner.task_binding().0, command_id, work)?;
        self.task_action(owner, effect_id, "plan", TaskEffectAction::Plan(plan), work).await?;
        let stream = owner.stream();
        let aggregate = self.aggregate_on_with_work(&stream, work).await?;
        let effect = aggregate.effect_for_host(effect_id, work).await?;
        if !matches!(effect.status,
            EffectStatus::Planned | EffectStatus::Dispatched | EffectStatus::Indeterminate)
        {
            return clone_status_with_work(&effect.status, work);
        }
        self.providers.resolve_with_work(&effect, work)?;
        let dispatched = if effect.status == EffectStatus::Planned {
            matches!(self.task_action(owner, effect_id, "dispatch",
                TaskEffectAction::Dispatch(EffectAttemptId::from_bytes(effect_id.into_bytes())),
                work).await?, ApplyResult::Applied { .. })
        } else { false };
        drop(effect);
        drop(aggregate);
        let aggregate = self.aggregate_on_with_work(&stream, work).await?;
        let effect = aggregate.effect_for_host(effect_id, work).await?;
        if !matches!(effect.status, EffectStatus::Dispatched | EffectStatus::Indeterminate) {
            verify_owner(owner, true, work).await?;
            return clone_status_with_work(&effect.status, work);
        }
        let attestation = if dispatched {
            verify_owner(owner, false, work).await?;
            Some(self.providers.dispatch_state_and_attest_with_work(
                &self.issuer, effect_id, &effect, work).await?)
        } else {
            self.providers.reconcile_and_attest_with_work(
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
        let effect_id = task_effect_id_with_work(owner.task_binding().0, command_id, work)?;
        let stream = owner.stream();
        let aggregate = self.aggregate_on_with_work(&stream, work).await?;
        let effect = aggregate.effect_for_host(effect_id, work).await?;
        if effect.provider != plan.provider || effect.guarantee != plan.guarantee
            || effect.effect_kind != plan.effect_kind || effect.request != plan.request
            || effect.result_schema != plan.result_schema
        {
            return Err(effect_error(work, crate::wire::ErrorCode::Conflict,
                "task effect differs from retained command"));
        }
        if !matches!(effect.status, EffectStatus::Dispatched | EffectStatus::Indeterminate) {
            return clone_status_with_work(&effect.status, work);
        }
        match self.providers.reconcile_and_attest_with_work(
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
            let mut aggregate = self.aggregate_on_with_work(&stream, work).await?;
            let expected_revision = aggregate.reducer().operation_revision(operation_id)
                .map_or(aggregate.reducer().revision(), |revision| revision - 1);
            let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
            let scope = clone_scope_with_work(&self.scope, custody.work())?;
            custody.adopt(scope.logical_owned_bytes()?)?;
            let copied_action = prepare_action_with_work(effect_id, &action, custody.work())?;
            custody.adopt(action_owned_bytes(&copied_action)?)?;
            let (key, key_bytes) = effect_key("task-effect:", effect_id, Some(phase), custody.work())?;
            custody.adopt(key_bytes)?;
            let command = Command {
                operation_id, idempotency_key: key, expected_revision,
                scope, causal_parent: None, action: copied_action,
            };
            match aggregate.execute_task_command_with_work(command, owner, custody.work()).await {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                Err(error) if error.is_native_failure() => return Err(error),
                Err(error) if error.code() == crate::wire::ErrorCode::Conflict => {
                    if aggregate.reducer().operation_revision(operation_id).is_some() {
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
            let mut aggregate = self.aggregate_on_with_work(&stream, retained.work()).await?;
            let current = aggregate.effect_for_host(effect_id, retained.work()).await?;
            if current.status == attestation.status {
                return clone_status_with_work(&attestation.status, retained.work());
            }
            if current.attempts != effect.attempts
                || !matches!(current.status, EffectStatus::Dispatched | EffectStatus::Indeterminate)
            {
                return Err(effect_error(retained.work(), crate::wire::ErrorCode::Conflict,
                    "effect attempt changed during reconciliation"));
            }
            drop(current);
            let mut command_custody = crate::wire_codec::CodecQuota::new(retained.work(), 0)?;
            let scope = clone_scope_with_work(&self.scope, command_custody.work())?;
            command_custody.adopt(scope.logical_owned_bytes()?)?;
            let observation = clone_attestation_with_work(&attestation, command_custody.work())?;
            command_custody.adopt(attestation_owned_bytes(&observation)?)?;
            let (key, key_bytes) = effect_key("effect-reconcile:", operation_id, None, command_custody.work())?;
            command_custody.adopt(key_bytes)?;
            let command = Command {
                operation_id, idempotency_key: key,
                expected_revision: aggregate.reducer().revision(),
                scope, causal_parent: None, action: Action::ResolveEffect { observation },
            };
            #[cfg(feature = "filesystem")]
            let result = match owner {
                Some(owner) => aggregate.execute_task_command_with_work(
                    command, owner, command_custody.work()).await,
                None => {
                    if command_custody.work().is_some() {
                        return Err(effect_error(command_custody.work(),
                            crate::wire::ErrorCode::Unsupported,
                            "observed effect settlement requires its task owner"));
                    }
                    aggregate.execute(command).await
                },
            };
            #[cfg(not(feature = "filesystem"))]
            let result = {
                if command_custody.work().is_some() {
                    return Err(effect_error(command_custody.work(),
                        crate::wire::ErrorCode::Unsupported,
                        "native effect settlement requires filesystem task ownership"));
                }
                aggregate.execute(command).await
            };
            drop(command_custody);
            match result {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                Err(error) if error.is_native_failure() => return Err(error),
                Ok(_) => return clone_status_with_work(&attestation.status, retained.work()),
                Err(error) if error.code() == crate::wire::ErrorCode::Conflict => continue,
                Err(error) => return Err(error),
            }
        }
    }
}

impl<P: StreamProvider> DurableEffectObserver for ConversationEffectHost<P> {
    fn reconcile<'a>(&'a self, effect_id: EffectId) -> BoxFuture<'a, Result<EffectStatus>> {
        Box::pin(async move {
            let aggregate = self.aggregate().await?;
            let effect = aggregate.effect_for_host(effect_id, &mut None).await?;
            if !matches!(
                effect.status,
                EffectStatus::Dispatched | EffectStatus::Indeterminate
            ) {
                return clone_status_with_work(&effect.status, &mut None);
            }
            let Some(attestation) = self
                .providers
                .reconcile_and_attest(&self.issuer, effect_id, &effect)
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
