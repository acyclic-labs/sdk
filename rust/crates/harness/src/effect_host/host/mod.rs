use super::*;

pub(crate) fn effect_error(
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work
        .as_deref()
        .and_then(|io| io.original_native_work())
        .is_some()
    {
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
    effect_error(
        work,
        crate::wire::ErrorCode::NotFound,
        "effect is not in its owning conversation",
    )
}

#[cfg(feature = "filesystem")]
fn validate_task_plan<P: StreamProvider>(
    owner: &crate::durable_host::TaskJournalOwner<P>,
    plan: &TaskEffectPlan,
    planning: bool,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
        owner.require_effect_grants(
            &plan.provider,
            planning,
            &mut Some(
                &mut crate::filesystem::PreparationIo::from_native_work(native)
                    .map(crate::store::history::HistoryWork::new)?,
            ),
        )?;
        owner.validate_input_file(
            &plan.request,
            Some(&crate::filesystem::PreparationIo::from_native_work(native)?),
        )?;
        return owner.validate_input_file(
            &plan.result_schema,
            Some(&crate::filesystem::PreparationIo::from_native_work(native)?),
        );
    }
    owner.require_effect_grants(&plan.provider, planning, &mut None)?;
    owner.validate_input_file(&plan.request, None)?;
    owner.validate_input_file(&plan.result_schema, None)
}

#[cfg(feature = "filesystem")]
async fn verify_owner<P: StreamProvider>(
    owner: &crate::durable_host::TaskJournalOwner<P>,
    settlement: bool,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
        return owner
            .verify(
                settlement,
                &mut Some(
                    &mut crate::filesystem::PreparationIo::from_native_work(native)
                        .map(crate::store::history::HistoryWork::new)?,
                ),
            )
            .await;
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
    status: &EffectStatus,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
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
    (value.provider.capacity() as u64)
        .checked_add(status_owned_bytes(&value.status)?)
        .ok_or_else(|| Error::Invalid("effect attestation ownership overflows".into()))
}

#[cfg(feature = "filesystem")]
fn action_owned_bytes(action: &Action) -> Result<u64> {
    match action {
        Action::PlanEffect {
            provider,
            effect_kind,
            request,
            result_schema,
            ..
        } => (provider.capacity() as u64)
            .checked_add(effect_kind.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(request.logical_owned_bytes().ok()?))
            .and_then(|bytes| bytes.checked_add(result_schema.logical_owned_bytes().ok()?))
            .ok_or_else(|| Error::Invalid("effect action ownership overflows".into())),
        Action::MarkEffectDispatched { .. } => Ok(0),
        Action::ResolveEffect { observation } => attestation_owned_bytes(observation),
        _ => Err(Error::Invalid(
            "effect host action is not an effect action".into(),
        )),
    }
}

#[cfg(feature = "filesystem")]
enum TaskEffectAction<'a> {
    Plan(&'a TaskEffectPlan),
    Dispatch(crate::EffectAttemptId),
}

#[cfg(feature = "filesystem")]
fn prepare_action(
    effect_id: EffectId,
    action: &TaskEffectAction<'_>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Action> {
    Ok(match action {
        TaskEffectAction::Plan(plan) => {
            let provider = match work.as_deref_mut() {
                Some(io) => io.clone_text(&plan.provider)?,
                None => plan.provider.clone(),
            };
            let effect_kind = match work.as_deref_mut() {
                Some(io) => io.clone_text(&plan.effect_kind)?,
                None => plan.effect_kind.clone(),
            };
            Action::PlanEffect {
                effect_id,
                provider,
                guarantee: plan.guarantee,
                effect_kind,
                request: plan.request.copy(work)?,
                result_schema: plan.result_schema.copy(work)?,
            }
        }
        TaskEffectAction::Dispatch(attempt_id) => Action::MarkEffectDispatched {
            effect_id,
            attempt_id: *attempt_id,
        },
    })
}

fn effect_key<T: std::fmt::Display>(
    prefix: &str,
    identity: T,
    phase: Option<&str>,
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
    write!(&mut text, "{prefix}{identity}").map_err(|_| {
        effect_error(
            work,
            crate::wire::ErrorCode::Conflict,
            "effect identity formatting failed",
        )
    })?;
    if let Some(phase) = phase {
        text.push(':');
        text.push_str(phase);
    }
    if let Some(io) = work.as_deref_mut() {
        io.copy_bytes(text.len() as u64)?;
    }
    let retained = text.capacity() as u64;
    IdempotencyKey::new(text).map(|key| (key, retained))
}

/// Resolves a pinned provider attempt into its owning conversation history.
pub struct ConversationEffectHost<P> {
    stream: std::sync::Arc<P>,
    pub(super) authority: Authority,
    issuer: AuthorityIssuer,
    pub(super) scope: Scope,
    schemas: SchemaRegistry,
    content: Arc<dyn ContentResidencyVerifier>,
    pub(super) providers: EffectRegistry,
    effect_history_limits: HistoryReadLimits,
    terminal_effect_limit: usize,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_metadata: Option<acyclic_fs::NativeAllocation>,
}

impl<P: StreamProvider> ConversationEffectHost<P> {
    async fn effect_for_host<'a>(
        &self,
        aggregate: &'a StreamAggregate<P>,
        effect_id: EffectId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(std::borrow::Cow<'a, EffectState>, crate::NativeCustody)> {
        if let Some(effect) = aggregate.reducer().effect(effect_id) {
            return Ok((std::borrow::Cow::Borrowed(effect), None));
        }
        let custody;
        let (effect, retained) = aggregate
            .effect(effect_id, self.effect_history_limits, work)
            .await?
            .ok_or_else(|| missing_effect(work))?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            custody = match work.as_deref_mut() {
                Some(io) if io.original_native_work().is_some() => {
                    Some(io.take_native_custody(retained)?)
                }
                _ => None,
            };
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        {
            let _ = retained;
            custody = None;
        }
        Ok((std::borrow::Cow::Owned(effect), custody))
    }

    /// Binds one conversation, its signed effect-run scope and provider catalog.
    #[allow(
        clippy::too_many_arguments,
        reason = "conversation binding and its original custody"
    )]
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
            let mut slot = work
                .claim(
                    std::mem::size_of::<Self>() as u64,
                    0,
                    acyclic_fs::WorkCounters::UNCHARGED,
                )
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: None,
                })?;
            host.native_metadata
                .as_mut()
                .expect("validated native host")
                .absorb(&mut slot)
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: None,
                })?;
            let mut io = crate::filesystem::PreparationIo::from_native_work(work)
                .map(crate::store::history::HistoryWork::new)?;
            host.validate_binding(&mut Some(&mut io))?;
            return Ok(host);
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if host.native_metadata.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native effect metadata requires original work",
            });
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
        if self
            .native_metadata
            .as_ref()
            .is_some_and(|guard| guard.work().shares_ledger(work))
        {
            Ok(())
        } else {
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "effect host does not retain the original native metadata",
            })
        }
    }

    fn validate_binding(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if self.authority.kind != crate::core::AggregateKind::Conversation {
            return Err(effect_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "effect host requires a conversation aggregate",
            ));
        }
        let verifier = self.issuer.verifier(work)?;
        verifier.verify_audience(&self.authority, work)?;
        verifier.verify(&self.scope, work)?;
        if !self.scope.capabilities().contains(capability::EFFECT_RUN) {
            return Err(effect_error(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "effect host scope lacks effect:run",
            ));
        }
        Ok(())
    }

    /// Clones the original metadata through its SDK source producers.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn clone_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        use crate::store::history::HistoryIo as _;
        let mut io = crate::filesystem::PreparationIo::from_native_work(work)
            .map(crate::store::history::HistoryWork::new)?;
        let authority = io.clone_authority(&self.authority)?;
        let issuer = self.issuer.copy(&mut Some(&mut io))?;
        let scope = self.scope.copy(&mut Some(&mut io))?;
        let schemas = self.schemas.copy(&mut Some(&mut io))?;
        let providers = self.providers.clone_with_native_work(work)?;
        let mut host = Self::new(
            self.stream.clone(),
            authority,
            issuer,
            scope,
            schemas,
            Arc::clone(&self.content),
            providers,
            Some(io.into_custody()?),
            Some(work),
        )?;
        host.effect_history_limits = self.effect_history_limits;
        host.terminal_effect_limit = self.terminal_effect_limit;
        Ok(host)
    }

    /// Funds the SDK-owned Arc body before its one allocation attempt.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn into_arc_with_native_work(
        mut self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        use crate::store::history::HistoryIo as _;
        self.require_native_metadata(work)?;
        let mut io = crate::filesystem::PreparationIo::from_native_work(work)
            .map(crate::store::history::HistoryWork::new)?;
        let bytes = std::mem::size_of::<Self>() as u64;
        io.admit_allocation(bytes)?;
        io.retain_owned(bytes)?;
        let mut custody = io.into_custody()?;
        self.native_metadata
            .as_mut()
            .expect("validated native host")
            .absorb(&mut custody)
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: None,
            })?;
        Ok(Arc::new(self))
    }

    async fn aggregate(
        &self,
        stream: &std::sync::Arc<P>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(StreamAggregate<P>, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut native_io = work
            .as_deref()
            .and_then(|io| io.original_native_work())
            .map(crate::filesystem::PreparationIo::from_native_work)
            .transpose()?
            .map(crate::store::history::HistoryWork::new);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut observed = match native_io.as_mut() {
            Some(io) => Some(io as &mut dyn crate::store::history::HistoryIo),
            None => work
                .as_deref_mut()
                .map(|io| io as &mut dyn crate::store::history::HistoryIo),
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let mut observed = work
            .as_deref_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        let authority = match observed.as_deref_mut() {
            Some(io) => io.clone_authority(&self.authority)?,
            None => self.authority.clone(),
        };
        let verifier = self.issuer.verifier(&mut observed)?;
        let aggregate = StreamAggregate::open(
            stream,
            authority,
            verifier,
            self.schemas.clone(),
            None,
            default_projection_read_limits(),
            self.terminal_effect_limit,
            None,
            &mut observed,
        )
        .await?
        .with_content_verifier(Arc::clone(&self.content))
        .with_effect_history_read_limits(self.effect_history_limits)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let custody = native_io.map(|io| io.into_custody()).transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let custody = None;
        Ok((aggregate, custody))
    }
}

mod task;
