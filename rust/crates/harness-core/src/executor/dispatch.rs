use super::*;
#[cfg(not(target_arch = "wasm32"))]
use crate::wire::ErrorCode::Unsupported;
use futures::future::Either;

#[cfg(test)]
#[path = "dispatch_size_tests.rs"]
mod size_tests;

struct ObservedCall<'a> {
    operation_id: OperationId,
    dispatch: crate::model::ModelDispatch,
    model: &'a Model,
    sampling: crate::model::ModelSampling,
    purpose: ModelPurpose,
    clock: super::model_usage::CallClock,
}

struct PreparedDispatchStep {
    #[cfg(feature = "research")]
    journal_cut: Option<ExecutionJournalCut>,
    request: crate::model::PreparedModelRequest,
    admission: ModelEventAdmission,
    events: Vec<ModelEvent>,
    started: bool,
}

/// Only the response loop owns an inbox boundary. Borrowed standalone and
/// summary requests keep the same journal without inheriting that boundary.
#[derive(Clone, Copy)]
struct ModelStepScope<'a> {
    purpose: ModelPurpose,
    boundary: Option<&'a dyn crate::agent::ModelBoundary>,
}

impl StockExecution<'_> {
    /// Cancellation covers boundary reads, preparation, headers and chunks.
    pub(super) async fn cancellable_model_step(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        request: request::ModelStepRequest<'_>,
        purpose: ModelPurpose,
        phase: &mut DispatchPhase,
    ) -> Result<Vec<ModelEvent>> {
        let completion = super::phase::PreparedPhase::new(journal, self.io)?;
        completion
            .run(async {
                // Construct and retire provider custody inside the completion body.
                let cancellation = journal.model_cancellation();
                let execution =
                    self.dispatch_model_step(journal, input, step, request, purpose, phase);
                futures::pin_mut!(execution);
                match futures::future::select(cancellation, execution).await {
                    Either::Left((result, _)) => {
                        result?;
                        Err(Error::InteractionRejected(
                            crate::InteractionRejection::Cancelled,
                        ))
                    }
                    Either::Right((result, _)) => result,
                }
            })
            .await
    }

    /// Replays or resolves one model step.
    async fn dispatch_model_step(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        request: request::ModelStepRequest<'_>,
        purpose: ModelPurpose,
        phase: &mut DispatchPhase,
    ) -> Result<Vec<ModelEvent>> {
        crate::conversation::BoundedContentFuture::new(self.io, || async {
            let io = self.io;
            let request::ModelStepRequest {
                history: mut prior_messages,
                prepared,
                publish_reply,
            } = request;
            let retained = self
                .retained_dispatch_step(journal, input.operation_id, step, purpose)
                .await?;
            let scope = ModelStepScope {
                purpose,
                boundary: match &prior_messages {
                    request::ModelStepHistory::Turn(..) => journal.model_boundary(),
                    request::ModelStepHistory::Selected(_) => None,
                },
            };
            if let request::ModelStepHistory::Turn(history, steering) = &mut prior_messages
                && let Some(boundary) = scope.boundary
            {
                let (_, records) = self
                    .model_records(journal, input.operation_id, step, ModelPurpose::Response)
                    .await?;
                let frozen = records.iter().find_map(|record| match record.event {
                    ExecutionEvent::ContextPrepared {
                        step: recorded,
                        steering,
                        ..
                    } if recorded == step => Some(steering),
                    _ => None,
                });
                if matches!(frozen, Some(None)) {
                    return Err(Error::Storage(
                        "agent request has no frozen inbox range".into(),
                    ));
                }
                let messages = boundary
                    .messages(input.operation_id, step, frozen.flatten())
                    .await?;
                let from = history.len();
                for message in messages {
                    with_history_io(self.io, |work| {
                        message.content.validate_limits(self.stock.limits, work)
                    })?;
                    stock_vec_push(history, message, self.io)?;
                }
                steering.admit(from..history.len(), self.io)?;
            }
            if retained.request.is_none() {
                self.selected_tools().refresh().await?;
            }
            if let request::ModelStepHistory::Turn(history, _) = &mut prior_messages {
                let live;
                let tools = match retained.request.as_ref() {
                    Some(recorded) => &recorded.request().tools,
                    None => {
                        live = self.request_tools()?;
                        &live
                    }
                };
                {
                    let mut context = crate::context::Context {
                        messages: std::mem::take(history),
                        ..Default::default()
                    };
                    let result = self.tools_notice(&mut context, tools);
                    **history = context.messages;
                    result?;
                }
            }
            let prior_messages = match &prior_messages {
                request::ModelStepHistory::Turn(history, _) => history.as_slice(),
                request::ModelStepHistory::Selected(history) => history,
            };
            let PreparedDispatchStep {
                #[cfg(feature = "research")]
                journal_cut,
                request,
                mut admission,
                events: replayed,
                started,
            } = self
                .prepare_dispatch_step(
                    journal,
                    input,
                    step,
                    (prior_messages, prepared),
                    retained,
                    scope,
                    phase,
                )
                .await?;
            let purpose = scope.purpose;
            let request_digest = request.request_digest();
            let purpose_key = model_purpose_key(purpose);
            if started
                && purpose == ModelPurpose::Response
                && let Some(boundary) = scope.boundary
            {
                boundary.admitted(input.operation_id, step).await?;
            }
            #[cfg(not(target_arch = "wasm32"))]
            self.record_model_phase(&admission, started);
            if admission.completed {
                #[cfg(feature = "research")]
                if purpose == ModelPurpose::Response {
                    phase.1 = journal_cut;
                }
                crate::obs::obs_record!("items" = replayed.len());
                if purpose == ModelPurpose::Response {
                    self.retain_response_request(
                        journal,
                        input,
                        step,
                        prior_messages.len(),
                        request,
                    )
                    .await?;
                }
                return stock_active_model_events(replayed, io);
            }
            let dispatch = crate::model::ModelDispatch {
                operation_id: purpose.attempt_operation(input.operation_id, io)?,
                step,
                request_digest,
            };
            let _continuation_hold;
            let forced = self
                .stock
                .intervention
                .as_deref()
                .filter(|_| step == 0 && purpose == ModelPurpose::Response)
                .and_then(crate::agent::ContinuationOverride::events);
            let continuation = if let Some(mut events) = forced {
                if !events.starts_with(&replayed) {
                    return Err(Error::Conflict(
                        "forced action differs from its journal prefix".into(),
                    ));
                }
                if !started {
                    self.claim_model_dispatch(
                        journal,
                        input,
                        (step, scope, purpose_key),
                        &request,
                        phase,
                    )
                    .await?;
                }
                Some(events.split_off(replayed.len()))
            } else if started {
                self.reconcile_model_events(dispatch, &replayed).await?
            } else {
                None
            };
            // Only a fresh provider generation streams; a continuation replays.
            let live = publish_reply && continuation.is_none() && purpose == ModelPurpose::Response;
            let mut clock = super::model_usage::CallClock::unknown();
            let (stream, source) = if let Some(continuation) = continuation {
                {
                    _continuation_hold = self.hold_continuation(&continuation)?;
                }
                (Either::Left(continuation.into_iter()), None)
            } else {
                {
                    _continuation_hold = None;
                }
                if started {
                    return Err(self.unreconciled_attempt(input.operation_id));
                }
                let (attempt, source) =
                    super::phase::claim(super::phase::Publication::ModelDispatch.observe(
                        input.operation_id,
                        step,
                        self.claim_model_dispatch(
                            journal,
                            input,
                            (step, scope, purpose_key),
                            &request,
                            phase,
                        ),
                    ))
                    .await?;
                clock.start();
                #[cfg(not(target_arch = "wasm32"))]
                let generated = if let Some(work) = self.native_work() {
                    Either::Right(
                        attempt
                            .generate_native(
                                self.selected_provider().as_ref(),
                                &request,
                                dispatch,
                                work,
                            )?
                            .await?
                            .map(|result| stock_admit_model_event(result, self.io)),
                    )
                } else {
                    Either::Left(attempt.generate(
                        self.selected_provider().as_ref(),
                        &request,
                        dispatch,
                    ))
                };
                #[cfg(target_arch = "wasm32")]
                let generated =
                    attempt.generate(self.selected_provider().as_ref(), &request, dispatch);
                (Either::Right(generated), live.then_some(source))
            };
            let observed = self
                .record_model_events(
                    journal,
                    ObservedCall {
                        operation_id: input.operation_id,
                        dispatch,
                        model: &request.request().model,
                        sampling: request.request().sampling,
                        purpose,
                        clock,
                    },
                    stock_active_model_events(replayed, io)?,
                    &mut admission,
                    (stream, source),
                    #[cfg(feature = "research")]
                    phase,
                )
                .await?;
            if purpose == ModelPurpose::Response && admission.completed {
                self.retain_response_request(journal, input, step, prior_messages.len(), request)
                    .await?;
            }
            crate::obs::obs_record!("items" = observed.len());
            Ok(observed)
        })?
        .await
    }

    async fn prepare_dispatch_step(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        material: (&[ModelMessage], Option<crate::model::PreparedModelRequest>),
        retained: RetainedModelStep,
        scope: ModelStepScope<'_>,
        phase: &mut DispatchPhase,
    ) -> Result<PreparedDispatchStep> {
        let purpose = scope.purpose;
        let (prior_messages, prepared) = material;
        self.require_model_producer().await?;
        let RetainedModelStep {
            #[cfg(feature = "research")]
            journal_cut,
            request,
            admission,
            events,
        } = retained;
        let started = request.is_some();
        if started {
            phase.start();
        }
        if self.stock.reconciliation_only && !started && purpose != ModelPurpose::Response {
            return Err(Error::Indeterminate(input.operation_id));
        }
        let request = crate::conversation::BoundedContentFuture::new(self.io, || {
            self.resolve_model_request(
                journal,
                input,
                (step, prior_messages, scope, prepared),
                request,
            )
        })?
        .await?;
        Ok(PreparedDispatchStep {
            #[cfg(feature = "research")]
            journal_cut,
            request,
            admission,
            events,
            started,
        })
    }

    fn hold_continuation(
        &self,
        continuation: &Vec<ModelEvent>,
    ) -> Result<Option<crate::conversation::ContentHold<'_>>> {
        self.io
            .filter(|_| stock_is_native(self.io))
            .map(|io| {
                io.guard_owned(
                    continuation.capacity() as u64 * std::mem::size_of::<ModelEvent>() as u64,
                )
            })
            .transpose()
    }

    pub(super) async fn retained_dispatch_step(
        &self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        step: u32,
        purpose: ModelPurpose,
    ) -> Result<RetainedModelStep> {
        let (_, records) = self
            .model_records(journal, operation, step, purpose)
            .await?;
        retain_preparation(
            retained_model_step(journal, &records, step, purpose, self.stock.limits, self.io)
                .await?,
        )
    }

    /// A started attempt whose provider cannot reconcile it, with or without
    /// an observed prefix. Its response dispatched no tool call, and no
    /// recovery authority may generate the uncertain request again, so the
    /// step fails definitively instead of retaining a reservation that can
    /// never make progress. The recorded prefix is kept unchanged.
    fn unreconciled_attempt(&self, operation: OperationId) -> Error {
        if self.stock.reconciliation_only || super::unsettled_resume::enabled() {
            Error::Indeterminate(operation)
        } else {
            Error::ModelUnavailable {
                failure: crate::ModelTransportFailure::Interrupted,
                excerpt: String::new(),
            }
        }
    }

    async fn resolve_model_request(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        turn: (
            u32,
            &[ModelMessage],
            ModelStepScope<'_>,
            Option<crate::model::PreparedModelRequest>,
        ),
        retained_request: Option<crate::model::PreparedModelRequest>,
    ) -> Result<crate::model::PreparedModelRequest> {
        let (step, prior_messages, scope, prepared) = turn;
        let purpose = scope.purpose;
        let mut request = match (retained_request, prepared) {
            (Some(recorded), prepared) => {
                if matches!(
                    purpose,
                    ModelPurpose::Summary | ModelPurpose::SummaryPart(_)
                ) {
                    let selected = match prepared {
                        Some(request) => request,
                        None => self.prepare_request(input, step, prior_messages).await?,
                    };
                    if selected.request_digest() != recorded.request_digest() {
                        return Err(Error::Conflict(
                            "summary source or instruction differs from retained admission".into(),
                        ));
                    }
                }
                recorded
            }
            (None, Some(request)) => request,
            (None, None) if purpose == ModelPurpose::Response => {
                self.prepared_response(journal, input, step, prior_messages, scope.boundary)
                    .await?
            }
            (None, None) => self.prepare_request(input, step, prior_messages).await?,
        };
        self.verify_model_request_content(journal, &mut request)
            .await?;
        Ok(request)
    }

    async fn reconcile_model_events(
        &self,
        dispatch: crate::model::ModelDispatch,
        replayed: &[ModelEvent],
    ) -> Result<Option<Vec<ModelEvent>>> {
        #[cfg(not(target_arch = "wasm32"))]
        let io = self.io;
        #[cfg(not(target_arch = "wasm32"))]
        let continuation = if let Some(work) = self.native_work() {
            let (continuation, custody) = self
                .selected_provider()
                .reconcile_with_native_work(dispatch, replayed, work)?
                .await?;
            io.ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "native stock session lacks its original IO",
                )
            })?
            .absorb_native_custody(custody)?;
            continuation
        } else {
            self.selected_provider()
                .reconcile(dispatch, replayed)
                .await?
        };
        #[cfg(target_arch = "wasm32")]
        let continuation = self
            .selected_provider()
            .reconcile(dispatch, &replayed)
            .await?;
        Ok(continuation)
    }

    async fn claim_model_dispatch(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        turn: (u32, ModelStepScope<'_>, ModelPurposeKey),
        request: &crate::model::PreparedModelRequest,
        phase: &mut DispatchPhase,
    ) -> Result<(phase::ProviderAttempt, OperationId)> {
        let _writer = self.journal_writes.lock().await;
        let (step, scope, purpose_key) = turn;
        let purpose = scope.purpose;
        let io = self.io;
        let request_digest = request.request_digest();
        if self.stock.reconciliation_only {
            return Err(Error::Indeterminate(input.operation_id));
        }
        let (tail, current) = self
            .model_records(journal, input.operation_id, step, purpose)
            .await?;
        if let Some(existing) = current.iter().find_map(|record| match &record.event {
            ExecutionEvent::ModelStarted {
                step: recorded,
                request_digest,
                ..
            } if *recorded == step => Some(*request_digest),
            _ => None,
        }) {
            if existing != request_digest {
                return Err(Error::Conflict(
                    "model attempt identity is bound to another request".into(),
                ));
            }
            return Err(Error::Indeterminate(input.operation_id));
        }
        let request_key = with_history_io(io, |work| {
            preparation_format(format_args!("{purpose_key}:{step}:request"), work)
        })?;
        // A claim acquires the dispatch: its key names this attempt, so a
        // racer at the same tail is refused instead of recognised as a retry
        // and dispatching the model a second time. Settlements stay stable.
        let claim = OperationId::new();
        let claim_key = with_history_io(io, |work| {
            preparation_format(format_args!("{purpose_key}:{step}:claim:{claim}"), work)
        })?;
        let reference = if purpose == ModelPurpose::Response {
            self.stage_response_request(journal, input, step, request)
                .await?
        } else {
            transcript::stage_request(journal, input.operation_id, &request_key, request, None, io)
                .await?
        };
        let attempt = phase.start();
        match journal
            .append_if_tail(
                input.operation_id,
                tail,
                &claim_key,
                &ExecutionEvent::ModelStarted {
                    step,
                    purpose,
                    request_digest,
                    request: reference,
                    context: request.context_window(),
                },
                io,
            )?
            .await
        {
            Ok(true) => {}
            Err(error) if stock_is_native(io) => return Err(error),
            Err(error) if !error.leaves_effect_uncertain() => return Err(error),
            Ok(false) | Err(_) => {
                return Err(Error::Indeterminate(input.operation_id));
            }
        }
        if purpose == ModelPurpose::Response
            && let Some(boundary) = scope.boundary
        {
            boundary.admitted(input.operation_id, step).await?;
        }
        Ok((attempt, claim))
    }

    async fn record_model_events(
        &self,
        journal: &dyn ExecutionJournal,
        call: ObservedCall<'_>,
        mut observed: Vec<ModelEvent>,
        admission: &mut ModelEventAdmission,
        (stream, source): (
            Either<
                std::vec::IntoIter<ModelEvent>,
                impl futures::Stream<Item = Result<ModelEvent>> + Unpin,
            >,
            Option<OperationId>,
        ),
        #[cfg(feature = "research")] phase: &mut DispatchPhase,
    ) -> Result<Vec<ModelEvent>> {
        let ObservedCall {
            operation_id,
            dispatch,
            model,
            sampling,
            purpose,
            clock,
        } = call;
        let mut clock = clock;
        let step = dispatch.step;
        let purpose_key = model_purpose_key(purpose);
        let io = self.io;
        {
            let mut stream = stream;
            loop {
                let next = match &mut stream {
                    Either::Left(continuation) => continuation.next().map(Ok),
                    Either::Right(stream) => {
                        crate::obs::waiting("provider-event", operation_id, stream.next()).await
                    }
                };
                let Some(event) = next else {
                    break;
                };
                let event = event?;
                clock.observe(&event);
                let latency_ns = matches!(event, ModelEvent::Completed { .. })
                    .then(|| clock.elapsed_ns())
                    .flatten();
                let completed_at = matches!(event, ModelEvent::Completed { .. })
                    .then(|| clock.completed_at())
                    .flatten();
                let index = admission.count;
                let key = with_history_io(io, |work| match source {
                    Some(source) => preparation_format(
                        format_args!("model-live:{source}:{purpose_key}:{step}:{index}"),
                        work,
                    ),
                    None => preparation_format(format_args!("{purpose_key}:{step}:{index}"), work),
                })?;
                stock_push_model_event(&mut observed, event, io)?;
                let event = observed.last().ok_or_else(|| {
                    stock_boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "model observation was not retained",
                    )
                })?;
                admission.observe(event, self.stock.limits, io)?;
                // Reconciliation owns a finite continuation. Reject a known
                // successor before committing its terminal event; live streams
                // settle at completion without waiting for transport closure.
                if admission.completed
                    && let Either::Left(continuation) = &mut stream
                    && let Some(next) = continuation.next()
                {
                    admission.validate_next(&next, self.stock.limits, io)?;
                }
                let usage = match event {
                    ModelEvent::Completed { metadata } => {
                        Some(super::model_usage::ModelCallUsage {
                            version: ModelUsageRecord::VERSION,
                            completed: true,
                            dispatch,
                            purpose,
                            provider: &model.provider,
                            model: &model.name,
                            agent_path: self
                                .stock
                                .task_context
                                .as_ref()
                                .and_then(|(context, _)| context.agent())
                                .map(|(agent, _)| agent.as_str()),
                            sampling,
                            tokens: super::model_usage::ModelTokenUsage::reported(metadata),
                            started_at: clock.started_at(),
                            completed_at,
                            latency_ns,
                            provider_request_id: metadata["provider_request_id"].as_str(),
                            #[cfg(feature = "research")]
                            system_fingerprint: metadata["system_fingerprint"].as_str(),
                            ttft_ns: clock.ttft_ns(),
                            #[cfg(feature = "research")]
                            cache: ModelCacheObservation::reported(metadata),
                            abandoned: admission.abandoned,
                        })
                    }
                    _ => None,
                };
                let writer = self.journal_writes.lock().await;
                let receipt = crate::obs::waiting(
                    "journal-commit",
                    operation_id,
                    super::model_observation::append(
                        journal,
                        operation_id,
                        &key,
                        (step, purpose),
                        event,
                        usage.as_ref(),
                        io,
                    ),
                )
                .await?;
                drop(writer);
                #[cfg(feature = "research")]
                if admission.completed && purpose == ModelPurpose::Response {
                    phase.1 = receipt.journal_cut;
                }
                #[cfg(not(feature = "research"))]
                let _ = receipt;
                // Wait facts are durable now; response assembly needs none of them.
                // Recovery loads its original ordered prefix from the journal.
                if matches!(event, ModelEvent::Wait(_) | ModelEvent::Trace { .. }) {
                    stock_retire_event(
                        observed.pop().expect("the committed event is retained"),
                        io,
                    )?;
                }
                // The committed terminal event settles this step. Transport
                // closure is independent and may never follow it.
                if admission.completed {
                    break;
                }
            }
        }
        Ok(observed)
    }

    async fn require_model_producer(&self) -> Result<()> {
        let io = self.io;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = self.native_work() {
            work.remaining()?;
            if !self.selected_provider().supports_native_model(work)? {
                return Err(stock_boundary(
                    io,
                    Unsupported,
                    "model provider has no original native-ledger producer",
                ));
            }
        }
        let _ = io;
        Ok(())
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn record_model_phase(&self, admission: &ModelEventAdmission, started: bool) {
        crate::obs::obs_record!(
            "phase" = match (admission.completed, started) {
                (true, _) => "replay",
                (false, true) => "reconcile",
                (false, false) => "dispatch",
            }
        );
    }
}

#[derive(Clone, Copy)]
pub(super) struct ModelPurposeKey(ModelPurpose);
impl std::fmt::Display for ModelPurposeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            ModelPurpose::Response => f.write_str("model"),
            ModelPurpose::Summary => f.write_str("summary"),
            ModelPurpose::SummaryPart(part) => write!(f, "summary-part:{part}"),
            ModelPurpose::Count => f.write_str("count"),
        }
    }
}
pub(super) fn model_purpose_key(purpose: ModelPurpose) -> ModelPurposeKey {
    ModelPurposeKey(purpose)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
    #[test]
    fn retiring_native_events_releases_their_original_custody() -> Result<()> {
        let model = Model::new("test".into(), "events".into(), "1".into())?;
        let stock = StockExecutor::new(
            model,
            std::sync::Arc::new(crate::test_support::ScriptedModelProvider::new([])),
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        let sources = [
            ModelEvent::Wait(crate::model::ModelWait::Finished {
                elapsed: std::time::Duration::from_secs(1),
            }),
            ModelEvent::Content {
                delta: "payload".into(),
            },
        ];
        let bytes = sources
            .iter()
            .map(crate::contract::canonical_json_bytes)
            .collect::<Result<Vec<_>>>()?;
        let observed = crate::test_support::accounted::accounted(true, 16_384, false, |io| {
            let execution = StockExecution {
                io: Some(io),
                ..StockExecution::ordinary(&stock)
            };
            let mut events = Vec::new();
            for _ in 0..64 {
                for bytes in &bytes {
                    let event = stock_admit_model_event(
                        ModelEvent::decode_with_native_work(
                            bytes,
                            io.original_native_work().unwrap(),
                        ),
                        execution.io,
                    )?;
                    stock_push_model_event(&mut events, event, Some(io))?;
                    stock_retire_event(events.pop().unwrap(), Some(io))?;
                    assert_eq!(
                        io.owned_bytes()?,
                        (events.capacity() * std::mem::size_of::<ModelEvent>()) as u64
                    );
                }
            }
            drop(stock_events_into_iter(events, Some(io))?);
            Ok::<_, Error>(())
        })
        .map_err(|(cause, _)| Error::Storage(cause))?;
        observed.0?;
        assert_eq!(observed.2, 0);
        assert_eq!(
            observed.5,
            acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
        );
        Ok(())
    }
    #[tokio::test]
    async fn committed_waits_do_not_accumulate_in_live_response_storage() -> Result<()> {
        let model = Model::new("test".into(), "waits".into(), "1".into())?;
        let stock = StockExecutor::new(
            model.clone(),
            std::sync::Arc::new(crate::test_support::ScriptedModelProvider::new([])),
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        let mut capacity = None;
        for waits in [1, 64] {
            let execution = StockExecution::ordinary(&stock);
            let journal = super::super::tests::Journal::default();
            let operation_id = OperationId::new();
            let dispatch = crate::model::ModelDispatch {
                operation_id,
                step: 0,
                request_digest: [0; 32],
            };
            let events = (0..waits)
                .flat_map(|_| {
                    [
                        ModelEvent::Wait(crate::model::ModelWait::Started {
                            cause: Some(crate::ModelTransportFailure::Status(429)),
                            delay: std::time::Duration::from_secs(1),
                            retry_after: true,
                        }),
                        ModelEvent::Wait(crate::model::ModelWait::Finished {
                            elapsed: std::time::Duration::from_secs(1),
                        }),
                    ]
                })
                .chain([ModelEvent::Completed {
                    metadata: Value::Null,
                }]);
            let mut admission = ModelEventAdmission::default();
            let observed = execution
                .record_model_events(
                    &journal,
                    ObservedCall {
                        operation_id,
                        dispatch,
                        model: &model,
                        sampling: crate::model::ModelSampling::default(),
                        purpose: ModelPurpose::Response,
                        clock: super::super::model_usage::CallClock::unknown(),
                    },
                    Vec::new(),
                    &mut admission,
                    (Either::Right(futures::stream::iter(events.map(Ok))), None),
                    #[cfg(feature = "research")]
                    &mut DispatchPhase::new(),
                )
                .await?;
            assert_eq!(
                observed,
                [ModelEvent::Completed {
                    metadata: Value::Null
                }]
            );
            assert_eq!(
                *capacity.get_or_insert(observed.capacity()),
                observed.capacity()
            );
            assert_eq!(admission.count, waits * 2 + 1);
            let records = journal.replay(operation_id, 0, 256, None)?.await?;
            assert_eq!(records.len(), waits * 2 + 1);
        }
        Ok(())
    }
}
