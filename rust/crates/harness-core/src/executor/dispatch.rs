use super::*;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use crate::wire::ErrorCode::Unsupported;
use futures::{FutureExt as _, future::Either};

struct ObservedCall<'a> {
    operation_id: OperationId,
    dispatch: crate::model::ModelDispatch,
    model: &'a Model,
    sampling: crate::model::ModelSampling,
    max_output_tokens: Option<u32>,
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
        let cancellation = journal.model_cancellation();
        let execution: acyclic_stream::BoxProviderFuture<'_, Result<Vec<ModelEvent>>> =
            Box::pin(async {
                let request::ModelStepRequest {
                    history: mut prior_messages,
                    prepared,
                } = request;
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
                let history = match &prior_messages {
                    request::ModelStepHistory::Turn(history, _) => history.as_slice(),
                    request::ModelStepHistory::Selected(history) => history,
                };
                self.dispatch_model_step(journal, input, step, (history, prepared), scope, phase)
                    .await
            });
        completion
            .run(async {
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
        material: (&[ModelMessage], Option<crate::model::PreparedModelRequest>),
        scope: ModelStepScope<'_>,
        phase: &mut DispatchPhase,
    ) -> Result<Vec<ModelEvent>> {
        let io = self.io;
        let (prior_messages, prepared) = material;
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
                self.retain_response_request(journal, input, step, prior_messages.len(), request)
                    .await?;
            }
            return Ok(replayed);
        }
        let dispatch = crate::model::ModelDispatch {
            operation_id: purpose.attempt_operation(input.operation_id, io)?,
            step,
            request_digest,
        };
        #[cfg(feature = "filesystem")]
        let _continuation_hold;
        let continuation = if started {
            self.reconcile_model_events(dispatch, &replayed).await?
        } else {
            None
        };
        // Only a fresh provider generation streams; a continuation replays.
        let live = continuation.is_none() && purpose == ModelPurpose::Response;
        let mut clock = super::model_usage::CallClock::unknown();
        let stream = if let Some(continuation) = continuation {
            #[cfg(feature = "filesystem")]
            {
                _continuation_hold = self.hold_continuation(&continuation)?;
            }
            Either::Left(futures::stream::iter(continuation.into_iter().map(Ok)))
        } else {
            #[cfg(feature = "filesystem")]
            {
                _continuation_hold = None;
            }
            if started {
                return Err(self.unreconciled_attempt(input.operation_id));
            }
            let attempt = super::phase::claim(super::phase::Publication::ModelDispatch.observe(
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
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let generated = if let Some(work) = self.native_work() {
                Either::Right(
                    attempt
                        .generate_native(self.stock.provider.as_ref(), &request, dispatch, work)?
                        .await?
                        .map(|result| self.admit_model_event(result)),
                )
            } else {
                Either::Left(attempt.generate(self.stock.provider.as_ref(), &request, dispatch))
            };
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let generated = attempt.generate(self.stock.provider.as_ref(), &request, dispatch);
            Either::Right(generated)
        };
        let observed = self
            .record_model_events(
                journal,
                ObservedCall {
                    operation_id: input.operation_id,
                    dispatch,
                    model: &request.request().model,
                    sampling: request.request().sampling,
                    max_output_tokens: request.request().max_output_tokens,
                    purpose,
                    clock,
                },
                replayed,
                &mut admission,
                (stream, live),
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
    }

    async fn prepare_dispatch_step(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        material: (&[ModelMessage], Option<crate::model::PreparedModelRequest>),
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
        } = self
            .retained_dispatch_step(journal, input.operation_id, step, purpose)
            .await?;
        let started = request.is_some();
        if started {
            phase.start();
        }
        if self.stock.reconciliation_only && !started && purpose != ModelPurpose::Response {
            return Err(Error::Indeterminate(input.operation_id));
        }
        let preparation = self.resolve_model_request(
            journal,
            input,
            (step, prior_messages, scope, prepared),
            request,
        );
        let request = if started {
            preparation.await?
        } else {
            super::phase::run(journal, self.io, preparation).await?
        };
        Ok(PreparedDispatchStep {
            #[cfg(feature = "research")]
            journal_cut,
            request,
            admission,
            events,
            started,
        })
    }

    #[cfg(feature = "filesystem")]
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

    async fn retained_dispatch_step(
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
                if purpose == ModelPurpose::Summary {
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
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = self.io;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let continuation = if let Some(work) = self.native_work() {
            let (continuation, custody) = self
                .stock
                .provider
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
            self.stock.provider.reconcile(dispatch, replayed).await?
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let continuation = self.stock.provider.reconcile(dispatch, &replayed).await?;
        Ok(continuation)
    }

    async fn claim_model_dispatch(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        turn: (u32, ModelStepScope<'_>, &str),
        request: &crate::model::PreparedModelRequest,
        phase: &mut DispatchPhase,
    ) -> Result<phase::ProviderAttempt> {
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
        let claim_key = with_history_io(io, |work| {
            preparation_format(
                format_args!("{purpose_key}:{step}:claim:{}", OperationId::new()),
                work,
            )
        })?;
        let reference = if purpose == ModelPurpose::Response {
            self.stage_response_request(journal, input, step, &request_key, request)
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
        Ok(attempt)
    }

    async fn record_model_events(
        &self,
        journal: &dyn ExecutionJournal,
        call: ObservedCall<'_>,
        mut observed: Vec<ModelEvent>,
        admission: &mut ModelEventAdmission,
        (stream, live): (
            Either<
                impl futures::Stream<Item = Result<ModelEvent>> + Unpin,
                impl futures::Stream<Item = Result<ModelEvent>> + Unpin,
            >,
            bool,
        ),
        #[cfg(feature = "research")] phase: &mut DispatchPhase,
    ) -> Result<Vec<ModelEvent>> {
        let ObservedCall {
            operation_id,
            dispatch,
            model,
            sampling,
            max_output_tokens,
            purpose,
            clock,
        } = call;
        #[cfg(feature = "research")]
        let mut clock = clock;
        let step = dispatch.step;
        let purpose_key = model_purpose_key(purpose);
        let io = self.io;
        let mut text = self
            .stock
            .live_reply
            .as_ref()
            .filter(|_| live)
            .map(LiveReply::dispatch);
        {
            let mut stream = stream;
            loop {
                // Stage the text so far whenever the provider pauses.
                let next = match (stream.next().now_or_never(), text.as_mut()) {
                    (Some(next), _) => next,
                    (None, Some(text)) => {
                        text.flush().await?;
                        crate::obs::waiting("provider-event", operation_id, stream.next()).await
                    }
                    (None, None) => {
                        crate::obs::waiting("provider-event", operation_id, stream.next()).await
                    }
                };
                let Some(event) = next else {
                    break;
                };
                let event = event?;
                #[cfg(feature = "research")]
                clock.observe(&event);
                let latency_ns = matches!(event, ModelEvent::Completed { .. })
                    .then(|| clock.elapsed_ns())
                    .flatten();
                let index = observed.len();
                let key = with_history_io(io, |work| {
                    preparation_format(format_args!("{purpose_key}:{step}:{index}"), work)
                })?;
                stock_vec_push(&mut observed, event, io)?;
                let event = observed.last().ok_or_else(|| {
                    stock_boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "model observation was not retained",
                    )
                })?;
                admission.observe(event, self.stock.limits, io)?;
                if let Some(text) = text.as_mut() {
                    text.observe(event);
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
                            max_output_tokens,
                            tokens: super::model_usage::ModelTokenUsage::reported(metadata),
                            latency_ns,
                            #[cfg(feature = "research")]
                            provider_request_id: metadata["provider_request_id"].as_str(),
                            #[cfg(feature = "research")]
                            system_fingerprint: metadata["system_fingerprint"].as_str(),
                            #[cfg(feature = "research")]
                            ttft_ns: clock.ttft_ns(),
                            #[cfg(feature = "research")]
                            cache: ModelCacheObservation::reported(metadata),
                        })
                    }
                    _ => None,
                };
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
                #[cfg(feature = "research")]
                if admission.completed && purpose == ModelPurpose::Response {
                    phase.1 = receipt.journal_cut;
                }
                #[cfg(not(feature = "research"))]
                let _ = receipt;
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
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = self.native_work() {
            work.remaining()?;
            if !self.stock.provider.supports_native_model(work)? {
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
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn admit_model_event(
        &self,
        result: Result<(ModelEvent, acyclic_fs::NativeAllocation)>,
    ) -> Result<ModelEvent> {
        let io = self.io;

        let (event, custody) = result?;
        io.ok_or_else(|| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "native stock session lacks its original IO",
            )
        })?
        .absorb_native_custody(custody)?;
        Ok(event)
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

fn model_purpose_key(purpose: ModelPurpose) -> &'static str {
    match purpose {
        ModelPurpose::Response => "model",
        ModelPurpose::Summary => "summary",
    }
}
