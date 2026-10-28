use super::*;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use crate::wire::ErrorCode::Unsupported;
use futures::future::Either;

struct PreparedDispatchStep {
    request: crate::model::PreparedModelRequest,
    admission: ModelEventAdmission,
    events: Vec<ModelEvent>,
    started: bool,
}

impl StockExecution<'_> {
    /// Cancellation covers preparation and response headers as well as chunks.
    pub(super) async fn cancellable_model_step(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
        purpose: ModelPurpose,
        phase: &mut DispatchPhase,
    ) -> Result<Vec<ModelEvent>> {
        let completion = super::phase::PreparedPhase::new(journal, self.io)?;
        let cancellation = journal.model_cancellation();
        let execution: acyclic_stream::BoxProviderFuture<'_, Result<Vec<ModelEvent>>> = Box::pin(
            self.dispatch_model_step(journal, input, step, prior_messages, purpose, phase),
        );
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
    pub(super) async fn dispatch_model_step(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
        purpose: ModelPurpose,
        phase: &mut DispatchPhase,
    ) -> Result<Vec<ModelEvent>> {
        let io = self.io;
        let PreparedDispatchStep {
            request,
            mut admission,
            events: replayed,
            started,
        } = self
            .prepare_dispatch_step(journal, input, step, prior_messages, purpose, phase)
            .await?;
        let request_digest = request.request_digest();
        let purpose_key = model_purpose_key(purpose);
        #[cfg(not(target_arch = "wasm32"))]
        self.record_model_phase(&admission, started);
        if admission.completed {
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
            let attempt = if started {
                self.resume_model_attempt(input.operation_id, dispatch, replayed.is_empty(), phase)
                    .await?
            } else {
                super::phase::claim(self.claim_model_dispatch(
                    journal,
                    input,
                    (step, purpose, purpose_key),
                    &request,
                    phase,
                ))
                .await?
            };
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
                input.operation_id,
                (step, purpose, purpose_key),
                replayed,
                &mut admission,
                stream,
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
        prior_messages: &[ModelMessage],
        purpose: ModelPurpose,
        phase: &mut DispatchPhase,
    ) -> Result<PreparedDispatchStep> {
        self.require_model_producer().await?;
        let RetainedModelStep {
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
            (step, prior_messages, purpose),
            request,
            phase,
        );
        let request = if started {
            preparation.await?
        } else {
            super::phase::run(journal, self.io, preparation).await?
        };
        Ok(PreparedDispatchStep {
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

    async fn resume_model_attempt(
        &self,
        operation: OperationId,
        dispatch: crate::model::ModelDispatch,
        replayed_empty: bool,
        phase: &mut DispatchPhase,
    ) -> Result<phase::ProviderAttempt> {
        if replayed_empty && !self.stock.reconciliation_only && !super::unsettled_resume::enabled()
        {
            // Only the deleted task owner could reissue a retained request;
            // an agent turn has no such recovery authority.
            let _ = (dispatch, phase);
            Err(Error::Indeterminate(operation))
        } else if !self.stock.reconciliation_only && !super::unsettled_resume::enabled() {
            // An incomplete response has not dispatched any of its tool calls.
            // When the provider cannot recover its prefix, fail definitively
            // instead of retaining a reservation that can never make progress.
            // Keep the recorded prefix and never generate a different suffix.
            Err(Error::ModelUnavailable {
                failure: crate::ModelTransportFailure::Interrupted,
                excerpt: String::new(),
            })
        } else {
            Err(Error::Indeterminate(operation))
        }
    }

    async fn resolve_model_request(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        turn: (u32, &[ModelMessage], ModelPurpose),
        retained_request: Option<crate::model::PreparedModelRequest>,
        phase: &mut DispatchPhase,
    ) -> Result<crate::model::PreparedModelRequest> {
        let (step, prior_messages, purpose) = turn;
        let mut request = match retained_request {
            Some(recorded) => {
                if purpose == ModelPurpose::Summary
                    && self
                        .prepare_request(input, step, prior_messages)
                        .await?
                        .request_digest()
                        != recorded.request_digest()
                {
                    return Err(Error::Conflict(
                        "summary source or instruction differs from retained admission".into(),
                    ));
                }
                recorded
            }
            None if purpose == ModelPurpose::Response => {
                self.prepared_response(journal, input, step, prior_messages, phase)
                    .await?
            }
            None => self.prepare_request(input, step, prior_messages).await?,
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
        turn: (u32, ModelPurpose, &str),
        request: &crate::model::PreparedModelRequest,
        phase: &mut DispatchPhase,
    ) -> Result<phase::ProviderAttempt> {
        let (step, purpose, purpose_key) = turn;
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
        Ok(attempt)
    }

    async fn record_model_events(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        turn: (u32, ModelPurpose, &str),
        mut observed: Vec<ModelEvent>,
        admission: &mut ModelEventAdmission,
        stream: impl futures::Stream<Item = Result<ModelEvent>> + Unpin,
    ) -> Result<Vec<ModelEvent>> {
        let (step, purpose, purpose_key) = turn;
        let io = self.io;
        {
            let mut stream = stream;
            loop {
                let next = stream.next().await;
                let Some(event) = next else {
                    break;
                };
                let event = event?;
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
                super::model_observation::append(
                    journal,
                    operation_id,
                    &key,
                    (step, purpose),
                    event,
                    io,
                )
                .await?;
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
