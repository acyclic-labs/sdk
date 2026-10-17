use super::*;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use crate::wire::ErrorCode::Unsupported;
use futures::future::Either;

impl StockExecution<'_> {
    /// Cancellation covers preparation and response headers as well as chunks.
    pub(super) async fn cancellable_model_step(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
        purpose: ModelPurpose,
    ) -> Result<Vec<ModelEvent>> {
        let cancellation = journal.model_cancellation();
        let execution: acyclic_stream::BoxProviderFuture<'_, Result<Vec<ModelEvent>>> =
            Box::pin(self.dispatch_model_step(journal, input, step, prior_messages, purpose));
        match futures::future::select(cancellation, execution).await {
            Either::Left((result, _)) => {
                result?;
                Err(Error::InteractionRejected(
                    crate::InteractionRejection::Cancelled,
                ))
            }
            Either::Right((result, _)) => result,
        }
    }

    /// Replays or resolves one model step.
    pub(super) async fn dispatch_model_step(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
        purpose: ModelPurpose,
    ) -> Result<Vec<ModelEvent>> {
        let io = self.io;
        self.require_model_producer().await?;
        let (_, records) = self
            .model_records(journal, input.operation_id, step, purpose)
            .await?;
        let RetainedModelStep {
            request: retained_request,
            mut admission,
            events: replayed,
        } = retain_preparation(
            retained_model_step(journal, &records, step, purpose, self.stock.limits, io).await?,
        )?;
        let started = retained_request.is_some();
        if self.stock.reconciliation_only && !started && purpose != ModelPurpose::Response {
            return Err(Error::Indeterminate(input.operation_id));
        }
        let request = self
            .resolve_model_request(
                journal,
                input,
                (step, prior_messages, purpose),
                retained_request,
            )
            .await?;
        let request_digest = request.request_digest();
        let purpose_key = match purpose {
            ModelPurpose::Response => "model",
            ModelPurpose::Summary => "summary",
        };
        self.record_model_phase(&admission, started);
        if admission.completed {
            crate::obs::obs_record!("items" = replayed.len());
            return Ok(replayed);
        }
        let dispatch = crate::model::ModelDispatch {
            operation_id: purpose.attempt_operation(input.operation_id, io)?,
            step,
            request_digest,
        };
        #[cfg(feature = "filesystem")]
        let _continuation_hold;
        let stream = if started {
            let continuation = self.reconcile_model_events(dispatch, &replayed).await?;
            let continuation = continuation.ok_or(Error::Indeterminate(input.operation_id))?;
            #[cfg(feature = "filesystem")]
            {
                _continuation_hold = io
                    .filter(|_| stock_is_native(io))
                    .map(|io| {
                        io.guard_owned(
                            continuation.capacity() as u64
                                * std::mem::size_of::<ModelEvent>() as u64,
                        )
                    })
                    .transpose()?;
            }
            Either::Left(futures::stream::iter(continuation.into_iter().map(Ok)))
        } else {
            #[cfg(feature = "filesystem")]
            {
                _continuation_hold = None;
            }
            self.claim_model_dispatch(
                journal,
                input,
                (step, purpose, purpose_key),
                &request,
                dispatch,
            )
            .await?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let generated = if let Some(work) = self.native_work() {
                Either::Right(
                    self.stock
                        .provider
                        .generate_with_native_work(&request, dispatch, work)?
                        .await?
                        .map(|result| self.admit_model_event(result)),
                )
            } else {
                Either::Left(self.stock.provider.generate(request, dispatch))
            };
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let generated = self.stock.provider.generate(request, dispatch);
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
        crate::obs::obs_record!("items" = observed.len());
        Ok(observed)
    }

    async fn resolve_model_request(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        turn: (u32, &[ModelMessage], ModelPurpose),
        retained_request: Option<crate::model::PreparedModelRequest>,
    ) -> Result<crate::model::PreparedModelRequest> {
        let (step, prior_messages, purpose) = turn;
        let request = match retained_request {
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
                self.prepared_response(journal, input, step, prior_messages)
                    .await?
            }
            None => self.prepare_request(input, step, prior_messages).await?,
        };
        self.verify_model_request_content(journal, &request).await?;
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
        dispatch: crate::model::ModelDispatch,
    ) -> Result<()> {
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
        let claim_key = with_history_io(io, |work| {
            preparation_format(
                format_args!("{purpose_key}:{step}:claim:{}", OperationId::new()),
                work,
            )
        })?;
        let reference = stage_bytes(
            journal,
            input.operation_id,
            &request_key,
            request.bytes(),
            true,
            io,
        )
        .await?;
        self.claim_task_model(dispatch).await?;
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
            Ok(false) | Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                return Err(Error::Indeterminate(input.operation_id));
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }

    async fn claim_task_model(&self, dispatch: crate::model::ModelDispatch) -> Result<()> {
        let step = dispatch.step;
        let request_digest = dispatch.request_digest;
        if let Some((host, task, fence)) = &self.stock.task {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(work) = self.native_work() {
                host.claim_model_dispatch_with_native_work(
                    *task,
                    dispatch.operation_id,
                    step,
                    request_digest,
                    fence,
                    work,
                )?
                .await?;
            } else {
                host.claim_model_dispatch(
                    *task,
                    dispatch.operation_id,
                    step,
                    request_digest,
                    fence.clone(),
                )
                .await?;
            }
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            host.claim_model_dispatch(
                *task,
                dispatch.operation_id,
                step,
                request_digest,
                fence.clone(),
            )
            .await?;
        }
        Ok(())
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
            while let Some(event) = stream.next().await {
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
                let reference = stage_json(journal, operation_id, &key, event, io).await?;
                journal
                    .append(
                        operation_id,
                        &key,
                        &ExecutionEvent::Model {
                            step,
                            purpose,
                            event: reference,
                        },
                        io,
                    )?
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
        self.verify_execution_owner().await
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
