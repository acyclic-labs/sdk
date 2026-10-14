use super::*;
use crate::wire::ErrorCode::Unsupported;
use futures::future::Either;

impl StockExecution<'_> {
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
        self.verify_execution_owner().await?;
        let (_, records) = self
            .model_records(journal, input.operation_id, step, purpose)
            .await?;
        let RetainedModelStep {
            request: retained_request,
            mut admission,
            events: replayed,
        } = retain_preparation(
            retained_model_step(journal, &records, step, purpose, self.stock.limits, io).await?,
        );
        let started = retained_request.is_some();
        if self.stock.reconciliation_only && !started && purpose != ModelPurpose::Response {
            return Err(Error::Indeterminate(input.operation_id));
        }
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
        let request_digest = request.request_digest();
        let purpose_key = match purpose {
            ModelPurpose::Response => "model",
            ModelPurpose::Summary => "summary",
        };
        crate::obs::obs_record!(
            "phase" = match (admission.completed, started) {
                (true, _) => "replay",
                (false, true) => "reconcile",
                (false, false) => "dispatch",
            }
        );
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
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let continuation = if let Some(work) = self.native_work() {
                let (continuation, custody) = self
                    .stock
                    .provider
                    .reconcile_with_native_work(dispatch, &replayed, work)?
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
                self.stock.provider.reconcile(dispatch, &replayed).await?
            };
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let continuation = self.stock.provider.reconcile(dispatch, &replayed).await?;
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
                io,
            )
            .await?;
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
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let generated = if let Some(work) = self.native_work() {
                Either::Right(
                    self.stock
                        .provider
                        .generate_with_native_work(&request, dispatch, work)?
                        .await?
                        .map(|result| {
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
                        }),
                )
            } else {
                Either::Left(self.stock.provider.generate(request, dispatch))
            };
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let generated = self.stock.provider.generate(request, dispatch);
            Either::Right(generated)
        };
        let mut observed = replayed;
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
                let reference = stage_json(journal, input.operation_id, &key, event, io).await?;
                journal
                    .append(
                        input.operation_id,
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
        crate::obs::obs_record!("items" = observed.len());
        Ok(observed)
    }
}
