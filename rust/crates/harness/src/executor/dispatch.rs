use super::*;

impl StockExecution<'_> {
    /// Replays or resolves one model step.
    #[allow(clippy::too_many_lines, reason = "atomic model admission and recovery")]
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
        let is_native = if let Some(work) = self.native_work() {
            work.remaining()?;
            if !self.stock.provider.supports_native_model(work)? {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "model provider has no original native-ledger producer",
                });
            }
            true
        } else {
            false
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let is_native = false;
        self.verify_execution_owner().await?;
        let (_, records) = self
            .model_records(journal, input.operation_id, step, purpose)
            .await?;
        let RetainedModelStep {
            request: retained_request,
            mut admission,
            events: replayed_model,
        } = retained_model_step(journal, &records, step, purpose, self.stock.limits, io).await?;
        let started = retained_request.is_some();
        if self.stock.reconciliation_only && !started && purpose != ModelPurpose::Response {
            return Err(Error::Indeterminate(input.operation_id));
        }
        let request = if let Some(recorded) = retained_request {
            if purpose == ModelPurpose::Summary {
                let current = self.prepare_request(input, step, prior_messages).await?;
                if current.request_digest() != recorded.request_digest() {
                    return Err(Error::Conflict(
                        "summary source or instruction differs from retained admission".into(),
                    ));
                }
            }
            recorded
        } else {
            match purpose {
                ModelPurpose::Response => {
                    self.prepared_response(journal, input, step, prior_messages)
                        .await?
                }
                ModelPurpose::Summary => self.prepare_request(input, step, prior_messages).await?,
            }
        };
        self.verify_model_request_content(journal, &request).await?;
        let request_digest = request.request_digest();
        let purpose_key = match purpose {
            ModelPurpose::Response => "model",
            ModelPurpose::Summary => "summary",
        };
        let replay_completed = admission.completed;
        crate::obs::obs_record!(
            "phase" = match (replay_completed, started) {
                (true, _) => "replay",
                (false, true) => "reconcile",
                (false, false) => "dispatch",
            }
        );
        let model_events = if replay_completed {
            replayed_model
        } else if started {
            let dispatch = crate::model::ModelDispatch {
                operation_id: purpose.attempt_operation(input.operation_id, self.io)?,
                step,
                request_digest,
            };
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let continuation = if let Some(work) = self.native_work() {
                let (continuation, custody) = self
                    .stock
                    .provider
                    .reconcile_with_native_work(dispatch, &replayed_model, work)?
                    .await?;
                io.expect("native stock session retains its original IO")
                    .absorb_native_custody(custody)?;
                continuation
            } else {
                self.stock
                    .provider
                    .reconcile(dispatch, &replayed_model)
                    .await?
            };
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let continuation = self
                .stock
                .provider
                .reconcile(dispatch, &replayed_model)
                .await?;
            let Some(mut continuation) = continuation else {
                return Err(Error::Indeterminate(input.operation_id));
            };
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let continuation_bytes =
                continuation.capacity() as u64 * std::mem::size_of::<ModelEvent>() as u64;
            let mut observed = replayed_model;
            for event in continuation.drain(..) {
                let index = observed.len();
                let key = with_history_io(io, |work| {
                    crate::contract::preparation_format(
                        format_args!("{purpose_key}:{step}:{index}"),
                        work,
                    )
                })?;
                stock_vec_push(&mut observed, event, io)?;
                let event = &observed[index];
                admission.observe(event, self.stock.limits, io)?;
                let reference = stage_json(journal, input.operation_id, &key, event, io).await?;
                stock_append(
                    journal,
                    input.operation_id,
                    &key,
                    &ExecutionEvent::Model {
                        step,
                        purpose,
                        event: reference,
                    },
                    io,
                )
                .await?;
            }
            drop(continuation);
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if is_native {
                io.expect("native stock session retains its original IO")
                    .release(continuation_bytes)?;
            }
            observed
        } else {
            if self.stock.reconciliation_only {
                return Err(Error::Indeterminate(input.operation_id));
            }
            let (tail, current) = self
                .model_records(journal, input.operation_id, step, purpose)
                .await?;
            if let Some(existing) = current.iter().find_map(|record| match &record.event {
                ExecutionEvent::ModelStarted {
                    step: event_step,
                    request_digest,
                    ..
                } if *event_step == step => Some(*request_digest),
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
                crate::contract::preparation_format(
                    format_args!("{purpose_key}:{step}:request"),
                    work,
                )
            })?;
            let claim_key = with_history_io(io, |work| {
                crate::contract::preparation_format(
                    format_args!("{purpose_key}:{step}:claim:{}", OperationId::new()),
                    work,
                )
            })?;
            let request_ref = stage_bytes(
                journal,
                input.operation_id,
                &request_key,
                request.bytes(),
                io,
            )
            .await?;
            if let Some((host, task_id, fence)) = &self.stock.task {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if let Some(work) = self.native_work() {
                    host.claim_model_dispatch_with_native_work(
                        *task_id,
                        purpose.attempt_operation(input.operation_id, self.io)?,
                        step,
                        request_digest,
                        fence,
                        work,
                    )?
                    .await?;
                } else {
                    host.claim_model_dispatch(
                        *task_id,
                        purpose.attempt_operation(input.operation_id, self.io)?,
                        step,
                        request_digest,
                        fence.clone(),
                    )
                    .await?;
                }
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                host.claim_model_dispatch(
                    *task_id,
                    purpose.attempt_operation(input.operation_id, self.io)?,
                    step,
                    request_digest,
                    fence.clone(),
                )
                .await?;
            }
            let claimed = stock_append_if_tail(
                journal,
                input.operation_id,
                tail,
                &claim_key,
                &ExecutionEvent::ModelStarted {
                    step,
                    purpose,
                    request_digest,
                    request: request_ref,
                },
                io,
            )
            .await;
            match claimed {
                Ok(true) => {}
                Err(error) if is_native => return Err(error),
                Ok(false) | Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return Err(Error::Indeterminate(input.operation_id));
                }
                Err(error) => return Err(error),
            }
            let dispatch = crate::model::ModelDispatch {
                operation_id: purpose.attempt_operation(input.operation_id, self.io)?,
                step,
                request_digest,
            };
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let mut stream = if let Some(work) = self.native_work() {
                futures::future::Either::Right(
                    self.stock
                        .provider
                        .generate_with_native_work(&request, dispatch, work)?
                        .await?
                        .map(|result| {
                            let (event, custody) = result?;
                            io.expect("native stock session retains its original IO")
                                .absorb_native_custody(custody)?;
                            Ok(event)
                        }),
                )
            } else {
                futures::future::Either::Left(self.stock.provider.generate(request, dispatch))
            };
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let mut stream = self.stock.provider.generate(request, dispatch);
            let mut observed = Vec::new();
            while let Some(event) = stream.next().await {
                let event = event?;
                let index = observed.len();
                let key = with_history_io(io, |work| {
                    crate::contract::preparation_format(
                        format_args!("{purpose_key}:{step}:{index}"),
                        work,
                    )
                })?;
                stock_vec_push(&mut observed, event, io)?;
                let event = &observed[index];
                admission.observe(event, self.stock.limits, io)?;
                let reference = stage_json(journal, input.operation_id, &key, event, io).await?;
                stock_append(
                    journal,
                    input.operation_id,
                    &key,
                    &ExecutionEvent::Model {
                        step,
                        purpose,
                        event: reference,
                    },
                    io,
                )
                .await?;
            }
            observed
        };
        crate::obs::obs_record!("items" = model_events.len());
        Ok(model_events)
    }
}
