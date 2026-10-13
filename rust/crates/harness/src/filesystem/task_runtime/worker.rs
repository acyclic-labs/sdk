//! Worker for the filesystem task runtime.

use super::*;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Drives an already-claimed lease through the registered machine.
    pub async fn run_task(
        &self,
        lease: crate::distributed::WorkLease,
        commands: &dyn TaskCommandHost,
        maximum_transitions: u32,
    ) -> Result<TaskWorkerOutcome> {
        let progress = self
            .run_task_observed(&lease, commands, maximum_transitions, None)
            .await?;
        Ok(progress.with_lease(lease))
    }

    pub(super) async fn run_task_observed(
        &self,
        lease: &crate::distributed::WorkLease,
        commands: &dyn TaskCommandHost,
        maximum_transitions: u32,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<WorkerTurn> {
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        if maximum_transitions == 0 {
            #[cfg(not(target_arch = "wasm32"))]
            if io.is_some() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "invalid worker transition allowance",
                });
            }
            return Err(Error::Invalid("invalid worker transition allowance".into()));
        }
        let operation_id = lease.operation.operation_id;
        let task = TaskId::from_bytes(operation_id.into_bytes());
        let admission = self.admission_observed(task, io).await?;
        let fence = observed::lease_fence(lease, io)?;
        let run = async {
            let fence = &fence;
            #[cfg(not(target_arch = "wasm32"))]
            let context = if let Some(io) = io {
                let work = io.original_native_work().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "worker context requires original native work",
                })?;
                WorkerContext::Native(
                    self.task_context_with_native_work(task, fence, work)
                        .await?,
                )
            } else {
                WorkerContext::Ordinary(self.harness.durable_context(task, operation_id).await?)
            };
            #[cfg(target_arch = "wasm32")]
            let context =
                WorkerContext::Ordinary(self.harness.durable_context(task, operation_id).await?);
            #[cfg(not(target_arch = "wasm32"))]
            let mut session = if let WorkerContext::Native(context) = &context {
                let work =
                    io.and_then(|io| io.original_native_work())
                        .ok_or(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Unsupported,
                            reason: "native worker lost its original observer",
                        })?;
                self.host.start_task(lease, Some(work)).await?;
                self.open_task_with_native_work(task, fence, Arc::clone(context), work)
                    .await?
            } else {
                self.host.start_task(lease, None).await?;
                self.open_task(task, fence.clone()).await?
            };
            #[cfg(target_arch = "wasm32")]
            let mut session = {
                self.host.start_task(lease, None).await?;
                self.open_task(task, fence.clone()).await?
            };
            #[cfg(not(target_arch = "wasm32"))]
            let mut wake_custody;
            #[cfg(not(target_arch = "wasm32"))]
            let mut wake = if let Some(io) = io {
                let work = io.original_native_work().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "native wake input requires original observer",
                })?;
                let (wake, custody) = self
                    .host
                    .workflow_input_with_native_work(task, fence, work)
                    .await?;
                wake_custody = Some(custody);
                wake
            } else {
                wake_custody = None;
                self.host.workflow_input(task, fence).await?
            };
            #[cfg(target_arch = "wasm32")]
            let mut wake = self.host.workflow_input(task, fence).await?;
            let mut advanced = 0;
            loop {
                let revision = session.checkpoint().revision;
                #[cfg(not(target_arch = "wasm32"))]
                let _transition_custody;
                #[cfg(not(target_arch = "wasm32"))]
                let mut transition = if let Some(io) = io {
                    let work = io.original_native_work().ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unsupported,
                        reason: "native transition requires original observer",
                    })?;
                    let (value, custody) = session.latest_transition_with_native_work(work).await?;
                    _transition_custody = Some(custody);
                    value
                } else {
                    _transition_custody = None;
                    session.latest_transition().await?
                };
                #[cfg(target_arch = "wasm32")]
                let mut transition = session.latest_transition().await?;
                if advanced == maximum_transitions
                    && transition.as_ref().is_none_or(|value| {
                        matches!(value.status, MachineStatus::Suspended)
                            && !value.commands.is_empty()
                    })
                {
                    return Ok(WorkerTurn::Yielded);
                }
                let mut command_input = None;
                if let Some(transition) = &mut transition {
                    if !transition.commands.is_empty() {
                        match self
                            .dispatch_commands(
                                task,
                                &context,
                                fence,
                                &transition.commands,
                                commands,
                                io,
                            )
                            .await?
                        {
                            ready @ CommandDispatch::Ready { .. } => command_input = Some(ready),
                            CommandDispatch::Pending(index) => {
                                self.suspend_command(
                                    task,
                                    fence,
                                    revision,
                                    &transition.commands[index],
                                    io,
                                )
                                .await?;
                                return Ok(WorkerTurn::Suspended { task, revision });
                            }
                            CommandDispatch::Indeterminate => {
                                self.task_host()
                                    .settle_task(
                                        task,
                                        fence,
                                        Outcome::Indeterminate { operation_id },
                                        &mut observed::history(io)?
                                            .as_mut()
                                            .map(|work| work as &mut dyn crate::store::HistoryIo),
                                    )
                                    .await?;
                                return Ok(WorkerTurn::Reconciling);
                            }
                        }
                    }
                    let outcome = match &mut transition.status {
                        MachineStatus::Completed { value } => {
                            Some(Outcome::Succeeded(std::mem::take(value)))
                        }
                        MachineStatus::Failed { message } => Some(Outcome::Failed {
                            message: std::mem::take(message),
                        }),
                        MachineStatus::Suspended => None,
                    };
                    if let Some(outcome) = outcome {
                        self.task_host()
                            .settle_task(
                                task,
                                fence,
                                outcome,
                                &mut observed::history(io)?
                                    .as_mut()
                                    .map(|work| work as &mut dyn crate::store::HistoryIo),
                            )
                            .await?;
                        return Ok(WorkerTurn::Completed { task });
                    }
                }
                #[cfg(not(target_arch = "wasm32"))]
                let mut wake_input_used = false;
                let input = if transition.is_none() {
                    Value::Null
                } else if let Some(CommandDispatch::Ready { value, .. }) = command_input.as_mut() {
                    std::mem::take(value)
                } else if let Some((wake_revision, value)) = &mut wake {
                    if *wake_revision == revision {
                        #[cfg(not(target_arch = "wasm32"))]
                        {
                            wake_input_used = true;
                        }
                        std::mem::take(value)
                    } else {
                        self.task_host()
                            .suspend_workflow(
                                task,
                                fence,
                                revision,
                                None,
                                None,
                                None,
                                &mut observed::history(io)?
                                    .as_mut()
                                    .map(|work| work as &mut dyn crate::store::HistoryIo),
                            )
                            .await?;
                        return Ok(WorkerTurn::Suspended { task, revision });
                    }
                } else {
                    self.task_host()
                        .suspend_workflow(
                            task,
                            fence,
                            revision,
                            None,
                            None,
                            None,
                            &mut observed::history(io)?
                                .as_mut()
                                .map(|work| work as &mut dyn crate::store::HistoryIo),
                        )
                        .await?;
                    return Ok(WorkerTurn::Suspended { task, revision });
                };
                if advanced == maximum_transitions {
                    return Ok(WorkerTurn::Yielded);
                }
                if let Some(io) = io {
                    let hashed = acyclic_fs::WorkCounters {
                        bytes_hashed: b"harness/v2/task-worker-step\0".len() as u64 + 24,
                        ..Default::default()
                    };
                    io.preflight(hashed)?;
                    io.charge(hashed)?;
                }
                let mut digest = blake3::Hasher::new();
                digest.update(b"harness/v2/task-worker-step\0");
                digest.update(&task.into_bytes());
                digest.update(&revision.to_be_bytes());
                let mut bytes = [0; 16];
                bytes.copy_from_slice(&digest.finalize().as_bytes()[..16]);
                #[cfg(not(target_arch = "wasm32"))]
                let native_input = if let Some(CommandDispatch::Ready {
                    custody: Some(primary),
                    ..
                }) = command_input.as_mut()
                {
                    let key = step_key(revision, primary)?;
                    Some(key)
                } else {
                    None
                };
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(key) = native_input {
                    let Some(CommandDispatch::Ready { custody, .. }) = command_input.as_mut()
                    else {
                        unreachable!("native key requires retained command custody");
                    };
                    let custody = custody.take().ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unsupported,
                        reason: "native input has no original command custody",
                    })?;
                    session
                        .step_with_native_work(OperationId::from_bytes(bytes), key, input, custody)
                        .await?;
                } else if let Some(io) = io {
                    let work = io.original_native_work().ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unsupported,
                        reason: "native step requires original observer",
                    })?;
                    let mut custody = if wake_input_used {
                        wake_custody.take().ok_or(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Invalid,
                            reason: "native wake input has no original custody",
                        })?
                    } else {
                        io.take_native_custody(0)?
                    };
                    if !work.shares_ledger(custody.work()) {
                        drop(input);
                        return Err(Error::NativeWork {
                            cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                            custody: Some(custody),
                        });
                    }
                    let key = match step_key(revision, &mut custody) {
                        Ok(key) => key,
                        Err(error) => {
                            drop(input);
                            return Err(error);
                        }
                    };
                    session
                        .step_with_native_work(OperationId::from_bytes(bytes), key, input, custody)
                        .await?;
                } else {
                    session
                        .step(
                            OperationId::from_bytes(bytes),
                            IdempotencyKey::new(format!("task-step:{revision}"))?,
                            input,
                        )
                        .await?;
                }
                #[cfg(target_arch = "wasm32")]
                session
                    .step(
                        OperationId::from_bytes(bytes),
                        IdempotencyKey::new(format!("task-step:{revision}"))?,
                        input,
                    )
                    .await?;
                advanced += 1;
            }
        };
        deadline::run(
            &self.host,
            task,
            Some(&fence),
            admission.run_limits.deadline_epoch_ms,
            operation_id,
            run,
            io,
            |_| false,
        )
        .await
    }
}
