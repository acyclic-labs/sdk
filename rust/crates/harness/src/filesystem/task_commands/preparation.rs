use super::*;
use crate::runtime::TaskSpawner as _;

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn prepare_fork_command(
        &self,
        task: TaskId,
        context: &WorkerContext,
        fence: &LeaseFence,
        command: &WorkflowCommand,
        payload: Value,
        retained_input: Value,
        reconciliation_only: bool,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        use crate::filesystem::task_runtime::observed::boundary;
        use crate::wire::ErrorCode;
        let request: crate::resource_preparation::ResourcePreparationRequest =
            mail::decode_command(retained_input, io)?;
        if request.command.as_ref() != Some(command) || request.payload != payload {
            return Err(boundary(
                io,
                ErrorCode::Unauthorized,
                "fork command differs from its retained preparation request",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let WorkerContext::Native(shared) = context else {
                return Err(boundary(
                    Some(io),
                    ErrorCode::Unauthorized,
                    "native fork requires its original shared context",
                ));
            };
            let work = io.original_native_work().ok_or_else(|| {
                boundary(
                    Some(io),
                    ErrorCode::Unsupported,
                    "native fork requires original work",
                )
            })?;
            let preparation = if reconciliation_only {
                let Some(preparation) = self
                    .runtime
                    .readonly_fork_preparation_with_native_work(task, shared, &request, work)
                    .await?
                else {
                    return Ok(TaskCommandProgress::Indeterminate);
                };
                preparation
            } else {
                let request_bytes = (std::mem::size_of::<
                    crate::resource_preparation::ResourcePreparationRequest,
                >() as u64)
                    .checked_add(request.logical_owned_bytes()?)
                    .ok_or_else(|| {
                        boundary(
                            Some(io),
                            ErrorCode::Invalid,
                            "fork preparation request ownership overflows",
                        )
                    })?;
                let request = (request, io.take_native_custody(request_bytes)?);
                let owner = self
                    .runtime
                    .task_host()
                    .journal_owner(task, fence.clone(), Some(&crate::filesystem::PreparationIo::from_native_work(work)?))
                    .await?
                    .into_native_shared(work)?;
                let mut preparation = owner
                    .resource_preparation_with_native_work(request, work)?
                    .await?;
                let mut header = work.claim(
                    std::mem::size_of::<crate::resource_preparation::TaskResourcePreparation>()
                        as u64,
                    1,
                    acyclic_fs::WorkCounters::UNCHARGED,
                )?;
                preparation
                    .native_custody
                    .as_mut()
                    .ok_or_else(|| {
                        boundary(
                            Some(io),
                            ErrorCode::Unauthorized,
                            "native preparation has no original metadata custody",
                        )
                    })?
                    .absorb(&mut header)
                    .map_err(|cause| Error::NativeWork {
                        cause,
                        custody: Some(header),
                    })?;
                Arc::new(preparation)
            };
            let fork: crate::fork::ForkRequest =
                mail::decode_borrowed_command(&preparation.request().payload, io)?;
            let source = self.runtime.recursive_source().ok_or_else(|| {
                boundary(
                    Some(io),
                    ErrorCode::Unsupported,
                    "fork preparation source is not configured",
                )
            })?;
            let retained = if reconciliation_only {
                source
                    .preparation_for_reconciliation_with_native_work(
                        shared,
                        &preparation,
                        io.cancellation(),
                        work,
                    )?
                    .await?
            } else {
                source
                    .preparation_with_native_work(shared, &preparation, io.cancellation(), work)?
                    .await?
            };
            io.absorb_native_custody(retained.1)?;
            if retained.0.request != fork {
                return Err(boundary(
                    Some(io),
                    ErrorCode::Unauthorized,
                    "resolved fork source differs from original request",
                ));
            }
            if reconciliation_only {
                let report = retained
                    .0
                    .preparer
                    .reconcile_for_task_with_native_work(&preparation, &fork, work)?
                    .await?;
                io.absorb_native_custody(report.1)?;
                return match report.0 {
                    Some(_) => mail::reconciled(Some(io)),
                    None => Ok(TaskCommandProgress::Indeterminate),
                };
            }
            let report = retained
                .0
                .preparer
                .prepare_for_task_with_native_work(&preparation, &fork, work)?
                .await?;
            io.absorb_native_custody(report.1)?;
            return ready(Outcome::Succeeded(report.0), Some(io));
        }
        let cancellation = acyclic_fs::CancellationToken::new();
        let _guard = crate::filesystem::default_tools::InvocationCancellation(cancellation.clone());
        let mut owner = self.runtime.task_host().journal_owner(task, fence.clone(), None).await?;
        owner.invocation_stop = Some(cancellation.clone());
        let preparation = Arc::new(owner)
            .resource_preparation(request, &mut None)
            .await?;
        let fork: crate::fork::ForkRequest =
            serde_json::from_value(preparation.request().payload.clone())
                .map_err(|cause| Error::Invalid(format!("invalid exact fork request: {cause}")))?;
        let preparer = if let Some(preparer) = &self.fork_preparer {
            preparer.clone()
        } else {
            let source = self.runtime.recursive_source().ok_or_else(|| {
                Error::Unsupported("fork preparation source is not configured".into())
            })?;
            let retained = source
                .preparation(context.context(), &preparation, &cancellation)
                .await?;
            if retained.request != fork {
                return Err(Error::Unauthorized(
                    "resolved fork source differs from original request".into(),
                ));
            }
            retained.preparer
        };
        ready(
            Outcome::Succeeded(
                preparer
                    .prepare_for_task(fork, &Arc::new(preparation))
                    .await?,
            ),
            None,
        )
    }

    pub(super) async fn admit(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        command: OperationId,
        admission: crate::runtime::TaskAdmissionRecord,
        reconciliation_only: bool,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        use crate::filesystem::task_runtime::observed::boundary;
        use crate::wire::ErrorCode;
        if admission.parent != Some(task)
            || admission.operation_id.into_bytes() == task.into_bytes()
            || admission.operation_id == command
        {
            return Err(boundary(
                io,
                ErrorCode::Unauthorized,
                "prepared child does not belong to this calling task",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let work = io
            .map(|io| {
                io.original_native_work().ok_or_else(|| {
                    boundary(
                        Some(io),
                        ErrorCode::Unsupported,
                        "native child requires original work",
                    )
                })
            })
            .transpose()?;
        if reconciliation_only {
            let retained = 'retained: {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(work) = work {
                    let (retained, custody) =
                        crate::runtime::TaskSpawner::reconcile_admission_with_native_work(
                            self.runtime.task_host().as_ref(),
                            admission.operation_id,
                            work,
                        )?
                        .await?;
                    io.expect("native child has observer")
                        .absorb_native_custody(custody)?;
                    break 'retained retained;
                }
                break 'retained self
                    .runtime
                    .task_host()
                    .reconcile_admission(admission.operation_id)
                    .await?;
            };
            if let Some((_, retained)) = retained
                && retained != admission
            {
                return Err(boundary(
                    io,
                    ErrorCode::Conflict,
                    "retained child admission differs",
                ));
            }
            return mail::reconciled(io);
        }
        let result = 'admission: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = work {
                let io = io.expect("native child has observer");
                let bytes = (std::mem::size_of::<crate::runtime::TaskAdmissionRecord>() as u64)
                    .checked_add(admission.logical_owned_bytes()?)
                    .ok_or_else(|| {
                        boundary(
                            Some(io),
                            ErrorCode::Invalid,
                            "prepared admission custody overflows",
                        )
                    })?;
                let admission = (admission, io.take_native_custody(bytes)?);
                let fence = fence.clone_with_native_work(work)?;
                let (result, custody) = crate::runtime::TaskSpawner::admit_owned_with_native_work(
                    self.runtime.task_host().as_ref(),
                    admission,
                    fence,
                    work,
                )?
                .await?;
                io.absorb_native_custody(custody)?;
                break 'admission result;
            }
            break 'admission self
                .runtime
                .task_host()
                .admit_owned(admission, fence.clone())
                .await?;
        };
        match result {
            crate::Admission::Accepted(child) => ready(Outcome::Succeeded(child), io),
            crate::Admission::Rejected { reason } => {
                ready(Outcome::<Value>::Failed { message: reason }, io)
            }
            crate::Admission::Indeterminate { .. } => Ok(TaskCommandProgress::Indeterminate),
        }
    }

    pub(super) async fn task_command(
        &self,
        task: TaskId,
        context: &TaskContext,
        fence: &LeaseFence,
        command: &WorkflowCommand,
        payload: Value,
        reconciliation_only: bool,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        use crate::filesystem::task_runtime::observed::boundary;
        use crate::wire::ErrorCode;
        if command.kind == TASK_ADMIT_COMMAND_KIND {
            let input: TaskAdmitCommand = mail::decode_command(payload, io)?;
            let operation = child_operation(task, command.operation_id, io)?;
            let admission = 'admission: {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(io) = io {
                    let work = io.original_native_work().ok_or_else(|| {
                        boundary(
                            Some(io),
                            ErrorCode::Unsupported,
                            "native child requires original work",
                        )
                    })?;
                    let input_bytes = (std::mem::size_of::<Value>() as u64)
                        .checked_add(crate::contract::json_value_logical_owned_bytes(&input.input)?)
                        .ok_or_else(|| {
                            boundary(
                                Some(io),
                                ErrorCode::Invalid,
                                "child input ownership overflow",
                            )
                        })?;
                    let input_custody = io.take_native_custody(input_bytes)?;
                    let (admission, custody) = self
                        .runtime
                        .harness()
                        .registered_child_admission_with_native_work(
                            operation,
                            &input.name,
                            &input.version,
                            (input.input, input_custody),
                            task,
                            context.scope(),
                            work,
                        )?;
                    io.absorb_native_custody(custody)?;
                    break 'admission admission;
                }
                break 'admission self.runtime.harness().registered_child_admission(
                    operation,
                    &input.name,
                    &input.version,
                    input.input,
                    task,
                    context.scope(),
                )?;
            };
            self.admit(
                task,
                fence,
                command.operation_id,
                admission,
                reconciliation_only,
                io,
            )
            .await
        } else if reconciliation_only {
            mail::reconciled(io)
        } else {
            let input: TaskObserveCommand = mail::decode_command(payload, io)?;
            let child = TaskId::from_bytes(
                child_operation(task, input.admission_operation, io)?.into_bytes(),
            );
            if self.runtime.admission_observed(child, io).await?.parent != Some(task) {
                return Err(boundary(
                    io,
                    ErrorCode::Unauthorized,
                    "observed task is not this command's direct child",
                ));
            }
            match self.runtime.outcome_observed(child, io).await? {
                None | Some(Outcome::Indeterminate { .. }) => Ok(TaskCommandProgress::Pending),
                Some(outcome) => ready(outcome, io),
            }
        }
    }
    pub(super) async fn cancel_child_command(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        command: &WorkflowCommand,
        payload: Value,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        let input: TaskCancelCommand = mail::decode_command(payload, io)?;
        let mut observer = io.map(crate::filesystem::PreparationIo::history_io);
        let mut work = observer
            .as_mut()
            .map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        let state = self
            .runtime
            .task_host()
            .cancel_child_owned(task, fence, input.child, command.operation_id, &mut work)
            .await?;
        ready(Outcome::Succeeded(state), io)
    }
}
