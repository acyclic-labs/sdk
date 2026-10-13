use super::*;
use crate::filesystem::{PreparationIo, task_runtime::observed::boundary};
use crate::resource_preparation::{ResourcePreparationRequest, TaskResourcePreparation};
use crate::runtime::{TaskAdmissionRecord, TaskSpawner as _};
use crate::wire::ErrorCode;

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
        io: Option<&PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        let request: ResourcePreparationRequest = mail::decode_command(retained_input, io)?;
        if request.command.as_ref() != Some(command) || request.payload != payload {
            return Err(boundary(
                io,
                ErrorCode::Unauthorized,
                "fork command differs from its retained preparation request",
            ));
        }
        let cancellation = io.map(|io| io.cancellation().clone()).unwrap_or_default();
        let _cancel = io.is_none().then(|| {
            crate::filesystem::default_tools::InvocationCancellation(cancellation.clone())
        });
        let preparation: Arc<TaskResourcePreparation> = 'preparation: {
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
                if reconciliation_only {
                    let Some(preparation) = self
                        .runtime
                        .readonly_fork_preparation_with_native_work(task, shared, &request, work)
                        .await?
                    else {
                        return Ok(TaskCommandProgress::Indeterminate);
                    };
                    break 'preparation preparation;
                }
                let bytes = (std::mem::size_of::<ResourcePreparationRequest>() as u64)
                    .checked_add(request.logical_owned_bytes()?)
                    .ok_or_else(|| {
                        boundary(
                            Some(io),
                            ErrorCode::Invalid,
                            "fork preparation request ownership overflows",
                        )
                    })?;
                let custody = io.take_native_custody(bytes)?;
                let owner = self
                    .runtime
                    .task_host()
                    .journal_owner(task, fence, Some(&crate::executor::PreparationIo::from_native_work(work)?))
                    .await?
                    .into_native_shared(work)?;
                let preparation = owner
                    .resource_preparation_with_native_work((request, custody), work)?
                    .await?;
                break 'preparation Arc::new(preparation);
            }
            let owner = self
                .runtime
                .task_host()
                .journal_owner(task, fence.clone())
                .await?
                .with_invocation_cancellation(&cancellation);
            Arc::new(
                Arc::new(owner)
                    .resource_preparation(request, &mut None)
                    .await?,
            )
        };
        let fork: crate::fork::ForkRequest =
            match io {
                Some(io) => {
                    #[cfg(not(target_arch = "wasm32"))]
                    io.check_json_byte_bound(
                        &preparation.request().payload,
                        acyclic_stream::MAX_RECORD_BYTES as u64,
                    )?;
                    crate::filesystem::retain_decoded(io.value_decode_preparation(
                        &preparation.request().payload,
                        ErrorCode::Storage,
                    )?)
                }
                None => mail::decode_command(preparation.request().payload.clone(), None)?,
            };
        let preparer = 'preparer: {
            if io.is_none()
                && let Some(preparer) = &self.fork_preparer
            {
                break 'preparer Arc::clone(preparer);
            }
            let source = self.runtime.recursive_source().ok_or_else(|| {
                boundary(
                    io,
                    ErrorCode::Unsupported,
                    "fork preparation source is not configured",
                )
            })?;
            let retained = 'source: {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(io) = io {
                    let WorkerContext::Native(shared) = context else {
                        unreachable!("preparation checked native context")
                    };
                    let work = io
                        .original_native_work()
                        .expect("preparation checked original work");
                    let (retained, custody) = if reconciliation_only {
                        source
                            .preparation_for_reconciliation_with_native_work(
                                shared,
                                &preparation,
                                &cancellation,
                                work,
                            )?
                            .await?
                    } else {
                        source
                            .preparation_with_native_work(
                                shared,
                                &preparation,
                                &cancellation,
                                work,
                            )?
                            .await?
                    };
                    io.absorb_native_custody(custody)?;
                    break 'source retained;
                }
                source
                    .preparation(context.context(), &preparation, &cancellation)
                    .await?
            };
            if retained.request != fork {
                return Err(boundary(
                    io,
                    ErrorCode::Unauthorized,
                    "resolved fork source differs from original request",
                ));
            }
            retained.preparer
        };
        let report = 'report: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(io) = io {
                let work = io
                    .original_native_work()
                    .expect("preparation checked original work");
                if reconciliation_only {
                    let (report, custody) = preparer
                        .reconcile_for_task_with_native_work(&preparation, &fork, work)?
                        .await?;
                    io.absorb_native_custody(custody)?;
                    return if report.is_some() {
                        mail::reconciled(Some(io))
                    } else {
                        Ok(TaskCommandProgress::Indeterminate)
                    };
                }
                let (report, custody) = preparer
                    .prepare_for_task_with_native_work(&preparation, &fork, work)?
                    .await?;
                io.absorb_native_custody(custody)?;
                break 'report report;
            }
            preparer.prepare_for_task(fork, &preparation).await?
        };
        ready(Outcome::Succeeded(report), io)
    }

    pub(super) async fn admit(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        command: OperationId,
        admission: TaskAdmissionRecord,
        reconciliation_only: bool,
        io: Option<&PreparationIo>,
    ) -> Result<TaskCommandProgress> {
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
                    let (retained, custody) = self
                        .runtime
                        .task_host()
                        .reconcile_admission_with_native_work(admission.operation_id, work)?
                        .await?;
                    io.expect("native child has observer")
                        .absorb_native_custody(custody)?;
                    break 'retained retained;
                }
                self.runtime
                    .task_host()
                    .reconcile_admission(admission.operation_id)
                    .await?
            };
            if retained.is_some_and(|(_, retained)| retained != admission) {
                return Err(boundary(
                    io,
                    ErrorCode::Conflict,
                    "retained child admission differs",
                ));
            }
            return mail::reconciled(io);
        }
        let result = 'admit: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = work {
                let io = io.expect("native child has observer");
                let bytes = (std::mem::size_of::<TaskAdmissionRecord>() as u64)
                    .checked_add(admission.logical_owned_bytes()?)
                    .ok_or_else(|| {
                        boundary(
                            Some(io),
                            ErrorCode::Invalid,
                            "prepared admission custody overflows",
                        )
                    })?;
                let custody = io.take_native_custody(bytes)?;
                let fence = fence.clone_with_native_work(work)?;
                let (result, custody) = self
                    .runtime
                    .task_host()
                    .admit_owned_with_native_work((admission, custody), fence, work)?
                    .await?;
                io.absorb_native_custody(custody)?;
                break 'admit result;
            }
            self.runtime
                .task_host()
                .admit_owned(admission, fence.clone())
                .await?
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
        io: Option<&PreparationIo>,
    ) -> Result<TaskCommandProgress> {
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
                        .checked_add(crate::executor::json::json_value_logical_owned_bytes(
                            &input.input,
                        )?)
                        .ok_or_else(|| {
                            boundary(
                                Some(io),
                                ErrorCode::Invalid,
                                "child input ownership overflow",
                            )
                        })?;
                    let custody = io.take_native_custody(input_bytes)?;
                    let (admission, custody) = self
                        .runtime
                        .harness()
                        .registered_child_admission_with_native_work(
                            operation,
                            &input.name,
                            &input.version,
                            (input.input, custody),
                            task,
                            context.scope(),
                            work,
                        )?;
                    io.absorb_native_custody(custody)?;
                    break 'admission admission;
                }
                self.runtime.harness().registered_child_admission(
                    operation,
                    &input.name,
                    &input.version,
                    input.input,
                    task,
                    context.scope(),
                )?
            };
            return self
                .admit(
                    task,
                    fence,
                    command.operation_id,
                    admission,
                    reconciliation_only,
                    io,
                )
                .await;
        }
        if reconciliation_only {
            return mail::reconciled(io);
        }
        let input: TaskObserveCommand = mail::decode_command(payload, io)?;
        let child =
            TaskId::from_bytes(child_operation(task, input.admission_operation, io)?.into_bytes());
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

    pub(super) async fn cancel_child_command(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        command: &WorkflowCommand,
        payload: Value,
        io: Option<&PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        let input: TaskCancelCommand = mail::decode_command(payload, io)?;
        let mut observer = io.map(PreparationIo::history_io);
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
