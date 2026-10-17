use super::*;
use crate::filesystem::{PreparationIo, task_runtime::observed::boundary};
use crate::resource_preparation::{ResourcePreparationRequest, TaskResourcePreparation};
use crate::runtime::TaskAdmissionRecord;
use crate::wire::ErrorCode;

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn prepare_fork_command(
        &self,
        CommandInvocation {
            context,
            admission,
            fence,
            command,
            reconciliation_only,
            io,
        }: CommandInvocation<'_>,
        payload: Value,
    ) -> Result<TaskCommandProgress> {
        let task = admission.task_id();
        let retained_input = crate::model::clone_value(&admission.input, io)?;
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
            if let Some(work) = io.and_then(PreparationIo::original_native_work)
                && reconciliation_only
            {
                let Some(preparation) = self
                    .runtime
                    .readonly_fork_preparation_with_native_work(task, context, &request, work)
                    .await?
                else {
                    return Ok(TaskCommandProgress::Indeterminate);
                };
                break 'preparation preparation;
            }
            let owner = self
                .runtime
                .task_host()
                .journal_owner(task, fence, io)
                .await?
                .with_invocation_cancellation(&cancellation);
            #[cfg(not(target_arch = "wasm32"))]
            let owner = match io.and_then(PreparationIo::original_native_work) {
                Some(work) => owner.into_native_shared(work)?,
                None => Arc::new(owner),
            };
            #[cfg(target_arch = "wasm32")]
            let owner = Arc::new(owner);
            let mut history = io.map(PreparationIo::history_io);
            let preparation = owner
                .resource_preparation(
                    request,
                    &mut history
                        .as_mut()
                        .map(|work| work as &mut dyn crate::store::HistoryIo),
                )
                .await?;
            #[cfg(not(target_arch = "wasm32"))]
            let mut preparation = preparation;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(io) = io {
                preparation.native_custody = Some(io.take_native_custody(io.owned_bytes()?)?);
            }
            break 'preparation Arc::new(preparation);
        };
        let source = if let Some(preparer) = self.fork_preparer.as_ref().filter(|_| io.is_none()) {
            super::super::RetainedSourceBinding {
                preparer: Arc::clone(preparer),
                request: mail::decode_command(preparation.request().payload.clone(), None)?,
            }
        } else {
            self.runtime
                .recursive_source()
                .ok_or_else(|| {
                    boundary(
                        io,
                        ErrorCode::Unsupported,
                        "fork preparation source is not configured",
                    )
                })?
                .preparation(
                    context,
                    &preparation,
                    &cancellation,
                    reconciliation_only,
                    io,
                )
                .await?
        };
        let super::super::RetainedSourceBinding {
            preparer,
            request: fork,
        } = source;
        if reconciliation_only && io.is_some() {
            let report = preparer.reconcile_for_task(&fork, &preparation, io).await?;
            return if report.is_some() {
                mail::reconciled(io)
            } else {
                Ok(TaskCommandProgress::Indeterminate)
            };
        }
        let report = preparer.prepare_for_task(&fork, &preparation, io).await?;
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
                io.original_native_work()
                    .ok_or_else(|| io.invalid("dispatch requires original native work"))
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
                    io.ok_or_else(|| {
                        crate::executor::stock_boundary(
                            io,
                            crate::wire::ErrorCode::Invalid,
                            "native child has observer",
                        )
                    })?
                    .absorb_native_custody(custody)?;
                    break 'retained retained;
                }
                break 'retained self
                    .runtime
                    .task_host()
                    .reconcile_admission(admission.operation_id)
                    .await?;
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
                let io = io.ok_or_else(|| {
                    crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "native child has observer",
                    )
                })?;
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
            break 'admit self
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
        CommandInvocation {
            context,
            admission,
            fence,
            command,
            reconciliation_only,
            io,
        }: CommandInvocation<'_>,
        payload: Value,
    ) -> Result<TaskCommandProgress> {
        let task = admission.task_id();
        if command.kind == TASK_ADMIT_COMMAND_KIND {
            let mut input: TaskAdmitCommand = mail::decode_command(payload, io)?;
            let operation = super::super::recursive::derive_operation(
                b"harness/v2/task-child\0",
                &[task.into_bytes(), command.operation_id.into_bytes()],
                io,
            )?;
            if let Err(error) = self
                .runtime
                .stage_initial_command(context, operation, &mut input, io)
                .await
            {
                if let Error::Invalid(message) = error {
                    return ready(Outcome::<Value>::Failed { message }, io);
                }
                return query::failure(error, io);
            }
            let admission = 'admission: {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(io) = io {
                    let _work = io
                        .original_native_work()
                        .ok_or_else(|| io.invalid("dispatch requires original native work"))?;
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
                    io.absorb_native_custody(custody)?;
                    let (admission, _) = self.runtime.harness().registered_admission(
                        operation,
                        &input.name,
                        &input.version,
                        input.input,
                        Some((task, context.scope())),
                        None,
                        Some(io),
                    )?;
                    break 'admission admission;
                }
                break 'admission match self.runtime.harness().registered_admission(
                    operation,
                    &input.name,
                    &input.version,
                    input.input,
                    Some((task, context.scope())),
                    None,
                    None,
                ) {
                    Ok((admission, _)) => admission,
                    // Registration is synchronous validation: no admission,
                    // allocation or effect has been attempted at this point.
                    Err(error) => {
                        return ready(
                            Outcome::<Value>::Failed {
                                message: error.to_string(),
                            },
                            None,
                        );
                    }
                }
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
        if command.kind == TASK_INSPECT_COMMAND_KIND {
            let input: TaskInspectCommand = mail::decode_command(payload, io)?;
            if !context.scope().grants().contains("operation:observe") {
                return query::failure(
                    boundary(io, ErrorCode::Unauthorized, "task lacks operation:observe"),
                    io,
                );
            }
            let source = match self.runtime.admission_observed(input.task, io).await {
                Ok(source) => source,
                Err(error) => return query::failure(error, io),
            };
            let outcome = match self.runtime.outcome_observed(input.task, io).await {
                Ok(outcome) => outcome,
                Err(error) => return query::failure(error, io),
            };
            #[derive(Serialize)]
            struct Inspection<'a> {
                task: TaskId,
                admission: &'a TaskAdmissionRecord,
                outcome: Option<Outcome<Value>>,
            }
            return ready(
                Outcome::Succeeded(Inspection {
                    task: input.task,
                    admission: &source,
                    outcome,
                }),
                io,
            );
        }
        if reconciliation_only {
            return mail::reconciled(io);
        }
        let outcome = if command.kind == TASK_JOIN_COMMAND_KIND {
            let input: TaskJoinCommand = mail::decode_command(payload, io)?;
            match self.observe_direct_child(task, input.child, io).await {
                Ok(outcome) => outcome,
                Err(error) => return query::failure(error, io),
            }
        } else {
            let input: TaskObserveCommand = mail::decode_command(payload, io)?;
            self.observe_child(task, input.admission_operation, io)
                .await?
        };
        match outcome {
            None | Some(Outcome::Indeterminate { .. }) => Ok(TaskCommandProgress::Pending),
            Some(outcome) => ready(outcome, io),
        }
    }

    pub(super) async fn observe_child(
        &self,
        task: TaskId,
        admission: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<Option<Outcome<Value>>> {
        let child = TaskId::from_bytes(
            super::super::recursive::derive_operation(
                b"harness/v2/task-child\0",
                &[task.into_bytes(), admission.into_bytes()],
                io,
            )?
            .into_bytes(),
        );
        self.observe_direct_child(task, child, io).await
    }

    pub(super) async fn observe_direct_child(
        &self,
        task: TaskId,
        child: TaskId,
        io: Option<&PreparationIo>,
    ) -> Result<Option<Outcome<Value>>> {
        if self.runtime.admission_observed(child, io).await?.parent != Some(task) {
            return Err(boundary(
                io,
                ErrorCode::Unauthorized,
                "observed task is not a direct child",
            ));
        }
        self.runtime.outcome_observed(child, io).await
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
