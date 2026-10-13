//! Builds the next durable input from the original selection and command.
use super::*;
use crate::wire::ErrorCode::{Invalid, Unauthorized};
use crate::{
    executor::{stock_string_copy, with_history_io},
    filesystem::PreparationIo,
};

impl<P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    RecursiveExecutor<P, A, O>
{
    pub(super) async fn input(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        owner: &(
            Arc<FilesystemTaskRuntime<P, A, O>>,
            Arc<FilesystemWorkflowJournal<P, A, O>>,
        ),
        state: State,
        cancellation: &CancellationToken,
        output_io: Option<&PreparationIo>,
    ) -> Result<Option<Value>> {
        let temporary = recursive_temporary_io(output_io)?;
        let io = temporary.as_ref();
        let caller = context.durable_task_id().expect("admitted caller");
        let (arguments, intent) = match state {
            State::Ready { arguments } if self.durable.definition.name == "acyclic.fork" => {
                let original: ForkToolArguments = decode(&arguments, io)?;
                with_history_io(io, |work| {
                    original.source_task(caller, context.effective_scope().limits(), work)
                })?;
                let payload = stage(
                    context,
                    derived(invocation.operation_id, "source-arguments", io)?,
                    "source-arguments",
                    &arguments,
                    self.maximum_request_bytes,
                    cancellation,
                    io,
                )
                .await?;
                let intent = self
                    .source
                    .as_ref()
                    .expect("authorized source")
                    .select(context, &original, cancellation, io)?
                    .await?;
                validate_source_intent(&intent, io)?;
                reject_if(
                    !recursive_encoded_equal(&intent.arguments, &arguments, io)?
                        || intent.caller != caller
                        || intent.request.operation_id != invocation.operation_id,
                    io,
                    Unauthorized,
                    "source selector changed original caller, arguments or effect",
                )?;
                return encode(
                    &State::SourcePinned {
                        intent,
                        command: WorkflowCommand {
                            operation_id: derived(invocation.operation_id, "source-stage", io)?,
                            kind: stock_string_copy(FORK_SOURCE_STAGE, io)?,
                            payload,
                        },
                    },
                    output_io,
                )
                .map(Some);
            }
            State::Ready { arguments } => (arguments, None),
            State::SourcePinned { intent, command } => {
                validate_source_intent(&intent, io)?;
                check_source_command(&command, &intent, io)?;
                let retained = retained_payload(
                    context,
                    &command.payload,
                    self.maximum_request_bytes,
                    cancellation,
                    io,
                )
                .await?;
                reject_if(
                    retained != invocation.arguments
                        || !recursive_encoded_equal(&intent.arguments, &retained, io)?,
                    io,
                    Unauthorized,
                    "source command changed original arguments",
                )?;
                (retained, Some(intent))
            }
            State::Waiting {
                plan,
                command,
                report,
                receipt,
            } => {
                let report = report.as_deref();
                let receipt = receipt.as_deref();
                let stage = command_stage(&command, report, io)?;
                validate_plan(&plan, io)?;
                require_plan_arguments(&plan, &invocation.arguments, io)?;
                check_command(&command, stage, &plan, io)?;
                reject_if(
                    command.operation_id != derived(invocation.operation_id, stage, io)?,
                    io,
                    Unauthorized,
                    "outbox changed original effect",
                )?;
                if let Plan::Merge(merge) = &plan {
                    let before = io.map(PreparationIo::owned_bytes).transpose()?;
                    let retained = retained_payload(
                        context,
                        &command.payload,
                        self.maximum_request_bytes,
                        cancellation,
                        io,
                    )
                    .await?;
                    reject_if(
                        !recursive_encoded_equal(merge, &retained, io)?,
                        io,
                        Unauthorized,
                        "outbox changed original merge plan",
                    )?;
                    drop(retained);
                    recursive_release_since(io, before)?;
                }
                let Some(value) = self
                    .dispatch(
                        context,
                        owner,
                        &plan,
                        stage,
                        &command,
                        report,
                        receipt,
                        cancellation,
                        io,
                    )
                    .await?
                else {
                    return Ok(None);
                };
                let next = match next_stage(stage, &value) {
                    Some(next_stage) => Some(
                        self.command(context, next_stage, &plan, cancellation, io)
                            .await?,
                    ),
                    None => None,
                };
                return encode(
                    &Receipt {
                        operation_id: command.operation_id,
                        value: Outcome::Succeeded(value),
                        next,
                    },
                    output_io,
                )
                .map(Some);
            }
            State::Complete { .. } => unreachable!("completed protocol returns before input"),
        };
        let stage = if intent.is_some() {
            "prepare"
        } else {
            "merge-apply"
        };
        let plan = self
            .plan(
                context,
                owner,
                caller,
                &arguments,
                intent.as_ref(),
                cancellation,
                io,
            )
            .await?;
        let command = self
            .command(context, stage, &plan, cancellation, io)
            .await?;
        encode(
            &State::Waiting {
                plan,
                command,
                report: None,
                receipt: None,
            },
            output_io,
        )
        .map(Some)
    }

    pub(super) async fn plan(
        &self,
        context: &ToolContext,
        owner: &(
            Arc<FilesystemTaskRuntime<P, A, O>>,
            Arc<FilesystemWorkflowJournal<P, A, O>>,
        ),
        caller: TaskId,
        arguments: &Value,
        intent: Option<&RecursiveForkSourceIntent>,
        cancellation: &CancellationToken,
        io: Option<&PreparationIo>,
    ) -> Result<Plan> {
        let (runtime, workflow) = owner;
        let journal_owner = workflow.owner.as_ref().expect("validated tool owner");
        let source = self.source.as_ref().expect("authorized source");
        let plan = if self.durable.definition.name == "acyclic.fork" {
            let original: ForkToolArguments = decode(arguments, io)?;
            with_history_io(io, |work| {
                original.source_task(caller, context.effective_scope().limits(), work)
            })?;
            let intent = intent
                .ok_or_else(|| recursive_reject(io, Unauthorized, "fork source is not pinned"))?;
            let binding = source
                .fork(context, &original, intent, None, cancellation, io)?
                .await?;
            reject_if(
                binding.source.request != intent.request
                    || binding.child.operation_id != intent.child_operation_id,
                io,
                Unauthorized,
                "staging changed original source or allocation",
            )?;
            validate_fork_binding(context, &original, &binding, cancellation, io).await?;
            let operation = derived(context.operation_id(), "preparation-task", io)?;
            let payload = stage(
                context,
                derived(context.operation_id(), "preparation-payload", io)?,
                "preparation-payload",
                &binding.source.request,
                self.maximum_request_bytes,
                cancellation,
                io,
            )
            .await?;
            let request = fork_preparation_request(
                binding.source.preparer.as_ref(),
                &binding.source.request,
                task_id_for_admission(operation),
                binding.child,
                WorkflowCommand {
                    operation_id: context.operation_id(),
                    kind: stock_string_copy(FORK_PREPARE_TASK_COMMAND_KIND, io)?,
                    payload,
                },
                binding.limits,
                Some(binding.source_checkpoint),
                binding.input_files,
                io,
            )?;
            let preparation = 'admission: {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(work) = io.and_then(PreparationIo::original_native_work) {
                    let input_io = PreparationIo::from_native_work(work)?;
                    let input = encode(&request, Some(&input_io))?;
                    let input = (
                        input,
                        input_io.take_native_custody(input_io.owned_bytes()?)?,
                    );
                    let (admission, custody) = runtime
                        .harness()
                        .registered_child_admission_with_native_work(
                            operation,
                            "acyclic.fork_preparation",
                            "1",
                            input,
                            caller,
                            context.scope(),
                            work,
                        )?;
                    io.expect("native admission")
                        .absorb_native_custody(custody)?;
                    break 'admission admission;
                }
                runtime.harness().registered_child_admission(
                    operation,
                    "acyclic.fork_preparation",
                    "1",
                    encode(&request, io)?,
                    caller,
                    context.scope(),
                )?
            };
            Plan::Fork(recursive_box(
                RecursiveForkToolPlan {
                    arguments: original,
                    caller,
                    request,
                    preparation,
                    child_message_pointer: binding.child_message_pointer,
                    child_message_file_pointer: binding.child_message_file_pointer,
                    allocation_digest: intent.allocation_digest,
                },
                io,
            )?)
        } else {
            let original: MergeToolArguments = decode(arguments, io)?;
            let binding = source
                .merge(context, &original, None, cancellation, io)?
                .await?;
            validate_merge_binding(context, original.source, &binding, io)?;
            let controller = ParentProjectController::new(
                binding.filesystem.as_ref(),
                binding.destination.reducer(),
                &binding.verifier,
                &binding.scope,
                &binding.project,
                io,
            )?;
            let inspected = match &binding.source_read {
                Some(handle) => {
                    controller
                        .prepare_project_import_from_selected_source(
                            &binding.source_project,
                            &binding.source_generation,
                            None,
                            handle,
                            journal_owner,
                            context.task(),
                            context.operation_id(),
                        )
                        .await?
                }
                None => {
                    controller
                        .prepare_project_import(
                            &binding.source_project,
                            &binding.source_generation,
                            None,
                        )
                        .await?
                }
            };
            let expected_target_generation = crate::filesystem::fork_preparer_io::generation_ref(
                binding.project.provider(),
                inspected.target_head(),
                io,
            )?;
            let destination = match io {
                Some(io) => io
                    .history_io()
                    .clone_authority(binding.destination.reducer().authority())?,
                None => binding.destination.reducer().authority().clone(),
            };
            let authorized_source = binding
                .source_read
                .as_ref()
                .map(|source| {
                    with_history_io(io, |work| {
                        source.authorization().clone_with_work_and_custody(work)
                    })
                    .map(|copied| copied.0)
                })
                .transpose()?;
            drop(controller);
            Plan::Merge(recursive_box(
                RecursiveMergeToolPlan {
                    arguments: original,
                    caller,
                    operation_id: context.operation_id(),
                    destination,
                    source: binding.source,
                    source_project: binding.source_project,
                    source_generation: binding.source_generation,
                    authorized_source,
                    target_project: binding.project,
                    expected_target_generation,
                    notice: binding.notice,
                },
                io,
            )?)
        };
        validate_plan(&plan, io)?;
        require_plan_arguments(&plan, arguments, io)?;
        if !io.is_some_and(|io| io.original_native_work().is_some()) {
            crate::executor::json::json_byte_length(
                &plan,
                self.maximum_request_bytes
                    .min(context.scope().limits().file_bytes),
                &mut None,
            )?;
        }
        Ok(plan)
    }

    pub(super) async fn command(
        &self,
        context: &ToolContext,
        stage_kind: &str,
        plan: &Plan,
        cancellation: &CancellationToken,
        io: Option<&PreparationIo>,
    ) -> Result<WorkflowCommand> {
        #[derive(Serialize)]
        struct Admission<'a> {
            admission: &'a TaskAdmissionRecord,
        }
        let operation_id = derived(context.operation_id(), stage_kind, io)?;
        let payload = match command_reference(plan, stage_kind) {
            Some(reference) => with_history_io(io, |work| reference.copy(work))?,
            None => {
                if let Plan::Merge(merge) = plan {
                    let operation = derived(context.operation_id(), "merge-apply", io)?;
                    let payload = stage(
                        context,
                        derived(operation, "payload", io)?,
                        "merge-apply",
                        merge,
                        self.maximum_request_bytes,
                        cancellation,
                        io,
                    )
                    .await?;
                    return Ok(WorkflowCommand {
                        operation_id,
                        kind: stock_string_copy(command_kind(stage_kind), io)?,
                        payload,
                    });
                }
                let admission = match (plan, stage_kind) {
                    (Plan::Fork(plan), "prepare") => &plan.preparation,
                    (Plan::Fork(plan), "child-admit") => &plan.request.child,
                    _ => {
                        return Err(recursive_reject(
                            io,
                            Invalid,
                            "command does not match its plan",
                        ));
                    }
                };
                stage(
                    context,
                    derived(operation_id, "payload", io)?,
                    stage_kind,
                    &Admission { admission },
                    self.maximum_request_bytes,
                    cancellation,
                    io,
                )
                .await?
            }
        };
        Ok(WorkflowCommand {
            operation_id,
            kind: stock_string_copy(command_kind(stage_kind), io)?,
            payload,
        })
    }
}
