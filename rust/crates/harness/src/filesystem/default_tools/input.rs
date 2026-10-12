//! Input for portable recursive tools.

use super::*;

impl<P, A, O> RecursiveExecutor<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn input(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        owner: &RecursiveToolOwner<P, A, O>,
        state: State,
        cancellation: &CancellationToken,
        output_io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Option<Value>> {
        let temporary_io = recursive_temporary_io(output_io)?;
        let io = temporary_io.as_ref();
        let caller = context
            .durable_task_id()
            .expect("input builder has an admitted caller");
        let (arguments, intent) = match state {
            State::Ready { arguments } if self.durable.definition.name == "acyclic.fork" => {
                let original: ForkToolArguments = decode(&arguments, io)?;
                crate::executor::with_history_io(io, |work| {
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
                    .as_deref()
                    .expect("recursive source was authorized")
                    .select(context, &original, cancellation, io)?
                    .await?;
                validate_source_intent(&intent, &arguments, io)?;
                if intent.caller != caller || intent.request.operation_id != invocation.operation_id
                {
                    return Err(recursive_reject(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "source selector is not the actual original caller/effect",
                    ));
                }
                return encode(
                    &SourcePin {
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
            State::SourcePinned {
                arguments,
                intent,
                command,
            } => {
                validate_source_intent(&intent, &arguments, io)?;
                check_source_command(&command, &intent, io)?;
                let before_read = io
                    .map(crate::filesystem::PreparationIo::owned_bytes)
                    .transpose()?;
                let retained = retained_payload(
                    context,
                    &command.payload,
                    self.maximum_request_bytes,
                    cancellation,
                    io,
                )
                .await?;
                if retained != arguments {
                    return Err(recursive_reject(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "source-stage outbox changed original tool arguments",
                    ));
                }
                drop(retained);
                recursive_release_since(io, before_read)?;
                (arguments, Some(intent))
            }
            State::Waiting {
                plan,
                command,
                report,
                receipt,
                ..
            } => {
                let report = report.as_deref();
                let receipt = receipt.as_deref();
                let stage = command_stage(&command, report, io)?;
                validate_plan(&plan, &invocation.arguments, io)?;
                if io.is_some_and(|io| io.original_native_work().is_some()) {
                    check_command(&command, stage, &plan, io)?;
                }
                if command.operation_id != derived(invocation.operation_id, stage, io)? {
                    return Err(recursive_reject(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "recursive outbox operation differs from original effect",
                    ));
                }
                let expected = command_payload(stage, &plan, report, receipt, io)?;
                let before_read = io
                    .map(crate::filesystem::PreparationIo::owned_bytes)
                    .transpose()?;
                let retained = retained_payload(
                    context,
                    &command.payload,
                    self.maximum_request_bytes,
                    cancellation,
                    io,
                )
                .await?;
                if retained != expected {
                    return Err(recursive_reject(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "recursive outbox payload changed original retained plan",
                    ));
                }
                drop(retained);
                recursive_release_since(io, before_read)?;
                let Some(value) = self
                    .dispatch(
                        context,
                        owner,
                        &plan,
                        stage,
                        &command,
                        expected,
                        report,
                        receipt,
                        cancellation,
                        io,
                    )
                    .await?
                else {
                    return Ok(None);
                };
                let next_stage = next_stage(stage, &value);
                let observed_report = if stage == "observe" {
                    Some(decode::<ForkReport>(&value, io)?)
                } else {
                    None
                };
                let observed_receipt = if stage == "merge-apply" && next_stage.is_some() {
                    Some(decode::<ProjectMergeReceipt>(
                        value.get("receipt").ok_or_else(|| {
                            recursive_reject(
                                io,
                                crate::wire::ErrorCode::Invalid,
                                "merge receipt missing",
                            )
                        })?,
                        io,
                    )?)
                } else {
                    None
                };
                let next_report = observed_report.as_ref().or(report);
                let next_receipt = observed_receipt.as_ref().or(receipt);
                let next = match next_stage {
                    Some(stage) => Some(
                        self.command(
                            context,
                            stage,
                            &plan,
                            next_report,
                            next_receipt,
                            cancellation,
                            io,
                        )
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
            State::Complete { .. } => unreachable!("completed states return before building input"),
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
            .command(context, stage, &plan, None, None, cancellation, io)
            .await?;
        encode(&InitialPlan { plan, command }, output_io).map(Some)
    }

    pub(super) async fn plan(
        &self,
        context: &ToolContext,
        owner: &RecursiveToolOwner<P, A, O>,
        caller: TaskId,
        arguments: &Value,
        intent: Option<&RecursiveForkSourceIntent>,
        cancellation: &CancellationToken,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Plan> {
        let runtime = owner.runtime.as_ref();
        #[cfg(not(target_arch = "wasm32"))]
        let native_work = io.and_then(crate::filesystem::PreparationIo::original_native_work);
        let plan = if self.durable.definition.name == "acyclic.fork" {
            let original: ForkToolArguments = decode(arguments, io)?;
            crate::executor::with_history_io(io, |work| {
                original.source_task(caller, context.effective_scope().limits(), work)
            })?;
            let intent = intent.ok_or_else(|| {
                recursive_reject(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "fork source choice is not durably pinned",
                )
            })?;
            let binding = self
                .source
                .as_deref()
                .expect("recursive source was authorized")
                .fork(context, &original, intent, None, cancellation, io)?
                .await?;
            if binding.source.request != intent.request
                || binding.child.operation_id != intent.child_operation_id
            {
                return Err(recursive_reject(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "source staging changed the selected immutable source/allocation",
                ));
            }
            validate_fork_binding(context, &original, &binding, cancellation, io).await?;
            let operation = derived(context.operation_id(), "preparation-task", io)?;
            let payload = {
                let hold;
                let before = io
                    .map(crate::filesystem::PreparationIo::owned_bytes)
                    .transpose()?
                    .unwrap_or(0);
                let encoded = encode(&binding.source.request, io)?;
                hold = io
                    .map(|io| {
                        io.guard_owned(io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                            recursive_reject(
                                Some(io),
                                crate::wire::ErrorCode::Invalid,
                                "recursive payload custody underflow",
                            )
                        })?)
                    })
                    .transpose()?;
                let file = stage(
                    context,
                    derived(context.operation_id(), "preparation-payload", io)?,
                    "preparation-payload",
                    &encoded,
                    self.maximum_request_bytes,
                    cancellation,
                    io,
                )
                .await?;
                drop(encoded);
                drop(hold);
                file
            };
            let checkpoint = binding.source_checkpoint;
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
                Some(checkpoint),
                binding.input_files,
                io,
            )?;
            let preparation = 'preparation: {
                #[cfg(not(target_arch = "wasm32"))]
                if let (Some(io), Some(work)) = (io, native_work) {
                    let input_io = crate::filesystem::PreparationIo::from_native_work(work)?;
                    let input = encode(&request, Some(&input_io))?;
                    let input = (
                        input,
                        input_io.take_native_custody(input_io.owned_bytes()?)?,
                    );
                    let (preparation, custody) = runtime
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
                    io.absorb_native_custody(custody)?;
                    break 'preparation preparation;
                }
                break 'preparation runtime.harness().registered_child_admission(
                    operation,
                    "acyclic.fork_preparation",
                    "1",
                    encode(&request, io)?,
                    caller,
                    context.scope(),
                )?;
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
            let binding = self
                .source
                .as_deref()
                .expect("recursive source was authorized")
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
                            &owner.journal_owner,
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
                    crate::executor::with_history_io(io, |work| {
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
                    target_project: binding.project,
                    expected_target_generation,
                    authorized_source,
                    notice: binding.notice,
                },
                io,
            )?)
        };
        validate_plan(&plan, arguments, io)?;
        if !io.is_some_and(|io| io.original_native_work().is_some()) {
            crate::contract::json_byte_length(&plan, self.maximum_request_bytes
                    .min(context.scope().limits().file_bytes), &mut None).map(|_| ())?;
        }
        Ok(plan)
    }

    pub(super) async fn command(
        &self,
        context: &ToolContext,
        stage_kind: &str,
        plan: &Plan,
        report: Option<&ForkReport>,
        receipt: Option<&ProjectMergeReceipt>,
        cancellation: &CancellationToken,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<WorkflowCommand> {
        let operation_id = derived(context.operation_id(), stage_kind, io)?;
        let payload_hold;
        let before = io
            .map(crate::filesystem::PreparationIo::owned_bytes)
            .transpose()?
            .unwrap_or(0);
        let payload = command_payload(stage_kind, plan, report, receipt, io)?;
        payload_hold = io
            .map(|io| {
                io.guard_owned(io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                    recursive_reject(
                        Some(io),
                        crate::wire::ErrorCode::Invalid,
                        "recursive payload custody underflow",
                    )
                })?)
            })
            .transpose()?;
        let file = stage(
            context,
            derived(operation_id, "payload", io)?,
            stage_kind,
            &payload,
            self.maximum_request_bytes,
            cancellation,
            io,
        )
        .await?;
        drop(payload);
        drop(payload_hold);
        Ok(WorkflowCommand {
            operation_id,
            kind: stock_string_copy(command_kind(stage_kind), io)?,
            payload: file,
        })
    }
}

use crate::executor::{stock_string_copy};
