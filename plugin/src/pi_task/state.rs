use super::*;

pub(super) fn validate_effect_command(command: &WorkflowCommand) -> acyclic_harness::Result<()> {
    command.validate(&mut None)?;
    if command.kind != EFFECT_TASK_COMMAND_KIND {
        return Err(transition_error(
            "Pi task requires the existing effect command",
        ));
    }
    Ok(())
}

pub(super) fn validate_native_command(command: &WorkflowCommand) -> acyclic_harness::Result<()> {
    command.validate(&mut None)?;
    if command.kind != EFFECT_TASK_COMMAND_KIND && command.kind != NATIVE_LAUNCH_TASK_COMMAND_KIND {
        return Err(transition_error(
            "Pi child requires its retained native launch command",
        ));
    }
    Ok(())
}

pub(super) fn validate_control_command(command: &WorkflowCommand) -> acyclic_harness::Result<()> {
    command.validate(&mut None)?;
    if !matches!(
        command.kind.as_str(),
        "acyclic.task.admit.v1"
            | "acyclic.task.admit_prepared.v1"
            | "acyclic.task.observe.v1"
            | "acyclic.mail.send.v1"
            | "acyclic.mail.receive.v1"
            | "acyclic.timer.v1"
            | "acyclic.fork.prepare.v1"
            | "acyclic.pi.fork"
            | "acyclic.pi.import"
            | "acyclic.pi.writeback"
            | "acyclic.pi.observe-source"
            | "acyclic.pi.inspect"
            | "acyclic.pi.diff"
            | "acyclic.pi.operator-approval"
            | "acyclic.pi.builtin"
    ) {
        return Err(transition_error(
            "Pi control command has no installed scoped adapter",
        ));
    }
    Ok(())
}

/// The caller retains the checkpoint's original custody through extraction and transfers it to the moved body.
pub(super) fn initial_state(
    input: PiTaskInput,
    work: &acyclic_fs::NativeMaterializationWork,
) -> acyclic_harness::Result<PiTaskState> {
    input.binding.validate(
        None,
        input.binding.project_volumes.keys().map(String::as_str),
        true,
    )?;
    if let PiScratchAllocation::Created(creation) = &input.binding.scratch.allocation {
        // Preserve original SDK generation/provider/storage-name failures.
        creation.validate(&input.binding.scratch.volume)?;
    }
    Ok(match input.execution {
        PiTaskExecution::Host { root, preparation } => {
            if !matches!(
                input.binding.scratch.allocation,
                PiScratchAllocation::Preparing
            ) {
                return Err(transition_error(
                    "Pi root admission must precede scratch allocation",
                ));
            }
            root.validate(&input.binding, work)?;
            validate_effect_command(&preparation).map_err(|error| {
                native_validation(error, "Pi task requires the existing effect command")
            })?;
            PiTaskState::Preparing {
                command: preparation,
            }
        }
        PiTaskExecution::Child { launch } => {
            validate_native_command(&launch).map_err(|error| {
                native_validation(
                    error,
                    "Pi child requires its retained native launch command",
                )
            })?;
            PiTaskState::Ready { command: launch }
        }
    })
}

pub(super) enum TransitionInput {
    Null,
    Control(Box<PiControlInput>),
    Controls(ControlResults),
    Results(CommandResults),
    Other,
}

pub(super) fn advance(
    state: PiTaskState,
    input: TransitionInput,
) -> acyclic_harness::Result<(PiTaskState, Option<WorkflowCommand>)> {
    let reject = transition_error;
    Ok(match (state, input) {
        (PiTaskState::Hosting { allocation, reply }, TransitionInput::Null) => {
            (PiTaskState::Hosting { allocation, reply }, None)
        }
        (PiTaskState::Hosting { allocation, reply }, TransitionInput::Control(input)) => {
            let input = *input;
            validate_control_command(&input.command).map_err(|error| {
                native_validation(error, "Pi control command has no installed scoped adapter")
            })?;
            if reply
                .as_ref()
                .is_some_and(|reply| reply.operation_id == input.command.operation_id)
            {
                return Err(reject("Pi control result is already committed"));
            }
            (
                PiTaskState::Controlling {
                    allocation,
                    operation: input.command.operation_id,
                },
                Some(input.command),
            )
        }
        (
            PiTaskState::Controlling {
                allocation,
                operation,
            },
            TransitionInput::Controls(input),
        ) => {
            let [reply]: [PiControlReply; 1] = input
                .commands
                .try_into()
                .map_err(|_| reject("Pi control differs from its retained command"))?;
            if reply.operation_id != operation {
                return Err(reject("Pi control differs from its retained command"));
            }
            (
                PiTaskState::Hosting {
                    allocation,
                    reply: Some(reply),
                },
                None,
            )
        }
        (PiTaskState::Preparing { command }, TransitionInput::Null) => {
            validate_effect_command(&command).map_err(|error| {
                native_validation(error, "Pi task requires the existing effect command")
            })?;
            (
                PiTaskState::Allocating {
                    operation: command.operation_id,
                },
                Some(command),
            )
        }
        (PiTaskState::Ready { command }, TransitionInput::Null) => {
            validate_native_command(&command).map_err(|error| {
                native_validation(
                    error,
                    "Pi child requires its retained native launch command",
                )
            })?;
            (
                PiTaskState::Awaiting {
                    operation: command.operation_id,
                },
                Some(command),
            )
        }
        (
            state @ (PiTaskState::Allocating { .. } | PiTaskState::Awaiting { .. }),
            TransitionInput::Results(input),
        ) => {
            let allocating = matches!(&state, PiTaskState::Allocating { .. });
            let (PiTaskState::Allocating { operation } | PiTaskState::Awaiting { operation }) =
                state
            else {
                unreachable!()
            };
            let [result]: [CommandResult; 1] = input
                .commands
                .try_into()
                .map_err(|_| reject("Pi result differs from its retained effect command"))?;
            if result.operation_id != operation {
                return Err(reject("Pi result differs from its retained effect command"));
            }
            match result.value {
                Outcome::Succeeded(result) => {
                    result.validate(&mut None).map_err(|error| {
                        native_validation(error, "Pi result reference is invalid")
                    })?;
                    if allocating {
                        (
                            PiTaskState::Hosting {
                                allocation: result,
                                reply: None,
                            },
                            None,
                        )
                    } else {
                        (PiTaskState::Completed { result }, None)
                    }
                }
                Outcome::Failed { message } => (PiTaskState::Failed { message }, None),
                Outcome::Cancelled | Outcome::Indeterminate { .. } => {
                    return Err(reject("unsettled Pi effect cannot advance its workflow"));
                }
            }
        }
        _ => return Err(reject("Pi task does not accept this transition")),
    })
}
