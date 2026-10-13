//! Deterministic source selection, publication and child admission protocol.
use super::*;
use crate::filesystem::PreparationIo;
use crate::wire::ErrorCode::{Conflict, Invalid, Unauthorized};

pub(super) struct RecursiveToolMachine {
    identity: MachineIdentity,
    schema: Value,
    maximum: u64,
}
impl RecursiveToolMachine {
    pub(super) fn new(name: &str, maximum: u64) -> Self {
        let mut hash = blake3::Hasher::new();
        hash.update(b"harness/v2/recursive-tool-machine\0");
        hash.update(REVISION.as_bytes());
        hash.update(name.as_bytes());
        hash.update(&maximum.to_le_bytes());
        Self {
            identity: MachineIdentity {
                name: name.into(),
                version: REVISION.into(),
                digest: *hash.finalize().as_bytes(),
            },
            maximum,
            schema: json!({"oneOf":[
                {"type":"object","required":["phase","arguments"],"properties":{"phase":{"const":"ready"},"arguments":{"type":"object"}},"additionalProperties":false},
                {"type":"object","required":["phase","intent","command"],"properties":{"phase":{"const":"source_pinned"},"intent":{"type":"object"},"command":{"type":"object"}},"additionalProperties":false},
                {"type":"object","required":["phase","plan","command","report","receipt"],"properties":{"phase":{"const":"waiting"},"plan":{"type":"object"},"command":{"type":"object"},"report":{"type":["object","null"]},"receipt":{"type":["object","null"]}},"additionalProperties":false},
                {"type":"object","required":["phase","value"],"properties":{"phase":{"const":"complete"},"value":{"type":"object"}},"additionalProperties":false}
            ]}),
        }
    }
}
fn error(io: Option<&PreparationIo>, code: crate::wire::ErrorCode) -> Error {
    recursive_reject(
        io,
        code,
        "recursive transition does not match its admitted operation",
    )
}
fn initialize(
    machine: &RecursiveToolMachine,
    input: &Value,
    io: Option<&PreparationIo>,
) -> Result<Value> {
    recursive_byte_bound(input, machine.maximum, io)?;
    if machine.identity.name == "acyclic.fork" {
        recursive_decoded::<ForkToolArguments, _>(input, io, |_| Ok(()))?;
    } else {
        recursive_decoded::<MergeToolArguments, _>(input, io, |_| Ok(()))?;
    }
    #[derive(Serialize)]
    struct Ready<'a> {
        phase: &'static str,
        arguments: &'a Value,
    }
    encode(
        &Ready {
            phase: "ready",
            arguments: input,
        },
        io,
    )
}
fn transition(
    machine: &RecursiveToolMachine,
    original: &Value,
    input: &Value,
    io: Option<&PreparationIo>,
) -> Result<MachineTransition> {
    recursive_byte_bound(input, machine.maximum, io)?;
    let next = match decode::<State>(original, io)? {
        State::Ready { arguments } if machine.identity.name == "acyclic.fork" => {
            let next: State = decode(input, io)?;
            let State::SourcePinned { intent, command } = &next else {
                return Err(error(io, Unauthorized));
            };
            validate_source_intent(intent, io)?;
            reject_if(
                !recursive_encoded_equal(&intent.arguments, &arguments, io)?,
                io,
                Unauthorized,
                "source changed original arguments",
            )?;
            check_source_command(command, intent, io)?;
            next
        }
        State::Ready { arguments } => {
            let next: State = decode(input, io)?;
            let State::Waiting {
                plan,
                command,
                report: None,
                receipt: None,
            } = &next
            else {
                return Err(error(io, Unauthorized));
            };
            validate_plan(plan, io)?;
            require_plan_arguments(plan, &arguments, io)?;
            reject_if(
                !matches!(plan, Plan::Merge(_)),
                io,
                Unauthorized,
                "merge plan names another tool",
            )?;
            check_command(command, "merge-apply", plan, io)?;
            next
        }
        State::SourcePinned { intent, command } => {
            validate_source_intent(&intent, io)?;
            check_source_command(&command, &intent, io)?;
            let next: State = decode(input, io)?;
            let State::Waiting {
                plan,
                command,
                report: None,
                receipt: None,
            } = &next
            else {
                return Err(error(io, Unauthorized));
            };
            validate_plan(plan, io)?;
            let Plan::Fork(fork) = plan else {
                return Err(error(io, Unauthorized));
            };
            reject_if(
                fork.arguments != intent.arguments
                    || fork.caller != intent.caller
                    || fork.request.child.operation_id != intent.child_operation_id
                    || fork.allocation_digest != intent.allocation_digest
                    || !recursive_decoded::<ForkRequest, _>(
                        &fork.request.payload,
                        io,
                        |request| Ok(*request == intent.request),
                    )?,
                io,
                Unauthorized,
                "fork changed its pinned source or allocation",
            )?;
            check_command(command, "prepare", plan, io)?;
            next
        }
        State::Waiting {
            plan,
            command,
            mut report,
            mut receipt,
        } => {
            let stage = command_stage(&command, report.as_deref(), io)?;
            validate_plan(&plan, io)?;
            check_command(&command, stage, &plan, io)?;
            let result: Receipt = decode(input, io)?;
            reject_if(
                result.operation_id != command.operation_id,
                io,
                Unauthorized,
                "receipt names another command",
            )?;
            let value = match result.value {
                Outcome::Succeeded(value) => value,
                Outcome::Indeterminate { operation_id } => {
                    return Err(Error::Indeterminate(operation_id));
                }
                Outcome::Cancelled => return Err(error(io, Conflict)),
                Outcome::Failed { message } => {
                    #[derive(Serialize)]
                    struct Failed<'a> {
                        status: &'static str,
                        message: &'a str,
                    }
                    let value = encode(
                        &Failed {
                            status: "failed",
                            message: &message,
                        },
                        io,
                    )?;
                    return Ok(MachineTransition {
                        state: recursive_complete_state(&value, io)?,
                        commands: Vec::new(),
                        status: MachineStatus::Failed { message },
                    });
                }
            };
            match (&plan, stage) {
                (Plan::Fork(fork), "prepare") => reject_if(
                    !recursive_decoded::<TaskId, _>(&value, io, |task| {
                        Ok(*task == fork.preparation.task_id())
                    })?,
                    io,
                    Unauthorized,
                    "preparation acknowledged another task",
                )?,
                (Plan::Fork(fork), "observe") => {
                    let observed: ForkReport = decode(&value, io)?;
                    crate::executor::with_history_io(io, |work| observed.validate(work))?;
                    reject_if(
                        !recursive_encoded_equal(&observed.request, &fork.request.payload, io)?,
                        io,
                        Unauthorized,
                        "report changed its original request",
                    )?;
                    report = Some(recursive_box(observed, io)?);
                }
                (Plan::Fork(_), "fork-publish") => {
                    let before = io.map(PreparationIo::owned_bytes).transpose()?;
                    let seed: ForkSeed = decode(&value, io)?;
                    let expected = report
                        .as_ref()
                        .map(|report| {
                            crate::executor::with_history_io(io, |work| {
                                report.copy(work)?.0.into_seed(work)
                            })
                        })
                        .transpose()?;
                    let changed = Some(seed) != expected;
                    drop(expected);
                    recursive_release_since(io, before)?;
                    reject_if(
                        changed,
                        io,
                        Unauthorized,
                        "publication changed the prepared seed",
                    )?;
                }
                (Plan::Fork(fork), "child-admit") => {
                    reject_if(
                        !recursive_decoded::<TaskId, _>(&value, io, |task| {
                            Ok(*task == fork.request.child.task_id())
                        })?,
                        io,
                        Unauthorized,
                        "child admission acknowledged another task",
                    )?;
                    reject_if(
                        result.next.is_some(),
                        io,
                        Invalid,
                        "completed fork has an outbox",
                    )?;
                    let report = report.take().ok_or_else(|| error(io, Invalid))?;
                    let seed =
                        crate::executor::with_history_io(io, |work| (*report).into_seed(work))?;
                    #[derive(Serialize)]
                    struct Admitted<'a> {
                        status: &'static str,
                        id: crate::AgentId,
                        task: TaskId,
                        source: TaskId,
                        operation_id: OperationId,
                        seed: &'a ForkSeed,
                    }
                    return completed(
                        encode(
                            &Admitted {
                                status: "admitted",
                                id: fork.arguments.id,
                                task: fork.request.child.task_id(),
                                source: fork.arguments.source.unwrap_or(fork.caller),
                                operation_id: recursive_decoded::<ForkRequest, _>(
                                    &fork.request.payload,
                                    io,
                                    |request| Ok(request.operation_id),
                                )?,
                                seed: &seed,
                            },
                            io,
                        )?,
                        io,
                    );
                }
                (Plan::Merge(merge), "merge-apply") => {
                    if value.get("status").and_then(Value::as_str) == Some("applied") {
                        let observed: ProjectMergeReceipt =
                            decode(value.get("receipt").ok_or_else(|| error(io, Invalid))?, io)?;
                        validate_merge_receipt(merge, &observed, io)?;
                        receipt = Some(recursive_box(observed, io)?);
                    } else {
                        reject_if(
                            !matches!(
                                value.get("status").and_then(Value::as_str),
                                Some(
                                    "no_changes"
                                        | "stale_target"
                                        | "conflicted"
                                        | "fenced"
                                        | "idempotency_conflict"
                                )
                            ) || result.next.is_some(),
                            io,
                            Invalid,
                            "merge result is not a canonical provider outcome",
                        )?;
                        return completed(value, io);
                    }
                }
                (Plan::Merge(merge), "merge-publish") => {
                    let observed: ProjectMergeReceipt = decode(&value, io)?;
                    validate_merge_receipt(merge, &observed, io)?;
                    reject_if(
                        receipt.as_deref() != Some(&observed) || result.next.is_some(),
                        io,
                        Unauthorized,
                        "publication changed the original receipt",
                    )?;
                    #[derive(Serialize)]
                    struct Published<'a> {
                        status: &'static str,
                        receipt: &'a ProjectMergeReceipt,
                    }
                    return completed(
                        encode(
                            &Published {
                                status: "published",
                                receipt: &observed,
                            },
                            io,
                        )?,
                        io,
                    );
                }
                _ => return Err(error(io, Invalid)),
            }
            let stage = next_stage(stage, &value).ok_or_else(|| error(io, Invalid))?;
            let command = result.next.ok_or_else(|| error(io, Invalid))?;
            check_command(&command, stage, &plan, io)?;
            State::Waiting {
                plan,
                command,
                report,
                receipt,
            }
        }
        State::Complete { .. } => return Err(error(io, Conflict)),
    };
    let state = encode(&next, io)?;
    let command = match next {
        State::Waiting { command, .. } | State::SourcePinned { command, .. } => command,
        _ => return Err(error(io, Invalid)),
    };
    let mut commands = Vec::new();
    crate::executor::stock_vec_push(&mut commands, command, io)?;
    Ok(MachineTransition {
        state,
        commands,
        status: MachineStatus::Suspended,
    })
}
impl ResumableMachine for RecursiveToolMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        initialize(self, input, None)
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        transition(self, state, input, None)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_work(&self) -> bool {
        true
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn identity_with_native_work<'a>(
        &'a self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<&'a MachineIdentity> {
        work.charge(acyclic_fs::WorkCounters {
            items_examined: 1,
            ..Default::default()
        })?;
        Ok(&self.identity)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn initialize_with_native_work(
        &self,
        input: &Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        let io = PreparationIo::from_native_work(work)?;
        let value = initialize(self, input, Some(&io))?;
        let custody = io.take_native_custody(recursive_value_owned_bytes(&value)?)?;
        Ok((value, custody))
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn transition_with_native_work(
        &self,
        state: &Value,
        input: Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
        let io = PreparationIo::from_native_work(work)?;
        let transition = transition(self, state, &input, Some(&io))?;
        drop(input);
        let custody =
            io.take_native_custody(recursive_transition_owned_bytes(&transition, &io)?)?;
        Ok((transition, custody))
    }
}
pub(super) fn completed(value: Value, io: Option<&PreparationIo>) -> Result<MachineTransition> {
    Ok(MachineTransition {
        state: recursive_complete_state(&value, io)?,
        commands: Vec::new(),
        status: MachineStatus::Completed { value },
    })
}
pub(super) fn recursive_complete_state(value: &Value, io: Option<&PreparationIo>) -> Result<Value> {
    #[derive(Serialize)]
    struct Complete<'a> {
        phase: &'static str,
        value: &'a Value,
    }
    encode(
        &Complete {
            phase: "complete",
            value,
        },
        io,
    )
}
