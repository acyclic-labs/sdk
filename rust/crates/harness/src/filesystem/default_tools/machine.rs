//! Machine for portable recursive tools.

use super::*;

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
        let digest = *hash.finalize().as_bytes();
        Self {
            identity: MachineIdentity {
                name: name.into(),
                version: REVISION.into(),
                digest,
            },
            maximum,
            schema: json!({"oneOf":[
                {"type":"object","required":["phase","arguments"],"properties":{"phase":{"const":"ready"},"arguments":{"type":"object"}},"additionalProperties":false},
                {"type":"object","required":["phase","arguments","intent","command"],"properties":{
                    "phase":{"const":"source_pinned"},"arguments":{"type":"object"},"intent":{"type":"object"},
                    "command":{"type":"object"}},"additionalProperties":false},
                {"type":"object","required":["phase","arguments","plan","command","report","receipt"],"properties":{
                    "phase":{"const":"waiting"},"arguments":{"type":"object"},"plan":{"type":"object"},
                    "command":{"type":"object"},"report":{"type":["object","null"]},"receipt":{"type":["object","null"]}},"additionalProperties":false},
                {"type":"object","required":["phase","value"],"properties":{"phase":{"const":"complete"},"value":{"type":"object"}},"additionalProperties":false}
            ]}),
        }
    }
}
impl ResumableMachine for RecursiveToolMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        self.initialize_engine(input, None)
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        self.transition_engine(state, input, None)
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
        })
        .map_err(Error::from)?;
        Ok(&self.identity)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn initialize_with_native_work(
        &self,
        input: &Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let state = self.initialize_engine(input, Some(&io))?;
        let custody = io.take_native_custody(recursive_value_owned_bytes(&state)?)?;
        Ok((state, custody))
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn transition_with_native_work(
        &self,
        state: &Value,
        input: Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let transition = self.transition_engine(state, &input, Some(&io))?;
        drop(input);
        let custody =
            io.take_native_custody(recursive_transition_owned_bytes(&transition, &io)?)?;
        Ok((transition, custody))
    }
}

impl RecursiveToolMachine {
    fn initialize_engine(
        &self,
        input: &Value,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Value> {
        recursive_byte_bound(input, self.maximum, io)?;
        if self.identity.name == "acyclic.fork" {
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

    fn transition_engine(
        &self,
        original_state: &Value,
        input: &Value,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<MachineTransition> {
        recursive_byte_bound(input, self.maximum, io)?;
        let state: State = decode(original_state, io)?;
        let next = match state {
            State::Ready { arguments } => {
                if self.identity.name == "acyclic.fork" {
                    let selected: SourcePin = decode(input, io)?;
                    validate_source_intent(&selected.intent, &arguments, io)?;
                    check_source_command(&selected.command, &selected.intent, io)?;
                    State::SourcePinned {
                        arguments,
                        intent: selected.intent,
                        command: selected.command,
                    }
                } else {
                    let initial: InitialPlan = decode(input, io)?;
                    validate_plan(&initial.plan, &arguments, io)?;
                    if !matches!(initial.plan, Plan::Merge(_)) {
                        return Err(recursive_reject(
                            io,
                            crate::wire::ErrorCode::Unauthorized,
                            "merge tool plan names another tool",
                        ));
                    }
                    check_command(&initial.command, "merge-apply", &initial.plan, io)?;
                    State::Waiting {
                        arguments,
                        plan: initial.plan,
                        command: initial.command,
                        report: None,
                        receipt: None,
                    }
                }
            }
            State::SourcePinned {
                arguments,
                intent,
                command,
            } => {
                validate_source_intent(&intent, &arguments, io)?;
                check_source_command(&command, &intent, io)?;
                let initial: InitialPlan = decode(input, io)?;
                validate_plan(&initial.plan, &arguments, io)?;
                let Plan::Fork(plan) = &initial.plan else {
                    return Err(recursive_reject(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "pinned fork source produced another tool plan",
                    ));
                };
                if plan.caller != intent.caller
                    || plan.request.child.operation_id != intent.child_operation_id
                    || plan.allocation_digest != intent.allocation_digest
                    || !recursive_decoded::<ForkRequest, _>(&plan.request.payload, io, |request| {
                        Ok(*request == intent.request)
                    })?
                {
                    return Err(recursive_reject(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "fork plan changed its durably selected original source/allocation",
                    ));
                }
                check_command(&initial.command, "prepare", &initial.plan, io)?;
                State::Waiting {
                    arguments,
                    plan: initial.plan,
                    command: initial.command,
                    report: None,
                    receipt: None,
                }
            }
            State::Waiting {
                arguments,
                plan,
                command,
                mut report,
                mut receipt,
            } => {
                let stage = command_stage(&command, report.as_deref(), io)?;
                validate_plan(&plan, &arguments, io)?;
                check_command(&command, stage, &plan, io)?;
                let result: Receipt = decode(input, io)?;
                if result.operation_id != command.operation_id {
                    return Err(recursive_reject(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "recursive receipt belongs to another command",
                    ));
                }
                let value = match result.value {
                    Outcome::Succeeded(value) => value,
                    Outcome::Indeterminate { operation_id } => {
                        return Err(Error::Indeterminate(operation_id));
                    }
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
                    Outcome::Cancelled => {
                        return Err(recursive_reject(
                            io,
                            crate::wire::ErrorCode::Conflict,
                            "recursive command cancelled without a successful receipt",
                        ));
                    }
                };
                match (&plan, stage) {
                    (Plan::Fork(fork), "prepare") => {
                        if !recursive_decoded::<TaskId, _>(&value, io, |ack| {
                            Ok(*ack == fork.preparation.task_id())
                        })? {
                            return Err(recursive_reject(
                                io,
                                crate::wire::ErrorCode::Unauthorized,
                                "preparation admission acknowledged another child",
                            ));
                        }
                    }
                    (Plan::Fork(fork), "observe") => {
                        let observed: ForkReport = decode(&value, io)?;
                        crate::executor::with_history_io(io, |work| observed.validate(work))?;
                        if !recursive_encoded_equal(&observed.request, &fork.request.payload, io)? {
                            return Err(recursive_reject(
                                io,
                                crate::wire::ErrorCode::Unauthorized,
                                "fork report changed the original source request",
                            ));
                        }
                        report = Some(recursive_box(observed, io)?);
                    }
                    (Plan::Fork(_), "fork-publish") => {
                        let before = io.map(|io| io.owned_bytes()).transpose()?;
                        let seed: ForkSeed = decode(&value, io)?;
                        let expected = match report.as_ref() {
                            Some(report) => {
                                let copy = crate::executor::with_history_io(io, |work| {
                                    report.copy(work).map(|(report, _)| report)
                                })?;
                                Some(crate::executor::with_history_io(io, |work| {
                                    copy.into_seed(work)
                                })?)
                            }
                            None => None,
                        };
                        let changed = Some(seed) != expected;
                        drop(expected);
                        recursive_release_since(io, before)?;
                        if changed {
                            return Err(recursive_reject(
                                io,
                                crate::wire::ErrorCode::Unauthorized,
                                "fork publication receipt changed the prepared child",
                            ));
                        }
                    }
                    (Plan::Fork(fork), "child-admit") => {
                        if !recursive_decoded::<TaskId, _>(&value, io, |ack| {
                            Ok(*ack == fork.request.child.task_id())
                        })? {
                            return Err(recursive_reject(
                                io,
                                crate::wire::ErrorCode::Unauthorized,
                                "child admission receipt changed its task identity",
                            ));
                        }
                        if result.next.is_some() {
                            return Err(recursive_reject(
                                io,
                                crate::wire::ErrorCode::Invalid,
                                "completed fork has an unexpected outbox",
                            ));
                        }
                        let prepared = report.take().ok_or_else(|| {
                            recursive_reject(
                                io,
                                crate::wire::ErrorCode::Invalid,
                                "fork has no prepared report",
                            )
                        })?;
                        let seed = crate::executor::with_history_io(io, |work| {
                            (*prepared).into_seed(work)
                        })?;
                        #[derive(Serialize)]
                        struct Admitted<'a> {
                            status: &'static str,
                            id: crate::AgentId,
                            task: TaskId,
                            source: TaskId,
                            operation_id: OperationId,
                            seed: &'a ForkSeed,
                        }
                        let value = encode(
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
                        )?;
                        return completed(value, io);
                    }
                    (Plan::Merge(merge), "merge-apply") => {
                        if value.get("status").and_then(Value::as_str) == Some("applied") {
                            let observed: ProjectMergeReceipt = decode(
                                value.get("receipt").ok_or_else(|| {
                                    recursive_reject(
                                        io,
                                        crate::wire::ErrorCode::Invalid,
                                        "applied merge has no receipt",
                                    )
                                })?,
                                io,
                            )?;
                            validate_merge_receipt(merge, &observed, io)?;
                            receipt = Some(recursive_box(observed, io)?);
                        } else {
                            if !matches!(
                                value.get("status").and_then(Value::as_str),
                                Some(
                                    "no_changes"
                                        | "stale_target"
                                        | "conflicted"
                                        | "fenced"
                                        | "idempotency_conflict"
                                )
                            ) {
                                return Err(recursive_reject(
                                    io,
                                    crate::wire::ErrorCode::Invalid,
                                    "merge receipt is not a canonical provider outcome",
                                ));
                            }
                            if result.next.is_some() {
                                return Err(recursive_reject(
                                    io,
                                    crate::wire::ErrorCode::Invalid,
                                    "unapplied merge has a publication command",
                                ));
                            }
                            return completed(value, io);
                        }
                    }
                    (Plan::Merge(merge), "merge-publish") => {
                        let observed: ProjectMergeReceipt = decode(&value, io)?;
                        validate_merge_receipt(merge, &observed, io)?;
                        if receipt.as_deref() != Some(&observed) || result.next.is_some() {
                            return Err(recursive_reject(
                                io,
                                crate::wire::ErrorCode::Unauthorized,
                                "merge publication changed its original receipt",
                            ));
                        }
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
                    _ => {
                        return Err(recursive_reject(
                            io,
                            crate::wire::ErrorCode::Invalid,
                            "recursive stage does not match its original plan",
                        ));
                    }
                };
                let stage = next_stage(stage, &value).ok_or_else(|| {
                    recursive_reject(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "recursive next stage missing",
                    )
                })?;
                let command = result.next.ok_or_else(|| {
                    recursive_reject(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "recursive next outbox missing",
                    )
                })?;
                check_command(&command, stage, &plan, io)?;
                State::Waiting {
                    arguments,
                    plan,
                    command,
                    report,
                    receipt,
                }
            }
            State::Complete { .. } => {
                return Err(recursive_reject(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "recursive tool is already complete",
                ));
            }
        };
        let state = encode(&next, io)?;
        let command = match next {
            State::Waiting { command, .. } | State::SourcePinned { command, .. } => command,
            _ => {
                return Err(recursive_reject(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "recursive waiting state missing",
                ));
            }
        };
        let commands = recursive_command_outbox(command, io)?;
        Ok(MachineTransition {
            commands,
            state,
            status: MachineStatus::Suspended,
        })
    }
}

pub(super) fn completed(
    value: Value,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<MachineTransition> {
    Ok(MachineTransition {
        state: recursive_complete_state(&value, io)?,
        commands: Vec::new(),
        status: MachineStatus::Completed { value },
    })
}
pub(super) fn recursive_complete_state(
    value: &Value,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<Value> {
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
