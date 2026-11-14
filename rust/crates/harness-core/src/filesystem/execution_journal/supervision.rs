//! Queries derived from the original journal producer, never a second follower.
use super::*;
use crate::supervision::{
    Observation, Policy, Position,
    streams::{NativeMeasurement, Projection, Queries},
};
use std::borrow::Cow;

#[cfg(test)]
mod tests;

pub(super) enum Invocation<'a> {
    Published(Cow<'a, FileRef>),
    Proposed(crate::tool::ToolInvocation),
}

pub(super) enum Inputs<'a> {
    PolicyChanged,
    Progress,
    NativeCapture {
        call: OperationId,
        measurement: NativeMeasurement,
    },
    Usage {
        tokens: Option<u64>,
        usd_nanos: Option<u64>,
        spent_tokens: bool,
    },
    ModelOutput {
        digest: Option<[u8; 32]>,
    },
    Tool {
        execution: OperationId,
        invocation: Invocation<'a>,
        event: &'a ExecutionEvent,
    },
}

#[derive(Serialize)]
struct InvocationSignature<'a> {
    name: &'a str,
    arguments: Arguments<'a>,
    target: Option<&'a str>,
}

struct Arguments<'a> {
    value: &'a serde_json::Value,
    shell: bool,
}

impl Serialize for Arguments<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        // Preserve execution inputs, including exact source/argv, cwd, target
        // and lifetime. Only capture and return scheduling leave them unchanged.
        if !self.shell {
            return self.value.serialize(serializer);
        }
        let Some(arguments) = self.value.as_object() else {
            return self.value.serialize(serializer);
        };
        use serde::ser::SerializeMap as _;
        let mut map = serializer.serialize_map(None)?;
        for (name, value) in arguments {
            if !matches!(
                name.as_str(),
                "yield_time_ms" | "max_output_tokens" | "background"
            ) {
                map.serialize_entry(name, value)?;
            }
        }
        map.end()
    }
}

fn encode<T: Serialize>(
    value: &T,
    io: Option<&PreparationIo>,
) -> Result<acyclic_stream::OwnedBytes> {
    match io {
        Some(io) => io.encode(value, super::MAXIMUM_PAYLOAD_BYTES, true),
        None => crate::contract::canonical_json_bytes(value).map(Into::into),
    }
}

pub(super) fn indexed_headers<const N: usize>(
    mut keys: [acyclic_stream::OwnedBytes; N],
    io: Option<&PreparationIo>,
) -> Result<acyclic_stream::RecordHeaders> {
    keys.sort_unstable();
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = io.and_then(PreparationIo::original_native_work) {
        return acyclic_stream::RecordHeaders::from_keys_native(keys, &work.runtime_meter())
            .map_err(|error| Error::from(acyclic_fs::NativeTreeMaterializationError::Io(error)))?
            .map_err(Into::into);
    }
    let mut owned = crate::model::vec_with_capacity(N, io)?;
    owned.extend(keys);
    acyclic_stream::RecordHeaders::new(owned, Vec::new()).map_err(Into::into)
}

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    // Select a tool's admitted invocation before taking the journal's append
    // lock. The authenticated per-call index has at most a start and terminal;
    // it never scans the step, turn or conversation to locate the command.
    pub(super) async fn supervision_inputs<'a>(
        &self,
        operation: OperationId,
        event: &'a ExecutionEvent,
        measured: (Option<&ModelEvent>, Option<u64>, Option<u64>),
        io: Option<&PreparationIo>,
    ) -> Result<Option<Inputs<'a>>> {
        if self.agent_events.is_none() {
            return Ok(None);
        }
        if matches!(event, ExecutionEvent::Started { .. })
            && self.run_admission.as_ref().is_some_and(|source| {
                source.value.reservation != crate::supervision::Reservation::default()
            })
        {
            return Ok(Some(Inputs::PolicyChanged));
        }
        if let (Some(ModelEvent::Completed { metadata }), usd_nanos, tokens) = measured {
            let reported = crate::executor::ModelTokenUsage::reported(metadata);
            return Ok(Some(Inputs::Usage {
                spent_tokens: reported.input.is_some_and(|tokens| tokens > 0)
                    || reported.output.is_some_and(|tokens| tokens > 0)
                    || reported.cached.is_some_and(|tokens| tokens > 0)
                    || reported.cache_write.is_some_and(|tokens| tokens > 0)
                    || tokens.is_some_and(|tokens| tokens > 0),
                tokens,
                usd_nanos,
            }));
        }
        let (step, call_id) = match event {
            ExecutionEvent::NativeCapture { receipt } => {
                if !self
                    .run_admission
                    .as_ref()
                    .is_some_and(|source| source.value.no_progress_turns.is_some())
                {
                    return Ok(None);
                }
                let (record, _hold) = crate::executor::load_json::<
                    crate::runtime::NativeCaptureProgress,
                >(self, receipt, io)
                .await?;
                record.validate(operation, receipt)?;
                let measurement = if matches!(
                    record.evidence,
                    crate::runtime::NativeCaptureEvidence::Pending { .. }
                ) {
                    NativeMeasurement::Pending
                } else {
                    NativeMeasurement::Complete(record.changed)
                };
                return Ok(Some(Inputs::NativeCapture {
                    call: record.call,
                    measurement,
                }));
            }
            ExecutionEvent::Progress { .. } => {
                return Ok(self
                    .run_admission
                    .as_ref()
                    .is_some_and(|source| source.value.no_progress_turns.is_some())
                    .then_some(Inputs::Progress));
            }
            ExecutionEvent::ModelOutput { digest, .. } => {
                return Ok(Some(Inputs::ModelOutput { digest: *digest }));
            }
            ExecutionEvent::ToolStarted { invocation, .. } => {
                if !self.run_admission.as_ref().is_some_and(|source| {
                    source.value.quiet_ms.is_some() || !source.value.validation_commands.is_empty()
                }) {
                    return Ok(None);
                }
                return Ok(Some(Inputs::Tool {
                    execution: operation,
                    invocation: Invocation::Published(Cow::Borrowed(invocation)),
                    event,
                }));
            }
            ExecutionEvent::ToolCompleted { step, call_id, .. }
            | ExecutionEvent::ToolFailed { step, call_id, .. } => (*step, call_id),
            _ => return Ok(None),
        };
        let select = |event: &ExecutionEvent| matches!(event, ExecutionEvent::ToolStarted { .. });
        let selected = self
            .indexed_step(
                crate::executor::ExecutionRecordQuery {
                    operation,
                    step,
                    call_id: Some(call_id),
                    include_started: false,
                    maximum_step_records: Some(2),
                    maximum: Some(1),
                    select: &select,
                },
                io,
            )
            .await?;
        let invocation = selected.and_then(|(_, records)| {
            records.into_iter().find_map(|record| match record.event {
                ExecutionEvent::ToolStarted { invocation, .. } => Some(invocation),
                _ => None,
            })
        });
        let invocation = match invocation {
            Some(invocation) => Some(Invocation::Published(Cow::Owned(invocation))),
            None if matches!(event, ExecutionEvent::ToolFailed { .. }) => self
                .original_model_call(operation, step, call_id, io)
                .await?
                .map(Invocation::Proposed),
            None => None,
        };
        Ok(invocation.map(|invocation| Inputs::Tool {
            execution: operation,
            invocation,
            event,
        }))
    }

    pub(super) async fn supervision_projection(
        &self,
        source: &Position,
        inputs: Option<&Inputs<'_>>,
        io: Option<&PreparationIo>,
    ) -> Result<Option<Projection>> {
        let Some((agent, run, turn, _)) = &self.agent_events else {
            return Ok(None);
        };
        let defaults = Policy::default();
        let policy = self
            .run_admission
            .as_ref()
            .map(|source| &source.value)
            .unwrap_or(&defaults);
        let queries = Queries::new(&*self.stream, agent, *run);
        let Some(inputs) = inputs else {
            return Ok(None);
        };
        if matches!(inputs, Inputs::PolicyChanged) {
            return queries
                .project(source, &Observation::PolicyChanged, policy)
                .await
                .map(Some);
        }
        if matches!(inputs, Inputs::Progress) {
            return queries
                .project_progress(source, *turn, policy)
                .await
                .map(Some);
        }
        if let Inputs::NativeCapture { call, measurement } = inputs {
            return queries
                .project_native_capture(source, *turn, *call, *measurement, policy)
                .await
                .map(Some);
        }
        if let Inputs::Usage {
            tokens,
            usd_nanos,
            spent_tokens,
        } = inputs
        {
            return queries
                .project_usage(
                    source,
                    &crate::supervision::streams::TurnUsage {
                        turn: *turn,
                        tokens: *tokens,
                        usd_nanos: *usd_nanos,
                        spent_tokens: *spent_tokens,
                    },
                    policy,
                )
                .await
                .map(Some);
        }
        if let Inputs::ModelOutput { digest } = inputs {
            return queries
                .project(
                    source,
                    &Observation::ModelOutput {
                        turn: *turn,
                        output: *digest,
                    },
                    policy,
                )
                .await
                .map(Some);
        }
        let Inputs::Tool {
            execution,
            invocation,
            event,
        } = inputs
        else {
            return Ok(None);
        };
        let (invocation, target, _invocation_hold) = match invocation {
            Invocation::Published(file) => {
                let (value, hold) = crate::executor::load_json::<crate::tool::RecordedInvocation>(
                    self,
                    file.as_ref(),
                    io,
                )
                .await?;
                (Cow::Owned(value.invocation), value.target, hold)
            }
            Invocation::Proposed(value) => (Cow::Borrowed(value), None, None),
        };
        let command = (invocation.name == "shell")
            .then(|| {
                invocation
                    .arguments
                    .get("command")
                    .and_then(serde_json::Value::as_str)
            })
            .flatten();
        let _signature_hold;
        let signature = encode(
            &InvocationSignature {
                name: &invocation.name,
                arguments: Arguments {
                    value: &invocation.arguments,
                    shell: invocation.name == "shell",
                },
                target: target.as_deref(),
            },
            io,
        )?;
        _signature_hold = io.map(|io| io.guard_bytes(&signature)).transpose()?;
        let signature = std::str::from_utf8(&signature)
            .map_err(|_| Error::Invalid("tool signature is not UTF-8".into()))?;
        if matches!(event, ExecutionEvent::ToolStarted { .. }) {
            let check = command.is_some_and(|command| policy.validation_commands.contains(command));
            if !check && policy.quiet_ms.is_none() {
                return Ok(None);
            }
            let mut projection = queries
                .project(
                    source,
                    &Observation::ToolStarted {
                        turn: *turn,
                        command: signature,
                        check,
                    },
                    policy,
                )
                .await?;
            if let Some(quiet_ms) = policy.quiet_ms {
                let at_ms = self
                    .clock
                    .as_ref()
                    .ok_or_else(|| {
                        crate::executor::stock_boundary(
                            io,
                            crate::wire::ErrorCode::Invalid,
                            "quiet supervision has no original host clock",
                        )
                    })?
                    .now_unix_ms();
                self.tool_job_projection(
                    &mut projection,
                    crate::supervision::tool_jobs::Record {
                        version: 1,
                        call: invocation.operation_id,
                        event: crate::supervision::tool_jobs::Event::Started {
                            run: *run,
                            execution: *execution,
                            quiet_ms,
                            action: policy
                                .actions
                                .get(&crate::supervision::Kind::Stuck)
                                .copied()
                                .unwrap_or_default(),
                            at_ms,
                            source: source.clone(),
                        },
                    },
                    io,
                )
                .await?;
            }
            return Ok(Some(projection));
        }
        let _error_hold;
        let mut background_started = false;
        let error = match event {
            ExecutionEvent::ToolFailed { reason, cause, .. } => Some(encode(&(reason, cause), io)?),
            ExecutionEvent::ToolCompleted { result, .. } => {
                let (result, _result_hold) =
                    crate::executor::load_json::<crate::tool::ToolResult>(self, result, io).await?;
                let value = &result.value;
                let shell = invocation.name == "shell";
                background_started = shell
                    && value.get("background").and_then(serde_json::Value::as_bool) == Some(true);
                let failed = shell
                    && (value.get("success").and_then(serde_json::Value::as_bool) == Some(false)
                        || value
                            .get("exit_code")
                            .and_then(serde_json::Value::as_i64)
                            .is_some_and(|code| code != 0))
                    || value.get("error").is_some_and(|error| !error.is_null());
                if failed && !shell {
                    Some(encode(&value["error"], io)?)
                } else if failed {
                    let detail = value
                        .get("error")
                        .filter(|error| !error.is_null())
                        .or_else(|| {
                            value.get("stderr").filter(|stderr| {
                                stderr.as_str().is_some_and(|stderr| !stderr.is_empty())
                            })
                        })
                        .unwrap_or(&value["stdout"]);
                    Some(encode(&(&value["exit_code"], &value["stop"], detail), io)?)
                } else {
                    None
                }
            }
            _ => None,
        };
        _error_hold = error
            .as_ref()
            .map(|error| io.map(|io| io.guard_bytes(error)).transpose())
            .transpose()?;
        if error.is_none() && policy.quiet_ms.is_none() {
            return Ok(None);
        }
        let mut projection = queries
            .project(
                source,
                &Observation::ToolFinished {
                    command: signature,
                    error: error.as_deref(),
                },
                policy,
            )
            .await?;
        if policy.quiet_ms.is_some() && !background_started {
            self.tool_job_projection(
                &mut projection,
                crate::supervision::tool_jobs::Record {
                    version: 1,
                    call: invocation.operation_id,
                    event: crate::supervision::tool_jobs::Event::Finished,
                },
                io,
            )
            .await?;
        }
        Ok(Some(projection))
    }

    async fn tool_job_projection(
        &self,
        projection: &mut Projection,
        event: crate::supervision::tool_jobs::Record,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        use acyclic_stream::{AppendRecord, CommitCondition, CommitMutation};
        let Some((agent, _, _, _)) = &self.agent_events else {
            return Ok(());
        };
        let path = crate::supervision::tool_jobs::path(agent)?;
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|work| work as &mut dyn crate::store::HistoryIo);
        let through =
            crate::store::operations::tail_optional_with_work(&self.stream, &path, &mut work)
                .await?;
        let value = encode(&event, io)?;
        let call = super::super::bounded_io::clone_bytes(&event.call.into_bytes(), io)?;
        let headers = if matches!(
            event.event,
            crate::supervision::tool_jobs::Event::Started { .. }
        ) {
            indexed_headers(
                [
                    call,
                    super::super::bounded_io::clone_bytes(
                        crate::supervision::tool_jobs::STARTED_KEY,
                        io,
                    )?,
                ],
                io,
            )?
        } else {
            indexed_headers([call], io)?
        };
        let record = AppendRecord { value, headers };
        let mut records = crate::model::vec_with_capacity(1, io)?;
        records.push(record);
        crate::executor::stock_vec_push(
            &mut projection.effect.conditions,
            match through {
                Some(expected) => CommitCondition::Tail {
                    path: path.clone(),
                    expected,
                },
                None => CommitCondition::Absent { path: path.clone() },
            },
            io,
        )?;
        crate::executor::stock_vec_push(
            &mut projection.effect.mutations,
            CommitMutation::Append { path, records },
            io,
        )?;
        Ok(())
    }
}
