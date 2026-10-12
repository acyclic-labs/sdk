//! Builtin events enter only after the shared service caller Scope verifier.
use super::*;
use crate::pi_builtin::invalid;
use crate::pi_builtin::{
    MAXIMUM_BUILTIN_BYTES, PI_BUILTIN_COMMAND_KIND, PiBuiltinIo, PiBuiltinRequest,
};
use acyclic_harness::Result as RootResult;

impl PiSession {
    pub(super) async fn native_builtin_control(
        &mut self,
        caller: TaskId,
        event: &str,
        arguments: &Value,
        owner: &pi_owner::PiOwnerConfig,
        query_work: &acyclic_fs::NativeMaterializationWork,
    ) -> RootResult<ControlDispatchResponse> {
        self.runtime
            .verify_owner_with_native_work(owner, query_work)?;
        let native = self.native_host_with_native_work(query_work)?;
        let work = if caller == owner.policy().root_task {
            query_work.clone()
        } else {
            native.invocation_work(caller).await?
        };
        let request_bytes = bounded_input_size(arguments)?;
        let _request_custody = work.claim(request_bytes, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
        let request = builtin_request(caller, event, arguments)?;
        request.validate()?;
        let (value, custody) = if caller == owner.policy().root_task {
            request.require_root_source_read(owner.policy().root_task)?;
            let host = self.command_host(&work, Some(native.clone()))?;
            let (payload, _payload_custody) =
                pi_root_provider::guarded_pi_value(&request, MAXIMUM_BUILTIN_BYTES * 2, &work)?;
            let operation = request.operation(owner.policy().root_task, &work)?;
            let staged = pi_root_provider::stage_pi_input(
                owner,
                &self.filesystem,
                operation,
                "command",
                &payload,
                &work,
            )
            .await?;
            let _reference_custody = staged.1;
            let reference = staged.0;
            let command_kind = work.display(PI_BUILTIN_COMMAND_KIND)?;
            let _kind_custody = command_kind.1;
            let kind = command_kind.0;
            let command = acyclic_harness::workflow::WorkflowCommand {
                operation_id: operation,
                kind,
                payload: reference,
            };
            host.control_command_with_native_work(command).await?
        } else {
            self.native_child_builtin_command(owner, &native, &request, &work)
                .await?
        };
        let binding = self.binding(caller)?;
        #[derive(Serialize)]
        struct Response<'a> {
            session: &'a str,
            native_session: &'a str,
            caller_task: TaskId,
            operation: &'a str,
            event: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            phase: Option<&'a str>,
            #[serde(flatten)]
            body: &'a Value,
        }
        let response = Response {
            session: &self.state.root_session_id,
            native_session: &binding.native_session_id,
            caller_task: caller,
            operation: &request.sdk_operation,
            event,
            phase: match request.io {
                PiBuiltinIo::EditRead { .. } => Some("read"),
                PiBuiltinIo::EditWrite { .. } => Some("write"),
                _ => None,
            },
            body: &value,
        };
        let (response, response_custody) =
            pi_root_provider::guarded_pi_value(&response, MAXIMUM_BUILTIN_BYTES * 2, &work)?;
        drop(value);
        drop(custody);
        Ok((response, Some(response_custody)))
    }

    async fn native_child_builtin_command(
        &self,
        owner: &pi_owner::PiOwnerConfig,
        native: &Arc<pi_native_host::PiNativeCommandHost>,
        request: &PiBuiltinRequest,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> RootResult<(Value, acyclic_fs::NativeAllocation)> {
        use acyclic_harness::filesystem::{
            TOOL_TASK_COMMAND_KIND, TaskCommandHost as _, TaskCommandProgress,
        };
        let live = native
            .live_source(request.caller, work)
            .await?
            .ok_or_else(|| invalid("builtin caller has no current original native invocation"))?;
        crate::pi_builtin::authorize_owner(
            live.context.context(),
            &live.owner,
            request.caller,
            work,
        )
        .await?;
        if live.prepared.binding.task_id != request.caller {
            return Err(invalid(
                "builtin command substituted the original native task binding",
            ));
        }
        let original_fence = live.owner.task_binding().1;
        let reservation = work.display(&original_fence.reservation_id)?;
        let _reservation_custody = reservation.1;
        let placement = work.display(&original_fence.placement)?;
        let _placement_custody = placement.1;
        let fence = acyclic_harness::scheduler::LeaseFence {
            reservation_id: reservation.0,
            placement: placement.0,
        };
        #[derive(Serialize)]
        struct ToolCommand<'a> {
            name: &'a str,
            revision: &'a str,
            arguments: &'a PiBuiltinRequest,
        }
        let payload = pi_root_provider::guarded_pi_value(
            &ToolCommand {
                name: PI_BUILTIN_COMMAND_KIND,
                revision: crate::pi_builtin::PI_BUILTIN_TOOL_REVISION,
                arguments: request,
            },
            MAXIMUM_BUILTIN_BYTES * 2,
            work,
        )?;
        let _payload_custody = payload.1;
        let operation = request.operation(owner.policy().root_task, &work)?;
        let staged = pi_root_provider::stage_pi_input(
            owner,
            &self.filesystem,
            operation,
            "command",
            &payload.0,
            work,
        )
        .await?;
        let _reference_custody = staged.1;
        let kind = work.display(TOOL_TASK_COMMAND_KIND)?;
        let _kind_custody = kind.1;
        let command = acyclic_harness::workflow::WorkflowCommand {
            operation_id: operation,
            kind: kind.0,
            payload: staged.0,
        };
        let commands = self.runtime.runtime.commands();
        match commands
            .execute_with_native_work(live.context.clone(), fence, command, payload.0, work)?
            .await?
        {
            TaskCommandProgress::ReadyWithNativeCustody { value, custody } => Ok((value, custody)),
            TaskCommandProgress::Ready(_) => Err(invalid(
                "native builtin acknowledgement lost its original SDK custody",
            )),
            TaskCommandProgress::Pending | TaskCommandProgress::Indeterminate => {
                Err(acyclic_harness::Error::Indeterminate(operation))
            }
        }
    }
}

fn bounded_input_size(arguments: &Value) -> RootResult<u64> {
    let operation = bounded_string(arguments, "operation", 512)?;
    let path = bounded_string(arguments, "path", 4096)?;
    let content = match arguments.get("content") {
        None => "",
        Some(Value::String(value)) if value.len() as u64 <= MAXIMUM_BUILTIN_BYTES => value.as_str(),
        _ => return Err(invalid("builtin content must be bounded UTF-8")),
    };
    let digest = match arguments.get("expected_sha256") {
        None => "",
        Some(Value::String(value)) if value.len() == 64 => value,
        _ => return Err(invalid("builtin edit digest must name the exact preimage")),
    };
    let mut bytes = (operation.len() + path.len() + content.len() + digest.len()) as u64;
    if let Some(variants) = arguments.get("variants") {
        let variants = variants
            .as_array()
            .filter(|variants| variants.len() <= 4)
            .ok_or_else(|| invalid("builtin variants require at most four paths"))?;
        bytes += variants.len() as u64 * std::mem::size_of::<String>() as u64;
        for path in variants {
            bytes += path
                .as_str()
                .filter(|path| path.len() <= 4096)
                .ok_or_else(|| invalid("builtin variant path is not bounded"))?
                .len() as u64;
        }
    }
    Ok(bytes)
}

fn builtin_request(caller: TaskId, event: &str, arguments: &Value) -> RootResult<PiBuiltinRequest> {
    let maximum = || {
        arguments
            .get("maximum_bytes")
            .and_then(Value::as_u64)
            .filter(|maximum| (1..=MAXIMUM_BUILTIN_BYTES).contains(maximum))
            .ok_or_else(|| invalid("builtin read requires its original bounded byte allowance"))
    };
    let io = match event {
        "NativeBuiltinRead" => PiBuiltinIo::Read {
            maximum_bytes: maximum()?,
            variants: arguments
                .get("variants")
                .map(|value| {
                    value
                        .as_array()
                        .ok_or_else(|| invalid("invalid builtin variants"))?
                        .iter()
                        .map(|value| {
                            value
                                .as_str()
                                .map(str::to_owned)
                                .ok_or_else(|| invalid("invalid builtin variant"))
                        })
                        .collect::<RootResult<Vec<_>>>()
                })
                .transpose()?
                .unwrap_or_default(),
        },
        "NativeBuiltinWrite" => PiBuiltinIo::Write {
            content: bounded_string(arguments, "content", MAXIMUM_BUILTIN_BYTES as usize)?
                .to_owned(),
        },
        "NativeBuiltinEdit" => match bounded_string(arguments, "phase", 5)? {
            "read" => PiBuiltinIo::EditRead {
                maximum_bytes: maximum()?,
            },
            "write" => PiBuiltinIo::EditWrite {
                content: bounded_string(arguments, "content", MAXIMUM_BUILTIN_BYTES as usize)?
                    .to_owned(),
                expected_sha256: bounded_string(arguments, "expected_sha256", 64)?.to_owned(),
            },
            _ => return Err(invalid("unknown original builtin edit phase")),
        },
        _ => return Err(invalid("unknown original builtin IO event")),
    };
    Ok(PiBuiltinRequest {
        caller,
        sdk_operation: bounded_string(arguments, "operation", 512)?.to_owned(),
        path: bounded_string(arguments, "path", 4096)?.to_owned(),
        io,
    })
}

fn bounded_string<'a>(arguments: &'a Value, name: &str, maximum: usize) -> RootResult<&'a str> {
    arguments
        .get(name)
        .and_then(Value::as_str)
        .filter(|value| value.len() <= maximum)
        .ok_or_else(|| invalid("builtin field must be a bounded string"))
}
