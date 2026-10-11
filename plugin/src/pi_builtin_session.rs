//! Builtin events enter only after the shared service caller Scope verifier.
use super::*;
use crate::pi_builtin::{PiBuiltinHost, PiBuiltinIo, PiBuiltinRequest, MAXIMUM_BUILTIN_BYTES, PI_BUILTIN_COMMAND_KIND};
use acyclic_harness::Result as RootResult;
use crate::pi_builtin::{invalid, native_error};

impl PiSession {
    pub(super) async fn native_builtin_control(&mut self, caller: TaskId, event: &str,
        arguments: &Value, owner: &pi_owner::PiOwnerConfig, query_work: &acyclic_fs::NativeMaterializationWork,
    ) -> RootResult<ControlDispatchResponse> {
        self.runtime.verify_owner_with_native_work(owner, query_work)?;
        let native = self.native_host_with_native_work(query_work)?;
        let work = if caller == owner.policy().root_task { query_work.clone() }
            else { native.invocation_work(caller).await? };
        let request_bytes = bounded_input_size(arguments)?;
        // Input transport custody is still alive; reserve the DTO copy before
        // allocating it, and keep that charge through its final use.
        let _request_custody = work.retain_owned(request_bytes).map_err(native_error)?;
        let request = builtin_request(caller, event, arguments)?;
        request.validate()?;
        let host = PiBuiltinHost::new(self.runtime.clone(), self.filesystem.clone(), self.stream.clone(),
            &self.resources, native.original_owner(), &self.state, &self.roots, native.clone(), work.clone())?;
        let (value, custody) = if caller == owner.policy().root_task {
            request.require_root_source_read(owner.policy().root_task)?;
            let (payload, _payload_custody) = pi_root_provider::guarded_pi_value(&request,
                MAXIMUM_BUILTIN_BYTES * 2, &work)?;
            let staged = pi_root_provider::stage_pi_input_with_native_work(owner, &self.filesystem,
                request.operation(owner.policy().root_task), "command", &payload, &work).await?;
            let _reference_custody = staged.1;
            let reference = staged.0;
            let command_kind = work.display(PI_BUILTIN_COMMAND_KIND).map_err(native_error)?;
            let _kind_custody = command_kind.1;
            let kind = command_kind.0;
            let command = acyclic_harness::workflow::WorkflowCommand {
                operation_id: request.operation(owner.policy().root_task),
                kind, payload: reference,
            };
            pi_workflow::PiWorkflowHost::new(&self.relative_directory, self.runtime.clone(), self.filesystem.clone(),
                self.stream.clone(), &self.resources, &self.state).with_builtin_controls(&host)
                .with_work(work.clone()).control_command_with_native_work(command).await?
        } else { host.child_io(&request).await? };
        let binding = self.binding(caller)?;
        #[derive(Serialize)]
        struct Response<'a> {
            session: &'a str, native_session: &'a str, caller_task: TaskId,
            operation: &'a str, event: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")] phase: Option<&'a str>,
            #[serde(flatten)] body: &'a Value,
        }
        let response = Response { session: &self.state.root_session_id, native_session: &binding.native_session_id,
            caller_task: caller, operation: &request.sdk_operation, event,
            phase: match request.io { PiBuiltinIo::EditRead { .. } => Some("read"),
                PiBuiltinIo::EditWrite { .. } => Some("write"), _ => None }, body: &value };
        // Both the original body and its custody remain live while the SDK
        // observes every allocation of the final canonical response.
        let (response, response_custody) = pi_root_provider::guarded_pi_value(&response,
            MAXIMUM_BUILTIN_BYTES * 2, &work)?;
        drop(value);
        drop(custody);
        Ok((response, Some(response_custody)))
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
        None => "", Some(Value::String(value)) if value.len() == 64 => value,
        _ => return Err(invalid("builtin edit digest must name the exact preimage")),
    };
    let mut bytes = (operation.len() + path.len() + content.len() + digest.len()) as u64;
    if let Some(variants) = arguments.get("variants") {
        let variants = variants.as_array().filter(|variants| variants.len() <= 4)
            .ok_or_else(|| invalid("builtin variants require at most four paths"))?;
        bytes += variants.len() as u64 * std::mem::size_of::<String>() as u64;
        for path in variants {
            bytes += path.as_str().filter(|path| path.len() <= 4096)
                .ok_or_else(|| invalid("builtin variant path is not bounded"))?.len() as u64;
        }
    }
    Ok(bytes)
}

fn builtin_request(caller: TaskId, event: &str, arguments: &Value) -> RootResult<PiBuiltinRequest> {
    let maximum = || arguments.get("maximum_bytes").and_then(Value::as_u64)
        .filter(|maximum| (1..=MAXIMUM_BUILTIN_BYTES).contains(maximum))
        .ok_or_else(|| invalid("builtin read requires its original bounded byte allowance"));
    let io = match event {
        "NativeBuiltinRead" => PiBuiltinIo::Read { maximum_bytes: maximum()?,
            variants: arguments.get("variants").map(|value| value.as_array().ok_or_else(|| invalid("invalid builtin variants"))?
                .iter().map(|value| value.as_str().map(str::to_owned).ok_or_else(|| invalid("invalid builtin variant")))
                .collect::<RootResult<Vec<_>>>()).transpose()?.unwrap_or_default() },
        "NativeBuiltinWrite" => PiBuiltinIo::Write { content: bounded_string(arguments, "content", MAXIMUM_BUILTIN_BYTES as usize)?.to_owned() },
        "NativeBuiltinEdit" => match bounded_string(arguments, "phase", 5)? {
            "read" => PiBuiltinIo::EditRead { maximum_bytes: maximum()? },
            "write" => PiBuiltinIo::EditWrite { content: bounded_string(arguments, "content", MAXIMUM_BUILTIN_BYTES as usize)?.to_owned(),
                expected_sha256: bounded_string(arguments, "expected_sha256", 64)?.to_owned() },
            _ => return Err(invalid("unknown original builtin edit phase")),
        },
        _ => return Err(invalid("unknown original builtin IO event")),
    };
    Ok(PiBuiltinRequest { caller, sdk_operation: bounded_string(arguments, "operation", 512)?.to_owned(),
        path: bounded_string(arguments, "path", 4096)?.to_owned(), io })
}

fn bounded_string<'a>(arguments: &'a Value, name: &str, maximum: usize) -> RootResult<&'a str> {
    arguments.get(name).and_then(Value::as_str).filter(|value| value.len() <= maximum)
        .ok_or_else(|| invalid("builtin field must be a bounded string"))
}
