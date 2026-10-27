use super::*;

pub(super) fn command_error(machine: MachineId, value: &tonic::Status) -> ProviderError {
    obs::failed_rpc(value);
    match value.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown
        | tonic::Code::Internal => ProviderError::CommandIndeterminate(machine),
        tonic::Code::Unimplemented => ProviderError::Unsupported(value.message().into()),
        _ => ProviderError::Rejected(value.message().into()),
    }
}

pub(super) fn command_stream_error(
    machine: MachineId,
    exited: bool,
    value: &tonic::Status,
) -> ProviderError {
    obs::failed_rpc(value);
    if exited {
        ProviderError::Rejected("exec stream failed after its terminal result".into())
    } else {
        ProviderError::CommandIndeterminate(machine)
    }
}

pub(super) fn decode_exec_output(
    value: wire::ExecOutput,
    machine: MachineId,
    exited: &mut bool,
) -> Result<ExecOutput, ProviderError> {
    if decode_machine(value.machine.as_ref())? != machine || *exited {
        return Err(ProviderError::Rejected(
            "exec output identity or terminal order is invalid".into(),
        ));
    }
    match value.output {
        Some(wire::exec_output::Output::Stdout(data))
            if !data.is_empty() && data.len() <= MAX_OUTPUT_READ_BYTES as usize =>
        {
            Ok(ExecOutput::Stdout(data))
        }
        Some(wire::exec_output::Output::Stderr(data))
            if !data.is_empty() && data.len() <= MAX_OUTPUT_READ_BYTES as usize =>
        {
            Ok(ExecOutput::Stderr(data))
        }
        Some(wire::exec_output::Output::ExitCode(code)) => {
            *exited = true;
            Ok(ExecOutput::ExitCode(code))
        }
        Some(wire::exec_output::Output::Stopped(stop)) => {
            let kind = match wire::ExecStopKind::try_from(stop.kind) {
                Ok(wire::ExecStopKind::Timeout) => ExecStopKind::Timeout,
                Ok(wire::ExecStopKind::Expired) => ExecStopKind::Expired,
                Ok(wire::ExecStopKind::OutputLimit) => ExecStopKind::OutputLimit,
                Ok(wire::ExecStopKind::ControlStopped) => ExecStopKind::ControlStopped,
                Ok(wire::ExecStopKind::CaptureFailed) => ExecStopKind::CaptureFailed,
                _ => return Err(ProviderError::Rejected("invalid exec stop kind".into())),
            };
            if stop.message.chars().count() > 128 {
                return Err(ProviderError::Rejected(
                    "exec stop message exceeds its bound".into(),
                ));
            }
            *exited = true;
            Ok(ExecOutput::Stopped(ExecStop {
                kind,
                message: stop.message,
                cleanup_completed: stop.cleanup_completed,
            }))
        }
        _ => Err(ProviderError::Rejected("invalid exec output frame".into())),
    }
}

pub(super) fn decode_machine_output(
    value: wire::MachineOutput,
    machine: MachineId,
    offset: u64,
    limit: u32,
) -> Result<MachineOutput, ProviderError> {
    if decode_machine(value.machine.as_ref())? != machine
        || value.offset < offset
        || value.data.len() > limit as usize
        || value.offset.checked_add(value.data.len() as u64).is_none()
    {
        return Err(ProviderError::Rejected(
            "invalid retained console output".into(),
        ));
    }
    Ok(MachineOutput {
        machine,
        offset: value.offset,
        data: value.data,
    })
}
