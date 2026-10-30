use super::*;

pub(super) fn command_error(machine: MachineId, value: &tonic::Status) -> ProviderError {
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
        Some(wire::exec_output::Output::Data(data))
            if !data.bytes.is_empty() && data.bytes.len() <= MAX_OUTPUT_READ_BYTES as usize =>
        {
            match wire::ExecStream::try_from(data.stream) {
                Ok(wire::ExecStream::Stdout) => Ok(ExecOutput::Data(data.bytes)),
                Ok(wire::ExecStream::Stderr) => Ok(ExecOutput::Stderr(data.bytes)),
                _ => Err(ProviderError::Rejected("exec output channel is invalid; recovery: return STDOUT or STDERR on the original stream".into())),
            }
        }
        Some(wire::exec_output::Output::ExitCode(code)) => {
            *exited = true;
            Ok(ExecOutput::ExitCode(code))
        }
        Some(wire::exec_output::Output::Stopped(stop)) => {
            let kind =
                match wire::ExecStopKind::try_from(stop.kind) {
                    Ok(wire::ExecStopKind::Timeout) => ExecStopKind::Timeout,
                    Ok(wire::ExecStopKind::Expired) => ExecStopKind::Expired,
                    Ok(wire::ExecStopKind::OutputLimit) => ExecStopKind::OutputLimit,
                    Ok(wire::ExecStopKind::ControlStopped) => ExecStopKind::ControlStopped,
                    Ok(wire::ExecStopKind::CaptureFailed) => ExecStopKind::CaptureFailed,
                    _ => return Err(ProviderError::Rejected(
                        "exec stop kind is invalid; recovery: repair the producer's terminal frame"
                            .into(),
                    )),
                };
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
