use super::*;

pub fn decode_read_response(
    response: wire::ReadFileResponse,
    machine: MachineId,
    offset: u64,
    limit: u32,
) -> Result<FileRead, ProviderError> {
    check_access(response.machine.as_ref(), response.offset, machine, offset)?;
    match response.result {
        Some(wire::read_file_response::Result::Data(data)) if data.len() <= limit as usize => {
            Ok(FileRead::Data(data))
        }
        Some(wire::read_file_response::Result::Errno(errno)) => {
            Ok(FileRead::Errno(decode_errno(errno)?))
        }
        _ => Err(ProviderError::Rejected(
            "file read result invalid; recovery: repair the native producer's terminal response"
                .into(),
        )),
    }
}

pub fn decode_write_response(
    response: &wire::WriteFileResponse,
    machine: MachineId,
    offset: u64,
    length: usize,
) -> Result<FileWrite, ProviderError> {
    check_access(response.machine.as_ref(), response.offset, machine, offset)
        .and_then(|()| match response.result {
            Some(wire::write_file_response::Result::Written(written))
                if written as usize <= length => Ok(FileWrite::Written(written)),
            Some(wire::write_file_response::Result::Errno(errno)) => {
                Ok(FileWrite::Errno(decode_errno(errno)?))
            }
            _ => Err(ProviderError::Rejected("file write result invalid; recovery: repair the native producer's terminal response".into())),
        })
        // Dispatch may already have changed the file. An unverified response
        // cannot establish rejection or authorize another write.
        .map_err(|_| ProviderError::FileIndeterminate(machine))
}

pub fn encode_access(machine: MachineId, access: &FileAccess) -> wire::FileAccess {
    wire::FileAccess {
        machine: Some(encode_machine(machine)),
        path: access.path.clone(),
        offset: access.offset,
        deadline_unix_ms: access.deadline_unix_ms,
    }
}

fn check_access(
    actual: Option<&wire::MachineId>,
    offset: u64,
    machine: MachineId,
    expected_offset: u64,
) -> Result<(), ProviderError> {
    if decode_machine(actual)? != machine || offset != expected_offset {
        return Err(ProviderError::Rejected("file result identity substituted; recovery: reconcile the original guest file operation".into()));
    }
    Ok(())
}

fn decode_errno(errno: u32) -> Result<NonZeroU32, ProviderError> {
    NonZeroU32::new(errno).ok_or_else(|| {
        ProviderError::Rejected(
            "file result errno is zero; recovery: return native bytes or written count for success"
                .into(),
        )
    })
}
