use super::*;

impl GrpcProvider {
    pub(super) async fn guest_read_file(
        &self,
        machine: MachineId,
        access: FileAccess,
        limit: u32,
    ) -> Result<FileRead, ProviderError> {
        if limit == 0 {
            return Err(ProviderError::Invalid(
                "guest file read admission failed; recovery: supply a positive chunk length".into(),
            ));
        }
        access.range(limit as usize)?;
        let deadline = GuestDeadline::absolute(access.deadline_unix_ms)?;
        let mut client = self.file_client(&deadline).await?;
        let mut request = Request::new(wire::ReadFileRequest {
            protocol: Some(protocol()),
            access: Some(encode_access(machine, &access)),
            limit,
        });
        request.set_timeout(deadline.remaining());
        let response = deadline
            .run(client.read_file(request))
            .await
            .ok_or(ProviderError::FileIndeterminate(machine))?
            .map_err(|error| file_error(machine, &error))?
            .into_inner();
        check_access(
            response.machine.as_ref(),
            response.offset,
            machine,
            access.offset,
        )?;
        match response.result {
            Some(wire::read_file_response::Result::Data(data)) if data.len() <= limit as usize => Ok(FileRead::Data(data)),
            Some(wire::read_file_response::Result::Errno(errno)) => Ok(FileRead::Errno(decode_errno(errno)?)),
            _ => Err(ProviderError::Rejected("file read result invalid; recovery: repair the native producer's terminal response".into())),
        }
    }

    pub(super) async fn guest_write_file(
        &self,
        machine: MachineId,
        access: FileAccess,
        data: Vec<u8>,
        truncate: bool,
    ) -> Result<FileWrite, ProviderError> {
        access.range(data.len())?;
        if truncate && access.offset != 0 {
            return Err(ProviderError::Invalid("file truncation admission failed; recovery: truncate only the first chunk at offset zero".into()));
        }
        let length = data.len();
        let deadline = GuestDeadline::absolute(access.deadline_unix_ms)?;
        let mut client = self.file_client(&deadline).await?;
        let mut request = Request::new(wire::WriteFileRequest {
            protocol: Some(protocol()),
            access: Some(encode_access(machine, &access)),
            data,
            truncate,
        });
        request.set_timeout(deadline.remaining());
        let response = deadline
            .run(client.write_file(request))
            .await
            .ok_or(ProviderError::FileIndeterminate(machine))?
            .map_err(|error| file_error(machine, &error))?
            .into_inner();
        check_access(
            response.machine.as_ref(),
            response.offset,
            machine,
            access.offset,
        )?;
        match response.result {
            Some(wire::write_file_response::Result::Written(written)) if written as usize <= length => Ok(FileWrite::Written(written)),
            Some(wire::write_file_response::Result::Errno(errno)) => Ok(FileWrite::Errno(decode_errno(errno)?)),
            _ => Err(ProviderError::Rejected("file write result invalid; recovery: repair the native producer's terminal response".into())),
        }
    }

    async fn file_client(&self, deadline: &GuestDeadline) -> Result<Client, ProviderError> {
        deadline.run(self.client()).await.ok_or_else(|| ProviderError::Rejected("guest file deadline expired before dispatch; recovery: obtain a fresh admitted deadline".into()))?
    }
}

fn encode_access(machine: MachineId, access: &FileAccess) -> wire::FileAccess {
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

fn file_error(machine: MachineId, error: &Status) -> ProviderError {
    match command_error(machine, error) {
        ProviderError::CommandIndeterminate(_) => ProviderError::FileIndeterminate(machine),
        cause => cause,
    }
}
