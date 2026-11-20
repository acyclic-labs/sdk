use super::*;

impl<E: RpcEndpoint> Provider<E> {
    pub(super) async fn guest_read_file(
        &self,
        machine: MachineId,
        access: FileAccess,
        limit: u32,
        observation: &obs::Span,
    ) -> Result<FileRead, ProviderError> {
        if limit == 0 {
            return Err(ProviderError::Invalid(
                "guest file read admission failed; recovery: supply a positive chunk length".into(),
            ));
        }
        access.validate_range(limit as usize)?;
        let deadline = self.endpoint.absolute_deadline(access.deadline_unix_ms())?;
        let mut client = self.file_client(&deadline).await?;
        let (input, _input_guard) =
            duplex::input(stream::iter(std::iter::once(wire::ReadFileRequest {
                protocol: Some(protocol()),
                access: Some(encode_access(machine, &access)),
                limit,
            })));
        let mut request = Request::new(input);
        if let Some(remaining) = deadline.remaining() {
            request.set_timeout(remaining);
        }
        let mut output = transport::dispatch(Some(&deadline), client.read_file(request), |stop| {
            stop.file(machine)
        })
        .await?
        .map_err(|error| {
            obs::failed_rpc(observation, &error);
            guest_error(ProviderError::FileIndeterminate(machine), error)
        })?
        .into_inner();
        let response = deadline
            .run(output.message())
            .await
            .map_err(|stop| stop.file(machine))?
            .map_err(|error| ProviderError::FileIndeterminate(machine).with_cause(error))?
            .ok_or(ProviderError::FileIndeterminate(machine))?;
        decode_read_response(response, machine, access.offset(), limit)
    }

    pub(super) async fn guest_write_file(
        &self,
        machine: MachineId,
        access: FileAccess,
        data: Vec<u8>,
        truncate: bool,
        observation: &obs::Span,
    ) -> Result<FileWrite, ProviderError> {
        access.validate_range(data.len())?;
        if truncate && access.offset() != 0 {
            return Err(ProviderError::Invalid("file truncation admission failed; recovery: truncate only the first chunk at offset zero".into()));
        }
        let length = data.len();
        let deadline = self.endpoint.absolute_deadline(access.deadline_unix_ms())?;
        let mut client = self.file_client(&deadline).await?;
        let (input, _input_guard) =
            duplex::input(stream::iter(std::iter::once(wire::WriteFileRequest {
                protocol: Some(protocol()),
                access: Some(encode_access(machine, &access)),
                data,
                truncate,
            })));
        let mut request = Request::new(input);
        if let Some(remaining) = deadline.remaining() {
            request.set_timeout(remaining);
        }
        let mut output = transport::dispatch(Some(&deadline), client.write_file(request), |stop| {
            stop.file(machine)
        })
        .await?
        .map_err(|error| {
            obs::failed_rpc(observation, &error);
            guest_error(ProviderError::FileIndeterminate(machine), error)
        })?
        .into_inner();
        let response = deadline
            .run(output.message())
            .await
            .map_err(|stop| stop.file(machine))?
            .map_err(|error| ProviderError::FileIndeterminate(machine).with_cause(error))?
            .ok_or(ProviderError::FileIndeterminate(machine))?;
        decode_write_response(&response, machine, access.offset(), length)
    }

    async fn file_client(&self, deadline: &E::Deadline) -> Result<E::Client, ProviderError> {
        deadline
            .run(self.client())
            .await
            .map_err(DeadlineStop::before_dispatch)?
    }
}
