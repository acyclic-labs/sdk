use super::*;
use acyclic_harness::{
    wire,
    wire_api::{
        HarnessWireApi, OperationControlRequest, follow_history, negotiate, operation_status,
        validate_cancel_request, validate_observe_request, validate_resume_protocol,
    },
    wire_codec::{decode_command_envelope, encode_authority},
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use futures::{StreamExt, stream};
impl NativeSession {
    async fn control(
        &self,
        request: &OperationControlRequest,
    ) -> Result<acyclic_harness::distributed::DistributedCoordinator<LocalStream>> {
        let verifier = self.config.owner.verifier()?;
        request.authenticate(&verifier)?;
        let reader = FilesystemContentVerifier::new(
            self.files.clone(),
            &verifier,
            &self.config.owner.scope,
            MAX_FRAME_BYTES as u64,
            None,
        )?;
        let coordinator = acyclic_harness::distributed::DistributedCoordinator::open(
            &self.stream,
            reader,
            None,
            &mut None,
        )
        .await?;
        let state = coordinator
            .scheduler()
            .operation(request.operation_id)
            .ok_or_else(|| Error::NotFound("operation".into()))?;
        let owner = match &state.spec.owner {
            acyclic_harness::scheduler::DurableOwner::Attached { authority }
            | acyclic_harness::scheduler::DurableOwner::Detached { authority } => authority,
        };
        if owner != &request.owner {
            return Err(Error::Unauthorized(
                "operation belongs to another owner".into(),
            ));
        }
        verifier.verify(&state.spec.owner_scope, &mut None)?;
        if !state
            .spec
            .owner_scope
            .capabilities()
            .contains("operation:declare")
        {
            return Err(Error::Unauthorized(
                "operation lacks its original declaration grant".into(),
            ));
        }
        Ok(coordinator)
    }
    pub fn start_cursor(&self) -> wire::ReplayCursor {
        wire::ReplayCursor {
            authority: Some(encode_authority(&self.config.owner.authority)),
            generation: self.config.generation.clone(),
            revision: 0,
        }
    }
    async fn replay_cut(&self, authority: &wire::Authority) -> Result<wire::ReplayCursor> {
        let binding = self.binding(authority)?;
        let reader = HistoryReader::new(
            &self.stream,
            &binding.authority,
            &(binding.verifier()?),
            &mut None,
        )?;
        let pinned = reader.pin(0, None).await?;
        Ok(wire::ReplayCursor {
            authority: Some(authority.clone()),
            generation: self.config.generation.clone(),
            revision: pinned.through_revision,
        })
    }
    fn binding(&self, authority: &wire::Authority) -> Result<&AuthorityBinding> {
        self.config
            .bindings()
            .find(|binding| encode_authority(&binding.authority) == *authority)
            .ok_or_else(|| {
                Error::Unauthorized("authority is not in this native composition".into())
            })
    }
}
impl HarnessWireApi for NativeSession {
    fn acknowledge<'a>(&'a self, request: wire::Acknowledge) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            let authority = request
                .authority
                .as_ref()
                .ok_or_else(|| Error::Invalid("acknowledgement authority missing".into()))?;
            self.binding(authority)?;
            if request.generation != self.config.generation {
                return Err(Error::Conflict("acknowledgement generation changed".into()));
            }
            Ok(())
        })
    }
    fn handshake<'a>(
        &'a self,
        request: wire::HandshakeRequest,
    ) -> BoxProviderFuture<'a, Result<wire::HandshakeResponse>> {
        Box::pin(async move { negotiate(&request, &wire::CapabilitySet::default()) })
    }
    fn submit<'a>(
        &'a self,
        envelope: wire::CommandEnvelope,
    ) -> BoxProviderFuture<'a, Result<wire::Admission>> {
        Box::pin(async move {
            let identity = envelope.operation.clone();
            let (authority, command) = decode_command_envelope(envelope)?;
            let binding = self.binding(&encode_authority(&authority))?;
            let mut aggregate = open_aggregate(
                &self.stream,
                &self.files,
                &self.config.stream_provider,
                binding,
            )
            .await?;
            let (state, error) = match aggregate
                .execute(
                    command,
                    #[cfg(feature = "filesystem")]
                    None,
                    &mut None,
                )
                .await
            {
                Ok(_) => (wire::AdmissionState::Accepted, None),
                Err(error @ Error::Indeterminate(_)) => (
                    wire::AdmissionState::Indeterminate,
                    Some(acyclic_harness::encode_error(&error)),
                ),
                Err(error) => (
                    wire::AdmissionState::Rejected,
                    Some(acyclic_harness::encode_error(&error)),
                ),
            };
            Ok(wire::Admission {
                operation: identity,
                state: state as i32,
                error,
            })
        })
    }
    fn replay<'a>(
        &'a self,
        request: wire::ResumeRequest,
    ) -> BoxProviderFuture<'a, Result<BoxProviderStream<'static, Result<wire::Delivery>>>> {
        Box::pin(async move {
            validate_resume_protocol(&request)?;
            let [cursor] = <[_; 1]>::try_from(request.cursors).map_err(|_| {
                Error::Invalid("terminal inspection requires one explicit aggregate cursor".into())
            })?;
            if cursor.generation != self.config.generation {
                return Err(Error::Conflict("native replay generation changed".into()));
            }
            let authority = cursor
                .authority
                .as_ref()
                .ok_or_else(|| Error::Invalid("cursor authority missing".into()))?;
            let binding = self.binding(authority)?;
            follow_history(
                &self.stream,
                &binding.verifier()?,
                cursor,
                (MAX_FRAME_BYTES - 8192) as u64,
            )
            .await
        })
    }
    fn authorize_operation_control<'a>(
        &'a self,
        request: &'a OperationControlRequest,
    ) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move { self.control(request).await.map(|_| ()) })
    }
    fn observe<'a>(
        &'a self,
        request: wire::ObserveRequest,
    ) -> BoxProviderFuture<'a, Result<wire::OperationStatus>> {
        Box::pin(async move {
            let control = validate_observe_request(&request)?;
            let coordinator = self.control(&control).await?;
            let mut status = operation_status(
                coordinator
                    .scheduler()
                    .operation(control.operation_id)
                    .ok_or_else(|| Error::NotFound("operation".into()))?,
            );
            status.replay_through = Some(self.replay_cut(&encode_authority(&control.owner)).await?);
            Ok(status)
        })
    }
    fn watch_operation<'a>(
        &'a self,
        request: wire::ObserveRequest,
    ) -> BoxProviderFuture<'a, Result<BoxProviderStream<'static, Result<wire::OperationStatus>>>>
    {
        Box::pin(async move {
            let control = validate_observe_request(&request)?;
            let coordinator = self.control(&control).await?;
            let source = coordinator.operation_changes(control.operation_id).await?;
            let binding = self.binding(&encode_authority(&control.owner))?.clone();
            let stream = self.stream.clone();
            let generation = self.config.generation.clone();
            Ok(Box::pin(stream::try_unfold(
                (source, binding, stream, generation),
                |(mut source, binding, stream, generation)| async move {
                    let Some(mut status) = source.next().await.transpose()? else {
                        return Ok(None);
                    };
                    let reader = HistoryReader::new(
                        &stream,
                        &binding.authority,
                        &(binding.verifier()?),
                        &mut None,
                    )?;
                    let pinned = reader.pin(0, None).await?;
                    status.replay_through = Some(wire::ReplayCursor {
                        authority: status.owner.clone(),
                        generation: generation.clone(),
                        revision: pinned.through_revision,
                    });
                    Ok(Some((status, (source, binding, stream, generation))))
                },
            ))
                as BoxProviderStream<
                    'static,
                    Result<wire::OperationStatus>,
                >)
        })
    }
    fn cancel<'a>(
        &'a self,
        request: wire::CancelRequest,
    ) -> BoxProviderFuture<'a, Result<wire::CancelResponse>> {
        Box::pin(async move {
            let control = validate_cancel_request(&request)?;
            let mut coordinator = self.control(&control.control).await?;
            let (_, state) = coordinator
                .cancel_operation(
                    &control.control.owner,
                    &control.control.scope,
                    &self.config.owner.verifier()?,
                    control.control.operation_id,
                    control.idempotency_key.clone(),
                    control.recursive,
                    None,
                    &mut None,
                )
                .await?;
            let mut status = operation_status(&state);
            status.replay_through = Some(
                self.replay_cut(&encode_authority(&control.control.owner))
                    .await?,
            );
            Ok(wire::CancelResponse {
                status: Some(status),
                operation: Some(wire::OperationIdentity {
                    operation_id: control.control.operation_id.to_string(),
                    idempotency_key: control.idempotency_key.0,
                }),
            })
        })
    }
}
