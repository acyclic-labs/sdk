#![deny(unsafe_code)]
//! Tonic server adapter over the transport-neutral Harness wire API.

use acyclic_harness::{
    Error, wire,
    wire_api::{
        HarnessWireApi, validate_admission, validate_cancel_request, validate_cancel_response,
        validate_command_protocol, validate_observe_request, validate_operation_status,
        validate_resume_protocol,
    },
};
use futures::{StreamExt as _, stream::BoxStream};
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// Generated tonic client and server surfaces using the canonical Harness messages.
#[allow(missing_docs)]
pub mod transport {
    include!(concat!(env!("OUT_DIR"), "/acyclic.harness.v1.rs"));
}

/// Thin tonic service; all admission and replay semantics belong to [`HarnessWireApi`].
#[derive(Clone)]
pub struct HarnessGrpcService {
    api: Arc<dyn HarnessWireApi>,
}

impl HarnessGrpcService {
    /// Binds a transport-neutral implementation.
    #[must_use]
    pub fn new(api: Arc<dyn HarnessWireApi>) -> Self {
        Self { api }
    }

    /// Wraps this implementation as a tonic server service.
    #[must_use]
    pub fn into_server(self) -> transport::harness_service_server::HarnessServiceServer<Self> {
        transport::harness_service_server::HarnessServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl transport::harness_service_server::HarnessService for HarnessGrpcService {
    async fn handshake(
        &self,
        request: Request<wire::HandshakeRequest>,
    ) -> Result<Response<wire::HandshakeResponse>, Status> {
        self.api
            .handshake(request.into_inner())
            .await
            .map(Response::new)
            .map_err(status)
    }

    async fn submit(
        &self,
        request: Request<wire::CommandEnvelope>,
    ) -> Result<Response<wire::Admission>, Status> {
        let command = request.into_inner();
        validate_command_protocol(&command).map_err(status)?;
        let admission = self.api.submit(command.clone()).await.map_err(status)?;
        validate_admission(&command, &admission).map_err(status)?;
        Ok(Response::new(admission))
    }

    type ReplayStream = BoxStream<'static, Result<wire::Delivery, Status>>;

    async fn replay(
        &self,
        request: Request<wire::ResumeRequest>,
    ) -> Result<Response<Self::ReplayStream>, Status> {
        validate_resume_protocol(request.get_ref()).map_err(status)?;
        let stream = self
            .api
            .replay(request.into_inner())
            .await
            .map_err(status)?
            .map(|item| item.map_err(status));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn observe(
        &self,
        request: Request<wire::ObserveRequest>,
    ) -> Result<Response<wire::OperationStatus>, Status> {
        let control = validate_observe_request(request.get_ref()).map_err(status)?;
        self.api
            .authorize_operation_control(&control)
            .await
            .map_err(status)?;
        let request = request.into_inner();
        let response = self.api.observe(request.clone()).await.map_err(status)?;
        validate_operation_status(&request, &response).map_err(status)?;
        Ok(Response::new(response))
    }

    async fn cancel(
        &self,
        request: Request<wire::CancelRequest>,
    ) -> Result<Response<wire::CancelResponse>, Status> {
        let (control, _, _) = validate_cancel_request(request.get_ref()).map_err(status)?;
        self.api
            .authorize_operation_control(&control)
            .await
            .map_err(status)?;
        let request = request.into_inner();
        let response = self.api.cancel(request.clone()).await.map_err(status)?;
        validate_cancel_response(&request, &response).map_err(status)?;
        Ok(Response::new(response))
    }
}

fn status(error: Error) -> Status {
    match error {
        Error::NotFound(message) => Status::not_found(message),
        Error::Conflict(message) => Status::aborted(message),
        Error::Unsupported(message) => Status::unimplemented(message),
        Error::Invalid(message) => Status::invalid_argument(message),
        Error::Unauthorized(message) => Status::permission_denied(message),
        Error::Storage(message) => Status::unavailable(message),
        Error::Indeterminate(operation) => {
            Status::unavailable(format!("operation outcome is indeterminate: {operation}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_harness::{
        Result,
        wire_api::{current_protocol, negotiate},
    };
    use futures::{FutureExt as _, stream};

    struct FakeApi;

    impl HarnessWireApi for FakeApi {
        fn authorize_operation_control<'a>(
            &'a self,
            _: &'a acyclic_harness::wire_api::OperationControlRequest,
        ) -> futures::future::BoxFuture<'a, Result<()>> {
            async { Ok(()) }.boxed()
        }

        fn handshake<'a>(
            &'a self,
            request: wire::HandshakeRequest,
        ) -> futures::future::BoxFuture<'a, Result<wire::HandshakeResponse>> {
            async move { negotiate(&request, &wire::CapabilitySet::default()) }.boxed()
        }

        fn submit<'a>(
            &'a self,
            command: wire::CommandEnvelope,
        ) -> futures::future::BoxFuture<'a, Result<wire::Admission>> {
            async move {
                Ok(wire::Admission {
                    operation: command.operation,
                    state: wire::AdmissionState::Accepted as i32,
                    error: None,
                })
            }
            .boxed()
        }

        fn replay<'a>(
            &'a self,
            _: wire::ResumeRequest,
        ) -> futures::future::BoxFuture<
            'a,
            Result<futures::stream::BoxStream<'static, Result<wire::Delivery>>>,
        > {
            async move {
                Ok(Box::pin(stream::once(async {
                    Ok(wire::Delivery {
                        authority: None,
                        generation: "generation".into(),
                        from_revision: 0,
                        through_revision: 0,
                        events: Vec::new(),
                        live: true,
                    })
                })) as _)
            }
            .boxed()
        }

        fn observe<'a>(
            &'a self,
            request: wire::ObserveRequest,
        ) -> futures::future::BoxFuture<'a, Result<wire::OperationStatus>> {
            async move {
                Ok(wire::OperationStatus {
                    operation: Some(wire::OperationIdentity {
                        operation_id: request.operation_id,
                        idempotency_key: String::new(),
                    }),
                    state: wire::CompletionState::Running as i32,
                    error: None,
                    protocol: request.protocol,
                    owner: request.owner,
                    cancellation_requested: false,
                    revision: 1,
                })
            }
            .boxed()
        }

        fn cancel<'a>(
            &'a self,
            request: wire::CancelRequest,
        ) -> futures::future::BoxFuture<'a, Result<wire::CancelResponse>> {
            async move {
                let operation = Some(wire::OperationIdentity {
                    operation_id: request.operation_id,
                    idempotency_key: request.idempotency_key,
                });
                Ok(wire::CancelResponse {
                    status: Some(wire::OperationStatus {
                        operation: Some(wire::OperationIdentity {
                            operation_id: operation
                                .as_ref()
                                .map_or_else(String::new, |value| value.operation_id.clone()),
                            idempotency_key: String::new(),
                        }),
                        state: wire::CompletionState::Cancelled as i32,
                        error: None,
                        protocol: request.protocol,
                        owner: request.owner,
                        cancellation_requested: false,
                        revision: 2,
                    }),
                    operation,
                })
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn tonic_surface_delegates_to_the_common_wire_api() -> Result<()> {
        use transport::harness_service_server::HarnessService as _;

        let service = HarnessGrpcService::new(Arc::new(FakeApi));
        let response = service
            .handshake(Request::new(wire::HandshakeRequest {
                protocol: Some(current_protocol()),
                required: Some(wire::CapabilitySet::default()),
            }))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .into_inner();
        assert_eq!(response.protocol, Some(current_protocol()));

        let replay = service
            .replay(Request::new(wire::ResumeRequest {
                protocol: Some(current_protocol()),
                cursors: Vec::new(),
            }))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .into_inner()
            .next()
            .await
            .ok_or_else(|| Error::Storage("missing delivery".into()))?
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert!(replay.live);

        let operation_id = acyclic_harness::OperationId::from_bytes([12; 16]).to_string();
        let owner = wire::Authority {
            kind: wire::AggregateKind::Task as i32,
            id: "owner".into(),
        };
        let scope = wire::Scope {
            id: "control".into(),
            capabilities: vec!["operation:observe".into(), "operation:cancel".into()],
            issuer: "runtime".into(),
            parent_proof: Vec::new(),
            proof: vec![1; 32],
        };
        let observed = service
            .observe(Request::new(wire::ObserveRequest {
                protocol: Some(current_protocol()),
                owner: Some(owner.clone()),
                operation_id: operation_id.clone(),
                scope: Some(scope.clone()),
            }))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .into_inner();
        assert_eq!(
            observed.operation.map(|value| value.operation_id),
            Some(operation_id.clone())
        );

        let operation = wire::OperationIdentity {
            operation_id: operation_id.clone(),
            idempotency_key: "cancel-1".into(),
        };
        let cancelled = service
            .cancel(Request::new(wire::CancelRequest {
                operation_id,
                protocol: Some(current_protocol()),
                owner: Some(owner),
                scope: Some(scope),
                recursive: true,
                idempotency_key: operation.idempotency_key.clone(),
            }))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .into_inner();
        assert_eq!(cancelled.operation, Some(operation));
        Ok(())
    }
}
