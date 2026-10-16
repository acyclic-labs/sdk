/// Generated client implementations.
pub mod machines_service_client {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    ///
    #[derive(Debug, Clone)]
    pub struct MachinesServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl MachinesServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> MachinesServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::Body>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + std::marker::Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + std::marker::Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> MachinesServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::Body>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::Body>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::Body>,
            >>::Error: Into<StdError> + std::marker::Send + std::marker::Sync,
        {
            MachinesServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        ///
        pub async fn exec(
            &mut self,
            request: impl tonic::IntoRequest<super::ExecRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::ExecOutput>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/Exec",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("acyclic.machines.v1.MachinesService", "Exec"));
            self.inner.server_streaming(req, path, codec).await
        }
        ///
        pub async fn read_output(
            &mut self,
            request: impl tonic::IntoRequest<super::ReadOutputRequest>,
        ) -> std::result::Result<tonic::Response<super::MachineOutput>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/ReadOutput",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.machines.v1.MachinesService", "ReadOutput"),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn qualify_image(
            &mut self,
            request: impl tonic::IntoRequest<super::QualifyImageRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ImageQualification>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/QualifyImage",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.machines.v1.MachinesService",
                        "QualifyImage",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn create(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateMachineRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MachineAdmission>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/Create",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.machines.v1.MachinesService", "Create"),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn checkpoint(
            &mut self,
            request: impl tonic::IntoRequest<super::CheckpointMachineRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CheckpointAdmission>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/Checkpoint",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.machines.v1.MachinesService", "Checkpoint"),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn fork(
            &mut self,
            request: impl tonic::IntoRequest<super::ForkCheckpointRequest>,
        ) -> std::result::Result<tonic::Response<super::ForkAdmission>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/Fork",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("acyclic.machines.v1.MachinesService", "Fork"));
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn fork_machine(
            &mut self,
            request: impl tonic::IntoRequest<super::ForkMachineRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ForkMachineAdmission>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/ForkMachine",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.machines.v1.MachinesService", "ForkMachine"),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn suspend(
            &mut self,
            request: impl tonic::IntoRequest<super::MachineMutationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MutationAdmission>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/Suspend",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.machines.v1.MachinesService", "Suspend"),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn wake(
            &mut self,
            request: impl tonic::IntoRequest<super::MachineMutationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MutationAdmission>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/Wake",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("acyclic.machines.v1.MachinesService", "Wake"));
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn set_suspension_policy(
            &mut self,
            request: impl tonic::IntoRequest<super::SetSuspensionPolicyRequest>,
        ) -> std::result::Result<
            tonic::Response<super::PolicyAdmission>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/SetSuspensionPolicy",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.machines.v1.MachinesService",
                        "SetSuspensionPolicy",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn destroy_machine(
            &mut self,
            request: impl tonic::IntoRequest<super::MachineMutationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MutationAdmission>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/DestroyMachine",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.machines.v1.MachinesService",
                        "DestroyMachine",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn destroy_checkpoint(
            &mut self,
            request: impl tonic::IntoRequest<super::CheckpointMutationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MutationAdmission>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/DestroyCheckpoint",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.machines.v1.MachinesService",
                        "DestroyCheckpoint",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn recover(
            &mut self,
            request: impl tonic::IntoRequest<super::RecoverRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RecoveredAdmission>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/Recover",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.machines.v1.MachinesService", "Recover"),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn inspect_machine(
            &mut self,
            request: impl tonic::IntoRequest<super::InspectMachineRequest>,
        ) -> std::result::Result<tonic::Response<super::MachineState>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/InspectMachine",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.machines.v1.MachinesService",
                        "InspectMachine",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn inspect_checkpoint(
            &mut self,
            request: impl tonic::IntoRequest<super::InspectCheckpointRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CheckpointState>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/InspectCheckpoint",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.machines.v1.MachinesService",
                        "InspectCheckpoint",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn list_machines(
            &mut self,
            request: impl tonic::IntoRequest<super::ListMachinesRequest>,
        ) -> std::result::Result<tonic::Response<super::MachinePage>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/ListMachines",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.machines.v1.MachinesService",
                        "ListMachines",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn events(
            &mut self,
            request: impl tonic::IntoRequest<super::EventsRequest>,
        ) -> std::result::Result<tonic::Response<super::EventPage>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/Events",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.machines.v1.MachinesService", "Events"),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn usage(
            &mut self,
            request: impl tonic::IntoRequest<super::UsageRequest>,
        ) -> std::result::Result<tonic::Response<super::UsageReceipt>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/Usage",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("acyclic.machines.v1.MachinesService", "Usage"));
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn cancel(
            &mut self,
            request: impl tonic::IntoRequest<super::OperationRequest>,
        ) -> std::result::Result<tonic::Response<super::OperationState>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/Cancel",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.machines.v1.MachinesService", "Cancel"),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn inspect_operation(
            &mut self,
            request: impl tonic::IntoRequest<super::OperationRequest>,
        ) -> std::result::Result<tonic::Response<super::OperationState>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/InspectOperation",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.machines.v1.MachinesService",
                        "InspectOperation",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn watch_operation(
            &mut self,
            request: impl tonic::IntoRequest<super::OperationRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::OperationState>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/acyclic.machines.v1.MachinesService/WatchOperation",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.machines.v1.MachinesService",
                        "WatchOperation",
                    ),
                );
            self.inner.server_streaming(req, path, codec).await
        }
    }
}
