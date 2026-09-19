// @generated
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
/// Generated server implementations.
pub mod machines_service_server {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with MachinesServiceServer.
    #[async_trait]
    pub trait MachinesService: std::marker::Send + std::marker::Sync + 'static {
        ///
        async fn qualify_image(
            &self,
            request: tonic::Request<super::QualifyImageRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ImageQualification>,
            tonic::Status,
        >;
        ///
        async fn create(
            &self,
            request: tonic::Request<super::CreateMachineRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MachineAdmission>,
            tonic::Status,
        >;
        ///
        async fn checkpoint(
            &self,
            request: tonic::Request<super::CheckpointMachineRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CheckpointAdmission>,
            tonic::Status,
        >;
        ///
        async fn fork(
            &self,
            request: tonic::Request<super::ForkCheckpointRequest>,
        ) -> std::result::Result<tonic::Response<super::ForkAdmission>, tonic::Status>;
        ///
        async fn suspend(
            &self,
            request: tonic::Request<super::MachineMutationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MutationAdmission>,
            tonic::Status,
        >;
        ///
        async fn wake(
            &self,
            request: tonic::Request<super::MachineMutationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MutationAdmission>,
            tonic::Status,
        >;
        ///
        async fn set_suspension_policy(
            &self,
            request: tonic::Request<super::SetSuspensionPolicyRequest>,
        ) -> std::result::Result<tonic::Response<super::PolicyAdmission>, tonic::Status>;
        ///
        async fn destroy_machine(
            &self,
            request: tonic::Request<super::MachineMutationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MutationAdmission>,
            tonic::Status,
        >;
        ///
        async fn destroy_checkpoint(
            &self,
            request: tonic::Request<super::CheckpointMutationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MutationAdmission>,
            tonic::Status,
        >;
        ///
        async fn recover(
            &self,
            request: tonic::Request<super::RecoverRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RecoveredAdmission>,
            tonic::Status,
        >;
        ///
        async fn inspect_machine(
            &self,
            request: tonic::Request<super::InspectMachineRequest>,
        ) -> std::result::Result<tonic::Response<super::MachineState>, tonic::Status>;
        ///
        async fn inspect_checkpoint(
            &self,
            request: tonic::Request<super::InspectCheckpointRequest>,
        ) -> std::result::Result<tonic::Response<super::CheckpointState>, tonic::Status>;
        ///
        async fn list_machines(
            &self,
            request: tonic::Request<super::ListMachinesRequest>,
        ) -> std::result::Result<tonic::Response<super::MachinePage>, tonic::Status>;
        ///
        async fn events(
            &self,
            request: tonic::Request<super::EventsRequest>,
        ) -> std::result::Result<tonic::Response<super::EventPage>, tonic::Status>;
        ///
        async fn usage(
            &self,
            request: tonic::Request<super::UsageRequest>,
        ) -> std::result::Result<tonic::Response<super::UsageReceipt>, tonic::Status>;
        ///
        async fn cancel(
            &self,
            request: tonic::Request<super::OperationRequest>,
        ) -> std::result::Result<tonic::Response<super::OperationState>, tonic::Status>;
        ///
        async fn inspect_operation(
            &self,
            request: tonic::Request<super::OperationRequest>,
        ) -> std::result::Result<tonic::Response<super::OperationState>, tonic::Status>;
        /// Server streaming response type for the WatchOperation method.
        type WatchOperationStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<super::OperationState, tonic::Status>,
            >
            + std::marker::Send
            + 'static;
        ///
        async fn watch_operation(
            &self,
            request: tonic::Request<super::OperationRequest>,
        ) -> std::result::Result<
            tonic::Response<Self::WatchOperationStream>,
            tonic::Status,
        >;
    }
    ///
    #[derive(Debug)]
    pub struct MachinesServiceServer<T> {
        inner: Arc<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    impl<T> MachinesServiceServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for MachinesServiceServer<T>
    where
        T: MachinesService,
        B: Body + std::marker::Send + 'static,
        B::Error: Into<StdError> + std::marker::Send + 'static,
    {
        type Response = http::Response<tonic::body::Body>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            match req.uri().path() {
                "/acyclic.machines.v1.MachinesService/QualifyImage" => {
                    #[allow(non_camel_case_types)]
                    struct QualifyImageSvc<T: MachinesService>(pub Arc<T>);
                    impl<
                        T: MachinesService,
                    > tonic::server::UnaryService<super::QualifyImageRequest>
                    for QualifyImageSvc<T> {
                        type Response = super::ImageQualification;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::QualifyImageRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as MachinesService>::qualify_image(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = QualifyImageSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/acyclic.machines.v1.MachinesService/Create" => {
                    #[allow(non_camel_case_types)]
                    struct CreateSvc<T: MachinesService>(pub Arc<T>);
                    impl<
                        T: MachinesService,
                    > tonic::server::UnaryService<super::CreateMachineRequest>
                    for CreateSvc<T> {
                        type Response = super::MachineAdmission;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateMachineRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as MachinesService>::create(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = CreateSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/acyclic.machines.v1.MachinesService/Checkpoint" => {
                    #[allow(non_camel_case_types)]
                    struct CheckpointSvc<T: MachinesService>(pub Arc<T>);
                    impl<
                        T: MachinesService,
                    > tonic::server::UnaryService<super::CheckpointMachineRequest>
                    for CheckpointSvc<T> {
                        type Response = super::CheckpointAdmission;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CheckpointMachineRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as MachinesService>::checkpoint(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = CheckpointSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/acyclic.machines.v1.MachinesService/Fork" => {
                    #[allow(non_camel_case_types)]
                    struct ForkSvc<T: MachinesService>(pub Arc<T>);
                    impl<
                        T: MachinesService,
                    > tonic::server::UnaryService<super::ForkCheckpointRequest>
                    for ForkSvc<T> {
                        type Response = super::ForkAdmission;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ForkCheckpointRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as MachinesService>::fork(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = ForkSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/acyclic.machines.v1.MachinesService/Suspend" => {
                    #[allow(non_camel_case_types)]
                    struct SuspendSvc<T: MachinesService>(pub Arc<T>);
                    impl<
                        T: MachinesService,
                    > tonic::server::UnaryService<super::MachineMutationRequest>
                    for SuspendSvc<T> {
                        type Response = super::MutationAdmission;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::MachineMutationRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as MachinesService>::suspend(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = SuspendSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/acyclic.machines.v1.MachinesService/Wake" => {
                    #[allow(non_camel_case_types)]
                    struct WakeSvc<T: MachinesService>(pub Arc<T>);
                    impl<
                        T: MachinesService,
                    > tonic::server::UnaryService<super::MachineMutationRequest>
                    for WakeSvc<T> {
                        type Response = super::MutationAdmission;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::MachineMutationRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as MachinesService>::wake(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = WakeSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/acyclic.machines.v1.MachinesService/SetSuspensionPolicy" => {
                    #[allow(non_camel_case_types)]
                    struct SetSuspensionPolicySvc<T: MachinesService>(pub Arc<T>);
                    impl<
                        T: MachinesService,
                    > tonic::server::UnaryService<super::SetSuspensionPolicyRequest>
                    for SetSuspensionPolicySvc<T> {
                        type Response = super::PolicyAdmission;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SetSuspensionPolicyRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as MachinesService>::set_suspension_policy(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = SetSuspensionPolicySvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/acyclic.machines.v1.MachinesService/DestroyMachine" => {
                    #[allow(non_camel_case_types)]
                    struct DestroyMachineSvc<T: MachinesService>(pub Arc<T>);
                    impl<
                        T: MachinesService,
                    > tonic::server::UnaryService<super::MachineMutationRequest>
                    for DestroyMachineSvc<T> {
                        type Response = super::MutationAdmission;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::MachineMutationRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as MachinesService>::destroy_machine(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = DestroyMachineSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/acyclic.machines.v1.MachinesService/DestroyCheckpoint" => {
                    #[allow(non_camel_case_types)]
                    struct DestroyCheckpointSvc<T: MachinesService>(pub Arc<T>);
                    impl<
                        T: MachinesService,
                    > tonic::server::UnaryService<super::CheckpointMutationRequest>
                    for DestroyCheckpointSvc<T> {
                        type Response = super::MutationAdmission;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CheckpointMutationRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as MachinesService>::destroy_checkpoint(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = DestroyCheckpointSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/acyclic.machines.v1.MachinesService/Recover" => {
                    #[allow(non_camel_case_types)]
                    struct RecoverSvc<T: MachinesService>(pub Arc<T>);
                    impl<
                        T: MachinesService,
                    > tonic::server::UnaryService<super::RecoverRequest>
                    for RecoverSvc<T> {
                        type Response = super::RecoveredAdmission;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RecoverRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as MachinesService>::recover(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = RecoverSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/acyclic.machines.v1.MachinesService/InspectMachine" => {
                    #[allow(non_camel_case_types)]
                    struct InspectMachineSvc<T: MachinesService>(pub Arc<T>);
                    impl<
                        T: MachinesService,
                    > tonic::server::UnaryService<super::InspectMachineRequest>
                    for InspectMachineSvc<T> {
                        type Response = super::MachineState;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::InspectMachineRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as MachinesService>::inspect_machine(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = InspectMachineSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/acyclic.machines.v1.MachinesService/InspectCheckpoint" => {
                    #[allow(non_camel_case_types)]
                    struct InspectCheckpointSvc<T: MachinesService>(pub Arc<T>);
                    impl<
                        T: MachinesService,
                    > tonic::server::UnaryService<super::InspectCheckpointRequest>
                    for InspectCheckpointSvc<T> {
                        type Response = super::CheckpointState;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::InspectCheckpointRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as MachinesService>::inspect_checkpoint(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = InspectCheckpointSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/acyclic.machines.v1.MachinesService/ListMachines" => {
                    #[allow(non_camel_case_types)]
                    struct ListMachinesSvc<T: MachinesService>(pub Arc<T>);
                    impl<
                        T: MachinesService,
                    > tonic::server::UnaryService<super::ListMachinesRequest>
                    for ListMachinesSvc<T> {
                        type Response = super::MachinePage;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListMachinesRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as MachinesService>::list_machines(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = ListMachinesSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/acyclic.machines.v1.MachinesService/Events" => {
                    #[allow(non_camel_case_types)]
                    struct EventsSvc<T: MachinesService>(pub Arc<T>);
                    impl<
                        T: MachinesService,
                    > tonic::server::UnaryService<super::EventsRequest>
                    for EventsSvc<T> {
                        type Response = super::EventPage;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::EventsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as MachinesService>::events(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = EventsSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/acyclic.machines.v1.MachinesService/Usage" => {
                    #[allow(non_camel_case_types)]
                    struct UsageSvc<T: MachinesService>(pub Arc<T>);
                    impl<
                        T: MachinesService,
                    > tonic::server::UnaryService<super::UsageRequest> for UsageSvc<T> {
                        type Response = super::UsageReceipt;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UsageRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as MachinesService>::usage(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = UsageSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/acyclic.machines.v1.MachinesService/Cancel" => {
                    #[allow(non_camel_case_types)]
                    struct CancelSvc<T: MachinesService>(pub Arc<T>);
                    impl<
                        T: MachinesService,
                    > tonic::server::UnaryService<super::OperationRequest>
                    for CancelSvc<T> {
                        type Response = super::OperationState;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::OperationRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as MachinesService>::cancel(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = CancelSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/acyclic.machines.v1.MachinesService/InspectOperation" => {
                    #[allow(non_camel_case_types)]
                    struct InspectOperationSvc<T: MachinesService>(pub Arc<T>);
                    impl<
                        T: MachinesService,
                    > tonic::server::UnaryService<super::OperationRequest>
                    for InspectOperationSvc<T> {
                        type Response = super::OperationState;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::OperationRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as MachinesService>::inspect_operation(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = InspectOperationSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/acyclic.machines.v1.MachinesService/WatchOperation" => {
                    #[allow(non_camel_case_types)]
                    struct WatchOperationSvc<T: MachinesService>(pub Arc<T>);
                    impl<
                        T: MachinesService,
                    > tonic::server::ServerStreamingService<super::OperationRequest>
                    for WatchOperationSvc<T> {
                        type Response = super::OperationState;
                        type ResponseStream = T::WatchOperationStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::OperationRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as MachinesService>::watch_operation(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = WatchOperationSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        let mut response = http::Response::new(
                            tonic::body::Body::default(),
                        );
                        let headers = response.headers_mut();
                        headers
                            .insert(
                                tonic::Status::GRPC_STATUS,
                                (tonic::Code::Unimplemented as i32).into(),
                            );
                        headers
                            .insert(
                                http::header::CONTENT_TYPE,
                                tonic::metadata::GRPC_CONTENT_TYPE,
                            );
                        Ok(response)
                    })
                }
            }
        }
    }
    impl<T> Clone for MachinesServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    /// Generated gRPC service name
    pub const SERVICE_NAME: &str = "acyclic.machines.v1.MachinesService";
    impl<T> tonic::server::NamedService for MachinesServiceServer<T> {
        const NAME: &'static str = SERVICE_NAME;
    }
}
