// @generated
/// Generated client implementations.
pub mod filesystem_service_client {
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
    pub struct FilesystemServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl FilesystemServiceClient<tonic::transport::Channel> {
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
    impl<T> FilesystemServiceClient<T>
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
        ) -> FilesystemServiceClient<InterceptedService<T, F>>
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
            FilesystemServiceClient::new(InterceptedService::new(inner, interceptor))
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
        pub async fn handshake(
            &mut self,
            request: impl tonic::IntoRequest<super::HandshakeRequest>,
        ) -> std::result::Result<
            tonic::Response<super::HandshakeResponse>,
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
                "/acyclic.filesystem.v2.FilesystemService/Handshake",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.filesystem.v2.FilesystemService",
                        "Handshake",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn create_workspace(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateWorkspaceRequest>,
        ) -> std::result::Result<
            tonic::Response<super::WorkspaceResponse>,
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
                "/acyclic.filesystem.v2.FilesystemService/CreateWorkspace",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.filesystem.v2.FilesystemService",
                        "CreateWorkspace",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn open_workspace(
            &mut self,
            request: impl tonic::IntoRequest<super::OpenWorkspaceRequest>,
        ) -> std::result::Result<
            tonic::Response<super::WorkspaceResponse>,
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
                "/acyclic.filesystem.v2.FilesystemService/OpenWorkspace",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.filesystem.v2.FilesystemService",
                        "OpenWorkspace",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn delete_workspace(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteWorkspaceRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MutationResponse>,
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
                "/acyclic.filesystem.v2.FilesystemService/DeleteWorkspace",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.filesystem.v2.FilesystemService",
                        "DeleteWorkspace",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn get_head(
            &mut self,
            request: impl tonic::IntoRequest<super::GetHeadRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GenerationResponse>,
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
                "/acyclic.filesystem.v2.FilesystemService/GetHead",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.filesystem.v2.FilesystemService", "GetHead"),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn get_generation(
            &mut self,
            request: impl tonic::IntoRequest<super::GetGenerationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GenerationResponse>,
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
                "/acyclic.filesystem.v2.FilesystemService/GetGeneration",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.filesystem.v2.FilesystemService",
                        "GetGeneration",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn read(
            &mut self,
            request: impl tonic::IntoRequest<super::ReadRequest>,
        ) -> std::result::Result<tonic::Response<super::ReadResponse>, tonic::Status> {
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
                "/acyclic.filesystem.v2.FilesystemService/Read",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.filesystem.v2.FilesystemService", "Read"),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn stat(
            &mut self,
            request: impl tonic::IntoRequest<super::StatRequest>,
        ) -> std::result::Result<tonic::Response<super::StatResponse>, tonic::Status> {
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
                "/acyclic.filesystem.v2.FilesystemService/Stat",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.filesystem.v2.FilesystemService", "Stat"),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn list_directory(
            &mut self,
            request: impl tonic::IntoRequest<super::ListDirectoryRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListDirectoryResponse>,
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
                "/acyclic.filesystem.v2.FilesystemService/ListDirectory",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.filesystem.v2.FilesystemService",
                        "ListDirectory",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn read_link(
            &mut self,
            request: impl tonic::IntoRequest<super::ReadLinkRequest>,
        ) -> std::result::Result<tonic::Response<super::ReadResponse>, tonic::Status> {
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
                "/acyclic.filesystem.v2.FilesystemService/ReadLink",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.filesystem.v2.FilesystemService",
                        "ReadLink",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn plan_extents(
            &mut self,
            request: impl tonic::IntoRequest<super::PlanExtentsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::PlanExtentsResponse>,
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
                "/acyclic.filesystem.v2.FilesystemService/PlanExtents",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.filesystem.v2.FilesystemService",
                        "PlanExtents",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn apply_transaction(
            &mut self,
            request: impl tonic::IntoRequest<super::ApplyTransactionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MutationResponse>,
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
                "/acyclic.filesystem.v2.FilesystemService/ApplyTransaction",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.filesystem.v2.FilesystemService",
                        "ApplyTransaction",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn rebase_transaction(
            &mut self,
            request: impl tonic::IntoRequest<super::RebaseTransactionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RebaseTransactionResponse>,
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
                "/acyclic.filesystem.v2.FilesystemService/RebaseTransaction",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.filesystem.v2.FilesystemService",
                        "RebaseTransaction",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn fork_workspace(
            &mut self,
            request: impl tonic::IntoRequest<super::ForkWorkspaceRequest>,
        ) -> std::result::Result<
            tonic::Response<super::WorkspaceResponse>,
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
                "/acyclic.filesystem.v2.FilesystemService/ForkWorkspace",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.filesystem.v2.FilesystemService",
                        "ForkWorkspace",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn diff(
            &mut self,
            request: impl tonic::IntoRequest<super::DiffRequest>,
        ) -> std::result::Result<tonic::Response<super::DiffResponse>, tonic::Status> {
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
                "/acyclic.filesystem.v2.FilesystemService/Diff",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.filesystem.v2.FilesystemService", "Diff"),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn rebase(
            &mut self,
            request: impl tonic::IntoRequest<super::RebaseRequest>,
        ) -> std::result::Result<tonic::Response<super::RebaseResponse>, tonic::Status> {
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
                "/acyclic.filesystem.v2.FilesystemService/Rebase",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.filesystem.v2.FilesystemService", "Rebase"),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn plan_join(
            &mut self,
            request: impl tonic::IntoRequest<super::PlanJoinRequest>,
        ) -> std::result::Result<tonic::Response<super::JoinPlan>, tonic::Status> {
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
                "/acyclic.filesystem.v2.FilesystemService/PlanJoin",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.filesystem.v2.FilesystemService",
                        "PlanJoin",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn apply_join(
            &mut self,
            request: impl tonic::IntoRequest<super::ApplyJoinRequest>,
        ) -> std::result::Result<tonic::Response<super::JoinResponse>, tonic::Status> {
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
                "/acyclic.filesystem.v2.FilesystemService/ApplyJoin",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.filesystem.v2.FilesystemService",
                        "ApplyJoin",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn checkpoint(
            &mut self,
            request: impl tonic::IntoRequest<super::RetainGenerationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RetainGenerationResponse>,
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
                "/acyclic.filesystem.v2.FilesystemService/Checkpoint",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.filesystem.v2.FilesystemService",
                        "Checkpoint",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn pin(
            &mut self,
            request: impl tonic::IntoRequest<super::RetainGenerationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RetainGenerationResponse>,
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
                "/acyclic.filesystem.v2.FilesystemService/Pin",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.filesystem.v2.FilesystemService", "Pin"),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn export(
            &mut self,
            request: impl tonic::IntoRequest<super::ExportRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::ExportChunk>>,
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
                "/acyclic.filesystem.v2.FilesystemService/Export",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.filesystem.v2.FilesystemService", "Export"),
                );
            self.inner.server_streaming(req, path, codec).await
        }
        ///
        pub async fn import(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::ImportChunk>,
        ) -> std::result::Result<tonic::Response<super::ImportResponse>, tonic::Status> {
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
                "/acyclic.filesystem.v2.FilesystemService/Import",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.filesystem.v2.FilesystemService", "Import"),
                );
            self.inner.client_streaming(req, path, codec).await
        }
        ///
        pub async fn issue_mount_credential(
            &mut self,
            request: impl tonic::IntoRequest<super::CredentialRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CredentialResponse>,
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
                "/acyclic.filesystem.v2.FilesystemService/IssueMountCredential",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.filesystem.v2.FilesystemService",
                        "IssueMountCredential",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn issue_s3_credential(
            &mut self,
            request: impl tonic::IntoRequest<super::CredentialRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CredentialResponse>,
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
                "/acyclic.filesystem.v2.FilesystemService/IssueS3Credential",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.filesystem.v2.FilesystemService",
                        "IssueS3Credential",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn observe(
            &mut self,
            request: impl tonic::IntoRequest<super::ObserveRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ObserveResponse>,
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
                "/acyclic.filesystem.v2.FilesystemService/Observe",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.filesystem.v2.FilesystemService", "Observe"),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        pub async fn cancel(
            &mut self,
            request: impl tonic::IntoRequest<super::CancelRequest>,
        ) -> std::result::Result<tonic::Response<super::CancelResponse>, tonic::Status> {
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
                "/acyclic.filesystem.v2.FilesystemService/Cancel",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.filesystem.v2.FilesystemService", "Cancel"),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod filesystem_service_server {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with FilesystemServiceServer.
    #[async_trait]
    pub trait FilesystemService: std::marker::Send + std::marker::Sync + 'static {
        ///
        async fn handshake(
            &self,
            request: tonic::Request<super::HandshakeRequest>,
        ) -> std::result::Result<
            tonic::Response<super::HandshakeResponse>,
            tonic::Status,
        >;
        ///
        async fn create_workspace(
            &self,
            request: tonic::Request<super::CreateWorkspaceRequest>,
        ) -> std::result::Result<
            tonic::Response<super::WorkspaceResponse>,
            tonic::Status,
        >;
        ///
        async fn open_workspace(
            &self,
            request: tonic::Request<super::OpenWorkspaceRequest>,
        ) -> std::result::Result<
            tonic::Response<super::WorkspaceResponse>,
            tonic::Status,
        >;
        ///
        async fn delete_workspace(
            &self,
            request: tonic::Request<super::DeleteWorkspaceRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MutationResponse>,
            tonic::Status,
        >;
        ///
        async fn get_head(
            &self,
            request: tonic::Request<super::GetHeadRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GenerationResponse>,
            tonic::Status,
        >;
        ///
        async fn get_generation(
            &self,
            request: tonic::Request<super::GetGenerationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GenerationResponse>,
            tonic::Status,
        >;
        ///
        async fn read(
            &self,
            request: tonic::Request<super::ReadRequest>,
        ) -> std::result::Result<tonic::Response<super::ReadResponse>, tonic::Status>;
        ///
        async fn stat(
            &self,
            request: tonic::Request<super::StatRequest>,
        ) -> std::result::Result<tonic::Response<super::StatResponse>, tonic::Status>;
        ///
        async fn list_directory(
            &self,
            request: tonic::Request<super::ListDirectoryRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListDirectoryResponse>,
            tonic::Status,
        >;
        ///
        async fn read_link(
            &self,
            request: tonic::Request<super::ReadLinkRequest>,
        ) -> std::result::Result<tonic::Response<super::ReadResponse>, tonic::Status>;
        ///
        async fn plan_extents(
            &self,
            request: tonic::Request<super::PlanExtentsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::PlanExtentsResponse>,
            tonic::Status,
        >;
        ///
        async fn apply_transaction(
            &self,
            request: tonic::Request<super::ApplyTransactionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MutationResponse>,
            tonic::Status,
        >;
        ///
        async fn rebase_transaction(
            &self,
            request: tonic::Request<super::RebaseTransactionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RebaseTransactionResponse>,
            tonic::Status,
        >;
        ///
        async fn fork_workspace(
            &self,
            request: tonic::Request<super::ForkWorkspaceRequest>,
        ) -> std::result::Result<
            tonic::Response<super::WorkspaceResponse>,
            tonic::Status,
        >;
        ///
        async fn diff(
            &self,
            request: tonic::Request<super::DiffRequest>,
        ) -> std::result::Result<tonic::Response<super::DiffResponse>, tonic::Status>;
        ///
        async fn rebase(
            &self,
            request: tonic::Request<super::RebaseRequest>,
        ) -> std::result::Result<tonic::Response<super::RebaseResponse>, tonic::Status>;
        ///
        async fn plan_join(
            &self,
            request: tonic::Request<super::PlanJoinRequest>,
        ) -> std::result::Result<tonic::Response<super::JoinPlan>, tonic::Status>;
        ///
        async fn apply_join(
            &self,
            request: tonic::Request<super::ApplyJoinRequest>,
        ) -> std::result::Result<tonic::Response<super::JoinResponse>, tonic::Status>;
        ///
        async fn checkpoint(
            &self,
            request: tonic::Request<super::RetainGenerationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RetainGenerationResponse>,
            tonic::Status,
        >;
        ///
        async fn pin(
            &self,
            request: tonic::Request<super::RetainGenerationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RetainGenerationResponse>,
            tonic::Status,
        >;
        /// Server streaming response type for the Export method.
        type ExportStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<super::ExportChunk, tonic::Status>,
            >
            + std::marker::Send
            + 'static;
        ///
        async fn export(
            &self,
            request: tonic::Request<super::ExportRequest>,
        ) -> std::result::Result<tonic::Response<Self::ExportStream>, tonic::Status>;
        ///
        async fn import(
            &self,
            request: tonic::Request<tonic::Streaming<super::ImportChunk>>,
        ) -> std::result::Result<tonic::Response<super::ImportResponse>, tonic::Status>;
        ///
        async fn issue_mount_credential(
            &self,
            request: tonic::Request<super::CredentialRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CredentialResponse>,
            tonic::Status,
        >;
        ///
        async fn issue_s3_credential(
            &self,
            request: tonic::Request<super::CredentialRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CredentialResponse>,
            tonic::Status,
        >;
        ///
        async fn observe(
            &self,
            request: tonic::Request<super::ObserveRequest>,
        ) -> std::result::Result<tonic::Response<super::ObserveResponse>, tonic::Status>;
        ///
        async fn cancel(
            &self,
            request: tonic::Request<super::CancelRequest>,
        ) -> std::result::Result<tonic::Response<super::CancelResponse>, tonic::Status>;
    }
    ///
    #[derive(Debug)]
    pub struct FilesystemServiceServer<T> {
        inner: Arc<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    impl<T> FilesystemServiceServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for FilesystemServiceServer<T>
    where
        T: FilesystemService,
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
                "/acyclic.filesystem.v2.FilesystemService/Handshake" => {
                    #[allow(non_camel_case_types)]
                    struct HandshakeSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::HandshakeRequest>
                    for HandshakeSvc<T> {
                        type Response = super::HandshakeResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::HandshakeRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::handshake(&inner, request).await
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
                        let method = HandshakeSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/CreateWorkspace" => {
                    #[allow(non_camel_case_types)]
                    struct CreateWorkspaceSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::CreateWorkspaceRequest>
                    for CreateWorkspaceSvc<T> {
                        type Response = super::WorkspaceResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateWorkspaceRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::create_workspace(&inner, request)
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
                        let method = CreateWorkspaceSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/OpenWorkspace" => {
                    #[allow(non_camel_case_types)]
                    struct OpenWorkspaceSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::OpenWorkspaceRequest>
                    for OpenWorkspaceSvc<T> {
                        type Response = super::WorkspaceResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::OpenWorkspaceRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::open_workspace(&inner, request)
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
                        let method = OpenWorkspaceSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/DeleteWorkspace" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteWorkspaceSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::DeleteWorkspaceRequest>
                    for DeleteWorkspaceSvc<T> {
                        type Response = super::MutationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteWorkspaceRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::delete_workspace(&inner, request)
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
                        let method = DeleteWorkspaceSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/GetHead" => {
                    #[allow(non_camel_case_types)]
                    struct GetHeadSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::GetHeadRequest>
                    for GetHeadSvc<T> {
                        type Response = super::GenerationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetHeadRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::get_head(&inner, request).await
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
                        let method = GetHeadSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/GetGeneration" => {
                    #[allow(non_camel_case_types)]
                    struct GetGenerationSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::GetGenerationRequest>
                    for GetGenerationSvc<T> {
                        type Response = super::GenerationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetGenerationRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::get_generation(&inner, request)
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
                        let method = GetGenerationSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/Read" => {
                    #[allow(non_camel_case_types)]
                    struct ReadSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::ReadRequest> for ReadSvc<T> {
                        type Response = super::ReadResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ReadRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::read(&inner, request).await
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
                        let method = ReadSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/Stat" => {
                    #[allow(non_camel_case_types)]
                    struct StatSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::StatRequest> for StatSvc<T> {
                        type Response = super::StatResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StatRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::stat(&inner, request).await
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
                        let method = StatSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/ListDirectory" => {
                    #[allow(non_camel_case_types)]
                    struct ListDirectorySvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::ListDirectoryRequest>
                    for ListDirectorySvc<T> {
                        type Response = super::ListDirectoryResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListDirectoryRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::list_directory(&inner, request)
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
                        let method = ListDirectorySvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/ReadLink" => {
                    #[allow(non_camel_case_types)]
                    struct ReadLinkSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::ReadLinkRequest>
                    for ReadLinkSvc<T> {
                        type Response = super::ReadResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ReadLinkRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::read_link(&inner, request).await
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
                        let method = ReadLinkSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/PlanExtents" => {
                    #[allow(non_camel_case_types)]
                    struct PlanExtentsSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::PlanExtentsRequest>
                    for PlanExtentsSvc<T> {
                        type Response = super::PlanExtentsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PlanExtentsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::plan_extents(&inner, request)
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
                        let method = PlanExtentsSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/ApplyTransaction" => {
                    #[allow(non_camel_case_types)]
                    struct ApplyTransactionSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::ApplyTransactionRequest>
                    for ApplyTransactionSvc<T> {
                        type Response = super::MutationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ApplyTransactionRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::apply_transaction(&inner, request)
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
                        let method = ApplyTransactionSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/RebaseTransaction" => {
                    #[allow(non_camel_case_types)]
                    struct RebaseTransactionSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::RebaseTransactionRequest>
                    for RebaseTransactionSvc<T> {
                        type Response = super::RebaseTransactionResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RebaseTransactionRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::rebase_transaction(
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
                        let method = RebaseTransactionSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/ForkWorkspace" => {
                    #[allow(non_camel_case_types)]
                    struct ForkWorkspaceSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::ForkWorkspaceRequest>
                    for ForkWorkspaceSvc<T> {
                        type Response = super::WorkspaceResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ForkWorkspaceRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::fork_workspace(&inner, request)
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
                        let method = ForkWorkspaceSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/Diff" => {
                    #[allow(non_camel_case_types)]
                    struct DiffSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::DiffRequest> for DiffSvc<T> {
                        type Response = super::DiffResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DiffRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::diff(&inner, request).await
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
                        let method = DiffSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/Rebase" => {
                    #[allow(non_camel_case_types)]
                    struct RebaseSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::RebaseRequest>
                    for RebaseSvc<T> {
                        type Response = super::RebaseResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RebaseRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::rebase(&inner, request).await
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
                        let method = RebaseSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/PlanJoin" => {
                    #[allow(non_camel_case_types)]
                    struct PlanJoinSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::PlanJoinRequest>
                    for PlanJoinSvc<T> {
                        type Response = super::JoinPlan;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PlanJoinRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::plan_join(&inner, request).await
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
                        let method = PlanJoinSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/ApplyJoin" => {
                    #[allow(non_camel_case_types)]
                    struct ApplyJoinSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::ApplyJoinRequest>
                    for ApplyJoinSvc<T> {
                        type Response = super::JoinResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ApplyJoinRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::apply_join(&inner, request).await
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
                        let method = ApplyJoinSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/Checkpoint" => {
                    #[allow(non_camel_case_types)]
                    struct CheckpointSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::RetainGenerationRequest>
                    for CheckpointSvc<T> {
                        type Response = super::RetainGenerationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RetainGenerationRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::checkpoint(&inner, request).await
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
                "/acyclic.filesystem.v2.FilesystemService/Pin" => {
                    #[allow(non_camel_case_types)]
                    struct PinSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::RetainGenerationRequest>
                    for PinSvc<T> {
                        type Response = super::RetainGenerationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RetainGenerationRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::pin(&inner, request).await
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
                        let method = PinSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/Export" => {
                    #[allow(non_camel_case_types)]
                    struct ExportSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::ServerStreamingService<super::ExportRequest>
                    for ExportSvc<T> {
                        type Response = super::ExportChunk;
                        type ResponseStream = T::ExportStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ExportRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::export(&inner, request).await
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
                        let method = ExportSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/Import" => {
                    #[allow(non_camel_case_types)]
                    struct ImportSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::ClientStreamingService<super::ImportChunk>
                    for ImportSvc<T> {
                        type Response = super::ImportResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::ImportChunk>>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::import(&inner, request).await
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
                        let method = ImportSvc(inner);
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
                        let res = grpc.client_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/acyclic.filesystem.v2.FilesystemService/IssueMountCredential" => {
                    #[allow(non_camel_case_types)]
                    struct IssueMountCredentialSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::CredentialRequest>
                    for IssueMountCredentialSvc<T> {
                        type Response = super::CredentialResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CredentialRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::issue_mount_credential(
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
                        let method = IssueMountCredentialSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/IssueS3Credential" => {
                    #[allow(non_camel_case_types)]
                    struct IssueS3CredentialSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::CredentialRequest>
                    for IssueS3CredentialSvc<T> {
                        type Response = super::CredentialResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CredentialRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::issue_s3_credential(
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
                        let method = IssueS3CredentialSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/Observe" => {
                    #[allow(non_camel_case_types)]
                    struct ObserveSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::ObserveRequest>
                    for ObserveSvc<T> {
                        type Response = super::ObserveResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ObserveRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::observe(&inner, request).await
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
                        let method = ObserveSvc(inner);
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
                "/acyclic.filesystem.v2.FilesystemService/Cancel" => {
                    #[allow(non_camel_case_types)]
                    struct CancelSvc<T: FilesystemService>(pub Arc<T>);
                    impl<
                        T: FilesystemService,
                    > tonic::server::UnaryService<super::CancelRequest>
                    for CancelSvc<T> {
                        type Response = super::CancelResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CancelRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as FilesystemService>::cancel(&inner, request).await
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
    impl<T> Clone for FilesystemServiceServer<T> {
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
    pub const SERVICE_NAME: &str = "acyclic.filesystem.v2.FilesystemService";
    impl<T> tonic::server::NamedService for FilesystemServiceServer<T> {
        const NAME: &'static str = SERVICE_NAME;
    }
}
