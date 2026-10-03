// @generated
/// Generated client implementations.
pub mod actors_service_client {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    /// Remote operations for creating, observing, and invoking actors.
    #[derive(Debug, Clone)]
    pub struct ActorsServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ActorsServiceClient<tonic::transport::Channel> {
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
    impl<T> ActorsServiceClient<T>
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
        ) -> ActorsServiceClient<InterceptedService<T, F>>
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
            ActorsServiceClient::new(InterceptedService::new(inner, interceptor))
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
        /// Creates an actor.
        pub async fn create_actor(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateActorRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateActorResponse>,
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
                "/acyclic.actors.v1.ActorsService/CreateActor",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.actors.v1.ActorsService", "CreateActor"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Replaces actor configuration with compare-and-swap semantics.
        pub async fn update_actor(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateActorRequest>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateActorResponse>,
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
                "/acyclic.actors.v1.ActorsService/UpdateActor",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.actors.v1.ActorsService", "UpdateActor"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Returns the current actor observation.
        pub async fn inspect_actor(
            &mut self,
            request: impl tonic::IntoRequest<super::InspectActorRequest>,
        ) -> std::result::Result<
            tonic::Response<super::InspectActorResponse>,
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
                "/acyclic.actors.v1.ActorsService/InspectActor",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.actors.v1.ActorsService", "InspectActor"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Adds a subscription to an actor.
        pub async fn add_subscription(
            &mut self,
            request: impl tonic::IntoRequest<super::AddSubscriptionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::AddSubscriptionResponse>,
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
                "/acyclic.actors.v1.ActorsService/AddSubscription",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.actors.v1.ActorsService", "AddSubscription"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Removes a subscription from an actor.
        pub async fn remove_subscription(
            &mut self,
            request: impl tonic::IntoRequest<super::RemoveSubscriptionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RemoveSubscriptionResponse>,
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
                "/acyclic.actors.v1.ActorsService/RemoveSubscription",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.actors.v1.ActorsService",
                        "RemoveSubscription",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Resumes a paused subscription.
        pub async fn resume_subscription(
            &mut self,
            request: impl tonic::IntoRequest<super::ResumeSubscriptionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ResumeSubscriptionResponse>,
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
                "/acyclic.actors.v1.ActorsService/ResumeSubscription",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "acyclic.actors.v1.ActorsService",
                        "ResumeSubscription",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Requests an actor checkpoint.
        pub async fn checkpoint_actor(
            &mut self,
            request: impl tonic::IntoRequest<super::CheckpointActorRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CheckpointActorResponse>,
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
                "/acyclic.actors.v1.ActorsService/CheckpointActor",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.actors.v1.ActorsService", "CheckpointActor"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Invokes an actor method.
        pub async fn invoke_actor(
            &mut self,
            request: impl tonic::IntoRequest<super::InvokeActorRequest>,
        ) -> std::result::Result<
            tonic::Response<super::InvokeActorResponse>,
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
                "/acyclic.actors.v1.ActorsService/InvokeActor",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("acyclic.actors.v1.ActorsService", "InvokeActor"),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod actors_service_server {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with ActorsServiceServer.
    #[async_trait]
    pub trait ActorsService: std::marker::Send + std::marker::Sync + 'static {
        /// Creates an actor.
        async fn create_actor(
            &self,
            request: tonic::Request<super::CreateActorRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateActorResponse>,
            tonic::Status,
        >;
        /// Replaces actor configuration with compare-and-swap semantics.
        async fn update_actor(
            &self,
            request: tonic::Request<super::UpdateActorRequest>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateActorResponse>,
            tonic::Status,
        >;
        /// Returns the current actor observation.
        async fn inspect_actor(
            &self,
            request: tonic::Request<super::InspectActorRequest>,
        ) -> std::result::Result<
            tonic::Response<super::InspectActorResponse>,
            tonic::Status,
        >;
        /// Adds a subscription to an actor.
        async fn add_subscription(
            &self,
            request: tonic::Request<super::AddSubscriptionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::AddSubscriptionResponse>,
            tonic::Status,
        >;
        /// Removes a subscription from an actor.
        async fn remove_subscription(
            &self,
            request: tonic::Request<super::RemoveSubscriptionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RemoveSubscriptionResponse>,
            tonic::Status,
        >;
        /// Resumes a paused subscription.
        async fn resume_subscription(
            &self,
            request: tonic::Request<super::ResumeSubscriptionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ResumeSubscriptionResponse>,
            tonic::Status,
        >;
        /// Requests an actor checkpoint.
        async fn checkpoint_actor(
            &self,
            request: tonic::Request<super::CheckpointActorRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CheckpointActorResponse>,
            tonic::Status,
        >;
        /// Invokes an actor method.
        async fn invoke_actor(
            &self,
            request: tonic::Request<super::InvokeActorRequest>,
        ) -> std::result::Result<
            tonic::Response<super::InvokeActorResponse>,
            tonic::Status,
        >;
    }
    /// Remote operations for creating, observing, and invoking actors.
    #[derive(Debug)]
    pub struct ActorsServiceServer<T> {
        inner: Arc<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    impl<T> ActorsServiceServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ActorsServiceServer<T>
    where
        T: ActorsService,
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
                "/acyclic.actors.v1.ActorsService/CreateActor" => {
                    #[allow(non_camel_case_types)]
                    struct CreateActorSvc<T: ActorsService>(pub Arc<T>);
                    impl<
                        T: ActorsService,
                    > tonic::server::UnaryService<super::CreateActorRequest>
                    for CreateActorSvc<T> {
                        type Response = super::CreateActorResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateActorRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ActorsService>::create_actor(&inner, request).await
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
                        let method = CreateActorSvc(inner);
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
                "/acyclic.actors.v1.ActorsService/UpdateActor" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateActorSvc<T: ActorsService>(pub Arc<T>);
                    impl<
                        T: ActorsService,
                    > tonic::server::UnaryService<super::UpdateActorRequest>
                    for UpdateActorSvc<T> {
                        type Response = super::UpdateActorResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpdateActorRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ActorsService>::update_actor(&inner, request).await
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
                        let method = UpdateActorSvc(inner);
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
                "/acyclic.actors.v1.ActorsService/InspectActor" => {
                    #[allow(non_camel_case_types)]
                    struct InspectActorSvc<T: ActorsService>(pub Arc<T>);
                    impl<
                        T: ActorsService,
                    > tonic::server::UnaryService<super::InspectActorRequest>
                    for InspectActorSvc<T> {
                        type Response = super::InspectActorResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::InspectActorRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ActorsService>::inspect_actor(&inner, request).await
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
                        let method = InspectActorSvc(inner);
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
                "/acyclic.actors.v1.ActorsService/AddSubscription" => {
                    #[allow(non_camel_case_types)]
                    struct AddSubscriptionSvc<T: ActorsService>(pub Arc<T>);
                    impl<
                        T: ActorsService,
                    > tonic::server::UnaryService<super::AddSubscriptionRequest>
                    for AddSubscriptionSvc<T> {
                        type Response = super::AddSubscriptionResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AddSubscriptionRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ActorsService>::add_subscription(&inner, request)
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
                        let method = AddSubscriptionSvc(inner);
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
                "/acyclic.actors.v1.ActorsService/RemoveSubscription" => {
                    #[allow(non_camel_case_types)]
                    struct RemoveSubscriptionSvc<T: ActorsService>(pub Arc<T>);
                    impl<
                        T: ActorsService,
                    > tonic::server::UnaryService<super::RemoveSubscriptionRequest>
                    for RemoveSubscriptionSvc<T> {
                        type Response = super::RemoveSubscriptionResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RemoveSubscriptionRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ActorsService>::remove_subscription(&inner, request)
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
                        let method = RemoveSubscriptionSvc(inner);
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
                "/acyclic.actors.v1.ActorsService/ResumeSubscription" => {
                    #[allow(non_camel_case_types)]
                    struct ResumeSubscriptionSvc<T: ActorsService>(pub Arc<T>);
                    impl<
                        T: ActorsService,
                    > tonic::server::UnaryService<super::ResumeSubscriptionRequest>
                    for ResumeSubscriptionSvc<T> {
                        type Response = super::ResumeSubscriptionResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ResumeSubscriptionRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ActorsService>::resume_subscription(&inner, request)
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
                        let method = ResumeSubscriptionSvc(inner);
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
                "/acyclic.actors.v1.ActorsService/CheckpointActor" => {
                    #[allow(non_camel_case_types)]
                    struct CheckpointActorSvc<T: ActorsService>(pub Arc<T>);
                    impl<
                        T: ActorsService,
                    > tonic::server::UnaryService<super::CheckpointActorRequest>
                    for CheckpointActorSvc<T> {
                        type Response = super::CheckpointActorResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CheckpointActorRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ActorsService>::checkpoint_actor(&inner, request)
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
                        let method = CheckpointActorSvc(inner);
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
                "/acyclic.actors.v1.ActorsService/InvokeActor" => {
                    #[allow(non_camel_case_types)]
                    struct InvokeActorSvc<T: ActorsService>(pub Arc<T>);
                    impl<
                        T: ActorsService,
                    > tonic::server::UnaryService<super::InvokeActorRequest>
                    for InvokeActorSvc<T> {
                        type Response = super::InvokeActorResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::InvokeActorRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ActorsService>::invoke_actor(&inner, request).await
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
                        let method = InvokeActorSvc(inner);
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
    impl<T> Clone for ActorsServiceServer<T> {
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
    pub const SERVICE_NAME: &str = "acyclic.actors.v1.ActorsService";
    impl<T> tonic::server::NamedService for ActorsServiceServer<T> {
        const NAME: &'static str = SERVICE_NAME;
    }
}
