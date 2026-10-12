macro_rules! unary_client {
    ($service:literal; $(($method:ident, $input:ident, $output:ident, $name:literal)),+ $(,)?) => {
        $(pub async fn $method(&mut self, request: impl tonic::IntoRequest<super::$input>)
            -> Result<tonic::Response<super::$output>, tonic::Status> {
            self.inner.ready().await.map_err(|error|
                tonic::Status::unknown(format!("Service was not ready: {}", error.into())))?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(concat!("/", $service, "/", $name));
            let mut request = request.into_request();
            request.extensions_mut().insert(GrpcMethod::new($service, $name));
            self.inner.unary(request, path, codec).await
        })+
    };
}

macro_rules! unary_server {
    ($server:ident, $service:ident, $name:literal;
     $(($method:ident, $message:ident, $input:ty, $output:ident, $rpc:literal,
        $kind:ident, $handler:ident)),+ $(,)?) => {
        /// Service methods.
        #[async_trait]
        pub trait $service: Send + Sync + 'static {
            $(async fn $method(&self, request: tonic::Request<$input>)
                -> Result<tonic::Response<super::$output>, tonic::Status>;)+
        }
        #[derive(Debug)]
        pub struct $server<T> { inner: Arc<T> }
        impl<T> $server<T> {
            pub fn new(inner: T) -> Self {
                Self { inner: Arc::new(inner) }
            }
        }
        impl<T, B> Service<http::Request<B>> for $server<T>
        where
            T: $service,
            B: Body + Send + 'static,
            B::Error: Into<StdError> + Send + 'static,
        {
            type Response = http::Response<tonic::body::Body>;
            type Error = std::convert::Infallible;
            type Future = BoxFuture<Self::Response, Self::Error>;
            fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }
            fn call(&mut self, request: http::Request<B>) -> Self::Future {
                match request.uri().path() {
                    $(concat!("/", $name, "/", $rpc) =>
                        unary_server!(@rpc self, request, $service, $method, $message,
                            $input, $output, $kind, $handler),)+
                    _ => Box::pin(async move {
                        let mut response = http::Response::new(tonic::body::Body::default());
                        let headers = response.headers_mut();
                        headers.insert(tonic::Status::GRPC_STATUS,
                            (tonic::Code::Unimplemented as i32).into());
                        headers.insert(http::header::CONTENT_TYPE,
                            tonic::metadata::GRPC_CONTENT_TYPE);
                        Ok(response)
                    }),
                }
            }
        }
        impl<T> Clone for $server<T> {
            fn clone(&self) -> Self { Self { inner: self.inner.clone() } }
        }
        impl<T> tonic::server::NamedService for $server<T> {
            const NAME: &'static str = $name;
        }
    };
    ($server:expr, $request:expr, $service:ident, $method:ident, $input:ident, $output:ident) => {
        unary_server!(@rpc $server, $request, $service, $method, $input, super::$input,
            $output, unary, UnaryService)
    };
    (@rpc $server:expr, $request:expr, $service:ident, $method:ident, $message:ident,
     $input:ty, $output:ident, $kind:ident, $handler:ident) => {{
        struct Rpc<T: $service>(Arc<T>);
        impl<T: $service> tonic::server::$handler<super::$message> for Rpc<T> {
            type Response = super::$output;
            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
            fn call(&mut self, request: tonic::Request<$input>) -> Self::Future {
                let inner = Arc::clone(&self.0);
                Box::pin(async move { inner.$method(request).await })
            }
        }
        let inner = Arc::clone(&$server.inner);
        Box::pin(async move {
            let codec = tonic_prost::ProstCodec::default();
            let mut grpc = tonic::server::Grpc::new(codec);
            Ok(grpc.$kind(Rpc(inner), $request).await)
        })
    }};
}

// @generated
/// buckets client bindings.
pub mod buckets_service_client {
    include!("buckets/client.rs");
}
/// buckets server bindings.
pub mod buckets_service_server {
    include!("buckets/server.rs");
}
/// objects client bindings.
pub mod objects_service_client {
    include!("objects/client.rs");
}
/// objects server bindings.
pub mod objects_service_server {
    include!("objects/server.rs");
}
/// multipart client bindings.
pub mod multipart_service_client {
    include!("multipart/client.rs");
}
/// multipart server bindings.
pub mod multipart_service_server {
    include!("multipart/server.rs");
}
