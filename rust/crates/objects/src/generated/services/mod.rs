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
    ($server:expr, $request:expr, $service:ident, $method:ident, $input:ident, $output:ident) => {{
        struct Rpc<T: $service>(Arc<T>);
        impl<T: $service> tonic::server::UnaryService<super::$input> for Rpc<T> {
            type Response = super::$output;
            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
            fn call(&mut self, request: tonic::Request<super::$input>) -> Self::Future {
                let inner = Arc::clone(&self.0);
                Box::pin(async move { inner.$method(request).await })
            }
        }
        let inner = Arc::clone(&$server.inner);
        Box::pin(async move {
            let codec = tonic_prost::ProstCodec::default();
            let mut grpc = tonic::server::Grpc::new(codec);
            Ok(grpc.unary(Rpc(inner), $request).await)
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
