use tonic::codegen::*;
/// Generated trait containing gRPC methods that should be implemented for use with ObjectsServiceServer.
#[async_trait]
pub trait ObjectsService: std::marker::Send + std::marker::Sync + 'static {
    async fn put_object(
        &self,
        request: tonic::Request<tonic::Streaming<super::PutObjectRequest>>,
    ) -> std::result::Result<tonic::Response<super::ObjectInfo>, tonic::Status>;
    /// Server streaming response type for the GetObject method.
    type GetObjectStream: tonic::codegen::tokio_stream::Stream<
            Item = std::result::Result<super::GetObjectResponse, tonic::Status>,
        > + std::marker::Send
        + 'static;
    async fn get_object(
        &self,
        request: tonic::Request<super::GetObjectRequest>,
    ) -> std::result::Result<tonic::Response<Self::GetObjectStream>, tonic::Status>;
    async fn head_object(
        &self,
        request: tonic::Request<super::HeadObjectRequest>,
    ) -> std::result::Result<tonic::Response<super::HeadObjectResponse>, tonic::Status>;
    async fn delete_object(
        &self,
        request: tonic::Request<super::DeleteObjectRequest>,
    ) -> std::result::Result<tonic::Response<super::DeleteObjectResponse>, tonic::Status>;
    async fn list_objects(
        &self,
        request: tonic::Request<super::ListObjectsRequest>,
    ) -> std::result::Result<tonic::Response<super::ListObjectsResponse>, tonic::Status>;
}
#[derive(Debug)]
pub struct ObjectsServiceServer<T> {
    inner: Arc<T>,
}
impl<T> ObjectsServiceServer<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}
impl<T, B> tonic::codegen::Service<http::Request<B>> for ObjectsServiceServer<T>
where
    T: ObjectsService,
    B: Body + std::marker::Send + 'static,
    B::Error: Into<StdError> + std::marker::Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        match req.uri().path() {
            "/acyclic.objects.v1.ObjectsService/PutObject" => {
                #[allow(non_camel_case_types)]
                struct PutObjectSvc<T: ObjectsService>(pub Arc<T>);
                impl<T: ObjectsService>
                    tonic::server::ClientStreamingService<super::PutObjectRequest>
                    for PutObjectSvc<T>
                {
                    type Response = super::ObjectInfo;
                    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                    fn call(
                        &mut self,
                        request: tonic::Request<tonic::Streaming<super::PutObjectRequest>>,
                    ) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        let fut =
                            async move { <T as ObjectsService>::put_object(&inner, request).await };
                        Box::pin(fut)
                    }
                }
                let inner = self.inner.clone();
                let fut = async move {
                    let method = PutObjectSvc(inner);
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    let res = grpc.client_streaming(method, req).await;
                    Ok(res)
                };
                Box::pin(fut)
            }
            "/acyclic.objects.v1.ObjectsService/GetObject" => {
                #[allow(non_camel_case_types)]
                struct GetObjectSvc<T: ObjectsService>(pub Arc<T>);
                impl<T: ObjectsService>
                    tonic::server::ServerStreamingService<super::GetObjectRequest>
                    for GetObjectSvc<T>
                {
                    type Response = super::GetObjectResponse;
                    type ResponseStream = T::GetObjectStream;
                    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                    fn call(
                        &mut self,
                        request: tonic::Request<super::GetObjectRequest>,
                    ) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        let fut =
                            async move { <T as ObjectsService>::get_object(&inner, request).await };
                        Box::pin(fut)
                    }
                }
                let inner = self.inner.clone();
                let fut = async move {
                    let method = GetObjectSvc(inner);
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    let res = grpc.server_streaming(method, req).await;
                    Ok(res)
                };
                Box::pin(fut)
            }
            "/acyclic.objects.v1.ObjectsService/HeadObject" => unary_server!(
                self,
                req,
                ObjectsService,
                head_object,
                HeadObjectRequest,
                HeadObjectResponse
            ),
            "/acyclic.objects.v1.ObjectsService/DeleteObject" => unary_server!(
                self,
                req,
                ObjectsService,
                delete_object,
                DeleteObjectRequest,
                DeleteObjectResponse
            ),
            "/acyclic.objects.v1.ObjectsService/ListObjects" => unary_server!(
                self,
                req,
                ObjectsService,
                list_objects,
                ListObjectsRequest,
                ListObjectsResponse
            ),
            _ => Box::pin(async move {
                let mut response = http::Response::new(tonic::body::Body::default());
                let headers = response.headers_mut();
                headers.insert(
                    tonic::Status::GRPC_STATUS,
                    (tonic::Code::Unimplemented as i32).into(),
                );
                headers.insert(
                    http::header::CONTENT_TYPE,
                    tonic::metadata::GRPC_CONTENT_TYPE,
                );
                Ok(response)
            }),
        }
    }
}
impl<T> Clone for ObjectsServiceServer<T> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        Self { inner }
    }
}
impl<T> tonic::server::NamedService for ObjectsServiceServer<T> {
    const NAME: &'static str = "acyclic.objects.v1.ObjectsService";
}
