use tonic::codegen::*;
/// Generated trait containing gRPC methods that should be implemented for use with MultipartServiceServer.
#[async_trait]
pub trait MultipartService: std::marker::Send + std::marker::Sync + 'static {
    async fn create_multipart(
        &self,
        request: tonic::Request<super::CreateMultipartRequest>,
    ) -> std::result::Result<tonic::Response<super::MultipartUpload>, tonic::Status>;
    async fn upload_part(
        &self,
        request: tonic::Request<tonic::Streaming<super::UploadPartRequest>>,
    ) -> std::result::Result<tonic::Response<super::UploadedPart>, tonic::Status>;
    async fn list_parts(
        &self,
        request: tonic::Request<super::ListPartsRequest>,
    ) -> std::result::Result<tonic::Response<super::ListPartsResponse>, tonic::Status>;
    async fn complete_multipart(
        &self,
        request: tonic::Request<super::CompleteMultipartRequest>,
    ) -> std::result::Result<tonic::Response<super::ObjectInfo>, tonic::Status>;
    async fn abort_multipart(
        &self,
        request: tonic::Request<super::AbortMultipartRequest>,
    ) -> std::result::Result<tonic::Response<super::AbortMultipartResponse>, tonic::Status>;
}
#[derive(Debug)]
pub struct MultipartServiceServer<T> {
    inner: Arc<T>,
}
impl<T> MultipartServiceServer<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}
impl<T, B> tonic::codegen::Service<http::Request<B>> for MultipartServiceServer<T>
where
    T: MultipartService,
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
            "/acyclic.objects.v1.MultipartService/CreateMultipart" => unary_server!(
                self,
                req,
                MultipartService,
                create_multipart,
                CreateMultipartRequest,
                MultipartUpload
            ),
            "/acyclic.objects.v1.MultipartService/UploadPart" => {
                #[allow(non_camel_case_types)]
                struct UploadPartSvc<T: MultipartService>(pub Arc<T>);
                impl<T: MultipartService>
                    tonic::server::ClientStreamingService<super::UploadPartRequest>
                    for UploadPartSvc<T>
                {
                    type Response = super::UploadedPart;
                    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                    fn call(
                        &mut self,
                        request: tonic::Request<tonic::Streaming<super::UploadPartRequest>>,
                    ) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        let fut = async move {
                            <T as MultipartService>::upload_part(&inner, request).await
                        };
                        Box::pin(fut)
                    }
                }
                let inner = self.inner.clone();
                let fut = async move {
                    let method = UploadPartSvc(inner);
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    let res = grpc.client_streaming(method, req).await;
                    Ok(res)
                };
                Box::pin(fut)
            }
            "/acyclic.objects.v1.MultipartService/ListParts" => unary_server!(
                self,
                req,
                MultipartService,
                list_parts,
                ListPartsRequest,
                ListPartsResponse
            ),
            "/acyclic.objects.v1.MultipartService/CompleteMultipart" => unary_server!(
                self,
                req,
                MultipartService,
                complete_multipart,
                CompleteMultipartRequest,
                ObjectInfo
            ),
            "/acyclic.objects.v1.MultipartService/AbortMultipart" => unary_server!(
                self,
                req,
                MultipartService,
                abort_multipart,
                AbortMultipartRequest,
                AbortMultipartResponse
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
impl<T> Clone for MultipartServiceServer<T> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        Self { inner }
    }
}
impl<T> tonic::server::NamedService for MultipartServiceServer<T> {
    const NAME: &'static str = "acyclic.objects.v1.MultipartService";
}
