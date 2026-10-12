use tonic::codegen::*;
/// Generated trait containing gRPC methods that should be implemented for use with BucketsServiceServer.
#[async_trait]
pub trait BucketsService: std::marker::Send + std::marker::Sync + 'static {
    async fn create_bucket(
        &self,
        request: tonic::Request<super::CreateBucketRequest>,
    ) -> std::result::Result<tonic::Response<super::Bucket>, tonic::Status>;
    async fn head_bucket(
        &self,
        request: tonic::Request<super::HeadBucketRequest>,
    ) -> std::result::Result<tonic::Response<super::Bucket>, tonic::Status>;
    async fn delete_bucket(
        &self,
        request: tonic::Request<super::DeleteBucketRequest>,
    ) -> std::result::Result<tonic::Response<super::DeleteBucketResponse>, tonic::Status>;
}
#[derive(Debug)]
pub struct BucketsServiceServer<T> {
    inner: Arc<T>,
}
impl<T> BucketsServiceServer<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}
impl<T, B> tonic::codegen::Service<http::Request<B>> for BucketsServiceServer<T>
where
    T: BucketsService,
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
            "/acyclic.objects.v1.BucketsService/CreateBucket" => unary_server!(
                self,
                req,
                BucketsService,
                create_bucket,
                CreateBucketRequest,
                Bucket
            ),
            "/acyclic.objects.v1.BucketsService/HeadBucket" => unary_server!(
                self,
                req,
                BucketsService,
                head_bucket,
                HeadBucketRequest,
                Bucket
            ),
            "/acyclic.objects.v1.BucketsService/DeleteBucket" => unary_server!(
                self,
                req,
                BucketsService,
                delete_bucket,
                DeleteBucketRequest,
                DeleteBucketResponse
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
impl<T> Clone for BucketsServiceServer<T> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        Self { inner }
    }
}
impl<T> tonic::server::NamedService for BucketsServiceServer<T> {
    const NAME: &'static str = "acyclic.objects.v1.BucketsService";
}
