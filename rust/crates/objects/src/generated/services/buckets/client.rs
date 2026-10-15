use tonic::codegen::*;
#[derive(Debug, Clone)]
pub struct BucketsServiceClient<T> {
    inner: tonic::client::Grpc<T>,
}
impl<T> BucketsServiceClient<T>
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
    /// Limits the maximum size of a decoded message.
    #[must_use]
    pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
        self.inner = self.inner.max_decoding_message_size(limit);
        self
    }
    /// Limits the maximum size of an encoded message.
    #[must_use]
    pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
        self.inner = self.inner.max_encoding_message_size(limit);
        self
    }
    unary_client! {
        "acyclic.objects.v1.BucketsService";
        (create_bucket, CreateBucketRequest, Bucket, "CreateBucket"),
        (head_bucket, HeadBucketRequest, Bucket, "HeadBucket"),
        (delete_bucket, DeleteBucketRequest, DeleteBucketResponse, "DeleteBucket"),
    }
}
