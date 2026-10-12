use tonic::codegen::*;
#[derive(Debug, Clone)]
pub struct ObjectsServiceClient<T> {
    inner: tonic::client::Grpc<T>,
}
impl<T> ObjectsServiceClient<T>
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
    pub async fn put_object(
        &mut self,
        request: impl tonic::IntoStreamingRequest<Message = super::PutObjectRequest>,
    ) -> std::result::Result<tonic::Response<super::ObjectInfo>, tonic::Status> {
        self.inner
            .ready()
            .await
            .map_err(|e| tonic::Status::unknown(format!("Service was not ready: {}", e.into())))?;
        let codec = tonic_prost::ProstCodec::default();
        let path =
            http::uri::PathAndQuery::from_static("/acyclic.objects.v1.ObjectsService/PutObject");
        let mut req = request.into_streaming_request();
        req.extensions_mut().insert(GrpcMethod::new(
            "acyclic.objects.v1.ObjectsService",
            "PutObject",
        ));
        self.inner.client_streaming(req, path, codec).await
    }
    pub async fn get_object(
        &mut self,
        request: impl tonic::IntoRequest<super::GetObjectRequest>,
    ) -> std::result::Result<
        tonic::Response<tonic::codec::Streaming<super::GetObjectResponse>>,
        tonic::Status,
    > {
        self.inner
            .ready()
            .await
            .map_err(|e| tonic::Status::unknown(format!("Service was not ready: {}", e.into())))?;
        let codec = tonic_prost::ProstCodec::default();
        let path =
            http::uri::PathAndQuery::from_static("/acyclic.objects.v1.ObjectsService/GetObject");
        let mut req = request.into_request();
        req.extensions_mut().insert(GrpcMethod::new(
            "acyclic.objects.v1.ObjectsService",
            "GetObject",
        ));
        self.inner.server_streaming(req, path, codec).await
    }
    unary_client! {
        "acyclic.objects.v1.ObjectsService";
        (head_object, HeadObjectRequest, HeadObjectResponse, "HeadObject"),
        (delete_object, DeleteObjectRequest, DeleteObjectResponse, "DeleteObject"),
        (list_objects, ListObjectsRequest, ListObjectsResponse, "ListObjects"),
    }
}
