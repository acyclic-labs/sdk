use tonic::codegen::*;
#[derive(Debug, Clone)]
pub struct MultipartServiceClient<T> {
    inner: tonic::client::Grpc<T>,
}
impl<T> MultipartServiceClient<T>
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
        "acyclic.objects.v1.MultipartService";
        (create_multipart, CreateMultipartRequest, MultipartUpload, "CreateMultipart"),
        (list_parts, ListPartsRequest, ListPartsResponse, "ListParts"),
        (complete_multipart, CompleteMultipartRequest, ObjectInfo, "CompleteMultipart"),
        (abort_multipart, AbortMultipartRequest, AbortMultipartResponse, "AbortMultipart"),
    }
    pub async fn upload_part(
        &mut self,
        request: impl tonic::IntoStreamingRequest<Message = super::UploadPartRequest>,
    ) -> std::result::Result<tonic::Response<super::UploadedPart>, tonic::Status> {
        self.inner
            .ready()
            .await
            .map_err(|e| tonic::Status::unknown(format!("Service was not ready: {}", e.into())))?;
        let codec = tonic_prost::ProstCodec::default();
        let path =
            http::uri::PathAndQuery::from_static("/acyclic.objects.v1.MultipartService/UploadPart");
        let mut req = request.into_streaming_request();
        req.extensions_mut().insert(GrpcMethod::new(
            "acyclic.objects.v1.MultipartService",
            "UploadPart",
        ));
        self.inner.client_streaming(req, path, codec).await
    }
}
