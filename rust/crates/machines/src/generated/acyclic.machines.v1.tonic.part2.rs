{
    #[allow(non_camel_case_types)]
    struct QualifyImageSvc<T: MachinesService>(pub Arc<T>);
    impl<T: MachinesService> tonic::server::UnaryService<super::QualifyImageRequest>
    for QualifyImageSvc<T> {
        type Response = super::ImageQualification;
        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
        fn call(
            &mut self,
            request: tonic::Request<super::QualifyImageRequest>,
        ) -> Self::Future {
            let inner = Arc::clone(&self.0);
            let fut = async move {
                <T as MachinesService>::qualify_image(&inner, request).await
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
        let method = QualifyImageSvc(inner);
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
