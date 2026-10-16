{
    #[allow(non_camel_case_types)]
    struct CancelSvc<T: MachinesService>(pub Arc<T>);
    impl<T: MachinesService> tonic::server::UnaryService<super::OperationRequest>
    for CancelSvc<T> {
        type Response = super::OperationState;
        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
        fn call(
            &mut self,
            request: tonic::Request<super::OperationRequest>,
        ) -> Self::Future {
            let inner = Arc::clone(&self.0);
            let fut = async move {
                <T as MachinesService>::cancel(&inner, request).await
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
        let method = CancelSvc(inner);
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
