{
    #[allow(non_camel_case_types)]
    struct WatchOperationSvc<T: MachinesService>(pub Arc<T>);
    impl<
        T: MachinesService,
    > tonic::server::ServerStreamingService<super::OperationRequest>
    for WatchOperationSvc<T> {
        type Response = super::OperationState;
        type ResponseStream = T::WatchOperationStream;
        type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
        fn call(
            &mut self,
            request: tonic::Request<super::OperationRequest>,
        ) -> Self::Future {
            let inner = Arc::clone(&self.0);
            let fut = async move {
                <T as MachinesService>::watch_operation(&inner, request).await
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
        let method = WatchOperationSvc(inner);
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
        let res = grpc.server_streaming(method, req).await;
        Ok(res)
    };
    Box::pin(fut)
}
