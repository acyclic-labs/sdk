{
    Box::pin(async move {
        let mut response = http::Response::new(tonic::body::Body::default());
        let headers = response.headers_mut();
        headers
            .insert(
                tonic::Status::GRPC_STATUS,
                (tonic::Code::Unimplemented as i32).into(),
            );
        headers.insert(http::header::CONTENT_TYPE, tonic::metadata::GRPC_CONTENT_TYPE);
        Ok(response)
    })
}
