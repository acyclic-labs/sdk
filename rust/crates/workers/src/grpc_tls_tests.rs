use super::*;

// Test trust selection in fresh processes.
#[tokio::test]
async fn platform_roots_and_exclusive_ca() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some((pem, key, unrelated, mode)) =
        acyclic_grpc_observability::test_support::tls_fixture(module_path!())?
    else { return Ok(()); };
    let incoming = tonic::transport::server::TcpIncoming::bind(([127, 0, 0, 1], 0).into())?;
    let endpoint = format!("https://localhost:{}", incoming.local_addr()?.port());
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let identity = tonic::transport::Identity::from_pem(&pem, key);
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))?
            .add_service(InspectService)
            .serve_with_incoming_shutdown(incoming, async { let _ = stopped.await; }),
    );
    let result = async {
        let authorization = BearerAuth::new("private-token")
            .map_err(|()| ConnectError::InvalidCredential)?;
        let platform = connect(&endpoint, authorization.clone(), None).await;
        if mode == "file" || mode == "dir" {
            let client = platform?;
            let mut client = client;
            client.inspect_job(wire::InspectJobRequest::default()).await?;
        } else {
            let error = platform.err().ok_or("private endpoint unexpectedly trusted")?;
            assert!(std::error::Error::source(&error).is_some(), "cause lost: {error:?}");
            let options = serde_json::from_value(serde_json::json!({"endpoint": endpoint, "token": "private-token"}))?;
            let error = crate::client_binding::Client::connect(options, tokio_util::sync::CancellationToken::new()).await.err().ok_or("typed client unexpectedly trusted")?;
            assert_eq!(error.code, "unavailable");
        }
        let client = connect(&endpoint, authorization.clone(), Some(pem.as_bytes())).await?;
        let mut client = client;
        client.inspect_job(wire::InspectJobRequest::default()).await?;
        let denied = connect(&endpoint, authorization, Some(&unrelated)).await;
        let error = denied.err().ok_or("unrelated supplied CA unexpectedly trusted")?;
        assert!(std::error::Error::source(&error).is_some(), "cause lost: {error:?}");
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }.await;
    let _ = shutdown.send(());
    server.await??;
    result
}

#[derive(Clone)]
struct InspectService;
impl tonic::server::NamedService for InspectService {
    const NAME: &'static str = "acyclic.workers.v1.WorkersService";
}
struct Inspect;
impl tonic::server::UnaryService<wire::InspectJobRequest> for Inspect {
    type Response = wire::InspectJobResponse;
    type Future = std::future::Ready<Result<tonic::Response<Self::Response>, tonic::Status>>;
    fn call(&mut self, request: tonic::Request<wire::InspectJobRequest>) -> Self::Future {
        assert_eq!(request.metadata().get("authorization").and_then(|value| value.to_str().ok()), Some("Bearer private-token"));
        std::future::ready(Ok(tonic::Response::new(wire::InspectJobResponse::default())))
    }
}
impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>> for InspectService {
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: tonic::codegen::http::Request<tonic::body::Body>) -> Self::Future {
        assert_eq!(request.uri().path(), "/acyclic.workers.v1.WorkersService/InspectJob");
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::<wire::InspectJobResponse, wire::InspectJobRequest>::default());
            Ok(grpc.unary(Inspect, request).await)
        })
    }
}
