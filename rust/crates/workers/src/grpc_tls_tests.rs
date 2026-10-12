use super::*;

// Test trust selection in fresh processes.
#[tokio::test]
async fn platform_roots_and_exclusive_ca() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    const FIXTURE: &str = "ACYCLIC_TLS_FIXTURE";
    const MODE: &str = "ACYCLIC_TLS_MODE";
    let Ok(directory) = std::env::var(FIXTURE) else {
        let directory = tempfile::tempdir()?;
        let trusted = directory.path().join("trusted");
        std::fs::create_dir(&trusted)?;
        let certified = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
        std::fs::write(trusted.join("root.pem"), certified.cert.pem())?;
        std::fs::write(directory.path().join("key.pem"), certified.signing_key.serialize_pem())?;
        let unrelated = rcgen::generate_simple_self_signed(["unrelated".to_owned()])?;
        std::fs::write(directory.path().join("unrelated.pem"), unrelated.cert.pem())?;
        std::fs::write(directory.path().join("empty.pem"), "")?;
        let module = module_path!();
        let test = format!("{}::platform_roots_and_exclusive_ca", module.split_once("::").map_or(module, |(_, path)| path));
        let modes = if cfg!(target_os = "linux") {
            &["unrelated", "file", "dir", "empty"][..]
        } else {
            &["unrelated"][..]
        };
        for mode in modes {
            let mut child = std::process::Command::new(std::env::current_exe()?);
            child.args([test.as_str(), "--exact", "--nocapture"])
                .env(FIXTURE, directory.path()).env(MODE, mode)
                .env_remove("SSL_CERT_FILE").env_remove("SSL_CERT_DIR");
            match *mode {
                "file" => { child.env("SSL_CERT_FILE", trusted.join("root.pem")); }
                "dir" => { child.env("SSL_CERT_DIR", &trusted); }
                "empty" => { child.env("SSL_CERT_FILE", directory.path().join("empty.pem")); }
                _ => { child.env("SSL_CERT_FILE", directory.path().join("unrelated.pem")); }
            }
            let output = child.output()?;
            assert!(output.status.success(), "{mode}: {}\n{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        }
        return Ok(());
    };
    let directory = std::path::Path::new(&directory);
    let pem = std::fs::read_to_string(directory.join("trusted/root.pem"))?;
    let key = std::fs::read_to_string(directory.join("key.pem"))?;
    let unrelated = std::fs::read(directory.join("unrelated.pem"))?;
    let mode = std::env::var(MODE)?;
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
        let platform = connect(&endpoint, "private-token", None).await;
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
        let client = connect(&endpoint, "private-token", Some(pem.as_bytes())).await?;
        let mut client = client;
        client.inspect_job(wire::InspectJobRequest::default()).await?;
        let denied = connect(&endpoint, "private-token", Some(&unrelated)).await;
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
