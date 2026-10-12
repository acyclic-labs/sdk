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
            .add_service(TlsService)
            .serve_with_incoming_shutdown(incoming, async { let _ = stopped.await; }),
    );
    let result = async {
        let platform = crate::grpc::connect(&endpoint, "private-token").await;
        if mode == "file" || mode == "dir" {
            let client = platform?;
            let mut client = client;
            client.inspect_actor(wire::InspectActorRequest::default()).await?;
        } else {
            let error = platform.err().ok_or("private endpoint unexpectedly trusted")?;
            assert!(std::error::Error::source(&error).is_some(), "cause lost: {error:?}");
            let error = crate::client::connect(&endpoint, "private-token").await.err().ok_or("typed client unexpectedly trusted")?;
            assert_eq!(error.code_name(), "unavailable");
        }
        let client = crate::grpc::connect_with_ca_certificate(&endpoint, "private-token", Some(pem.as_bytes())).await?;
        let mut client = client;
        client.inspect_actor(wire::InspectActorRequest::default()).await?;
        let denied = crate::grpc::connect_with_ca_certificate(&endpoint, "private-token", Some(&unrelated)).await;
        let error = denied.err().ok_or("unrelated supplied CA unexpectedly trusted")?;
        assert!(std::error::Error::source(&error).is_some(), "cause lost: {error:?}");
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }.await;
    let _ = shutdown.send(());
    server.await??;
    result
}
