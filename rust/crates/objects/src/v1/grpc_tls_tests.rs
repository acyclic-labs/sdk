use super::*;

// Test trust selection in fresh processes.
#[tokio::test]
async fn platform_roots_and_exclusive_ca() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some((pem, mut server, unrelated, mode)) =
        acyclic_grpc_observability::tests::tls_fixture(module_path!())?
    else {
        return Ok(());
    };
    let incoming = tonic::transport::server::TcpIncoming::bind(([127, 0, 0, 1], 0).into())?;
    let endpoint = format!("https://localhost:{}", incoming.local_addr()?.port());
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        server
            .add_service(wire::buckets_service_server::BucketsServiceServer::new(
                Fixture(MemoryObjects::new(MemoryOptions::default())?),
            ))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            }),
    );
    let result = async {
        let platform = GrpcObjects::connect(&endpoint, "exact-token", None, None).await;
        if mode == "file" || mode == "dir" {
            let client = platform?;
            client
                .create_bucket(wire::CreateBucketRequest {
                    name: "platform.roots".into(),
                    mutation: None,
                })
                .await?;
        } else {
            let error = platform
                .err()
                .ok_or("private endpoint unexpectedly trusted")?;
            assert!(
                std::error::Error::source(&error).is_some(),
                "cause lost: {error:?}"
            );
        }
        let client =
            GrpcObjects::connect(&endpoint, "exact-token", Some(pem.as_bytes()), None).await?;
        client
            .create_bucket(wire::CreateBucketRequest {
                name: "explicit.roots".into(),
                mutation: None,
            })
            .await?;
        let denied = GrpcObjects::connect(&endpoint, "exact-token", Some(&unrelated), None).await;
        let error = denied
            .err()
            .ok_or("unrelated supplied CA unexpectedly trusted")?;
        assert!(
            std::error::Error::source(&error).is_some(),
            "cause lost: {error:?}"
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    let _ = shutdown.send(());
    server.await??;
    result
}
