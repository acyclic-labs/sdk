use super::*;

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
            .add_service(StreamServiceServer::with_interceptor(
                Service::new(Arc::new(MemoryStream::default())),
                |request: tonic::Request<()>| {
                    if request
                        .metadata()
                        .get("authorization")
                        .and_then(|value| value.to_str().ok())
                        != Some("Bearer exact-token")
                    {
                        return Err(tonic::Status::unauthenticated("invalid credential"));
                    }
                    Ok(request)
                },
            ))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            }),
    );
    let result = async {
        let platform = Client::connect_eager(&endpoint, "exact-token").await;
        if mode == "file" || mode == "dir" {
            let client = platform?;
            assert_eq!(
                client.tail(StreamPath::new("platform/roots")?).await,
                Err(StreamError::NotFound)
            );
            let installed = Arc::new(Client::connect([&endpoint], "exact-token", None).await?);
            assert_eq!(
                crate::Stream::new(Arc::clone(&installed), "platform/roots")?
                    .tail()
                    .await,
                Err(StreamError::NotFound)
            );
        } else {
            let error = platform
                .err()
                .ok_or("private endpoint unexpectedly trusted")?;
            assert!(
                std::error::Error::source(&error).is_some(),
                "cause lost: {error:?}"
            );
        }
        for client in [
            Client::connect([&endpoint], "exact-token", Some(pem.as_bytes())).await?,
            Client::connect_eager_with_ca_certificate(&endpoint, "exact-token", pem.as_bytes())
                .await?,
        ] {
            assert_eq!(
                client.tail(StreamPath::new("explicit/roots")?).await,
                Err(StreamError::NotFound)
            );
        }
        let denied =
            Client::connect_eager_with_ca_certificate(&endpoint, "exact-token", &unrelated).await;
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
