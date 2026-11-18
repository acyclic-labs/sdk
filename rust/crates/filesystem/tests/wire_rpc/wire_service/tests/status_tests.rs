use super::*;
use tonic::Code;

#[tokio::test]
async fn absent_workspace_is_not_found_over_real_wire() -> Result<(), Box<dyn std::error::Error>> {
    use wire::filesystem_service_client::FilesystemServiceClient;
    use wire::filesystem_service_server::FilesystemServiceServer;
    let service = FilesystemWireService::new(Fs::memory(), FilesystemWireLimits::default())?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(FilesystemServiceServer::new(service))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = stopped.await;
                },
            ),
    );
    let result = async {
        let mut client = FilesystemServiceClient::connect(endpoint).await?;
        let response = client
            .open_workspace(wire::OpenWorkspaceRequest {
                selector: Some(wire::open_workspace_request::Selector::Name(
                    "absent".to_owned(),
                )),
            })
            .await;
        let error = response.err().ok_or("absent workspace was opened")?;
        Ok::<_, Box<dyn std::error::Error>>(error)
    }
    .await;
    let _ = stop.send(());
    server.await??;
    let error = result?;
    assert_eq!(error.code(), Code::NotFound, "{error}");
    Ok(())
}
