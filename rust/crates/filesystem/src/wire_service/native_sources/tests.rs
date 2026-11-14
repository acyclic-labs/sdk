use super::*;
use crate::{FilesystemWireLimits, FilesystemWireService, Fs};
use acyclic_fs_wire::wire::filesystem::v2 as wire;

fn operation(key: u8) -> Option<wire::OperationOptions> {
    Some(wire::OperationOptions {
        idempotency_key: vec![key; 16],
    })
}

#[tokio::test]
async fn native_source_rpcs_keep_read_only_observation_lazy_and_capture_replay_and_seal_real_files()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let directory = root.path().join("native");
    let provider = Arc::new(FilesystemNativeSources::new(BTreeMap::from([(
        "observed".to_owned(),
        directory.clone(),
    )]))?);
    let service = FilesystemWireService::new(Fs::memory(), FilesystemWireLimits::default())?
        .with_source_provider(provider);
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let serving = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(wire::filesystem_service_server::FilesystemServiceServer::new(service))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = stopped.await;
                },
            ),
    );
    let result = async {
        let mut client =
            wire::filesystem_service_client::FilesystemServiceClient::connect(endpoint).await?;
        let created = client
            .create_workspace(wire::CreateWorkspaceRequest {
                name: "observed".into(),
                profile: wire::FilesystemProfile::Portable as i32,
                operation: operation(1),
            })
            .await?
            .into_inner()
            .workspace
            .ok_or("workspace missing")?;
        let workspace = created.workspace.ok_or("workspace reference missing")?;
        let initial = client
            .get_source_state(wire::SourceStateRequest {
                workspace: Some(workspace.clone()),
            })
            .await?
            .into_inner();
        assert_eq!(initial.state, wire::SourceState::NeedsRescan as i32);
        assert_eq!(
            initial.reason,
            wire::SourceInvalidationReason::InitialSnapshotRequired as i32
        );
        assert!(initial.generation.is_none());
        // Observation must succeed while the configured native root is absent.
        std::fs::create_dir(&directory)?;
        std::fs::write(directory.join("value"), b"first")?;
        let reconciled = client
            .reconcile_source(wire::SourceOperationRequest {
                workspace: Some(workspace.clone()),
                operation: operation(2),
            })
            .await?
            .into_inner();
        assert_eq!(reconciled.state, wire::SourceState::Clean as i32);
        let generation = reconciled
            .generation
            .ok_or("reconciled generation missing")?;
        let read = client
            .read(wire::ReadRequest {
                generation: Some(generation),
                path: "/value".into(),
                maximum_bytes: 5,
                range: None,
            })
            .await?
            .into_inner();
        assert_eq!(read.contents, b"first");

        std::fs::write(directory.join("value"), b"second")?;
        let request = wire::SourceOperationRequest {
            workspace: Some(workspace.clone()),
            operation: operation(3),
        };
        let rescanned = client.rescan_source(request.clone()).await?.into_inner();
        assert_eq!(rescanned.state, wire::SourceState::Clean as i32);
        let generation = rescanned.generation.ok_or("rescanned generation missing")?;
        std::fs::write(directory.join("value"), b"third")?;
        let replayed = client.rescan_source(request).await?.into_inner();
        assert_eq!(replayed.generation, Some(generation.clone()));
        let read = client
            .read(wire::ReadRequest {
                generation: Some(generation),
                path: "/value".into(),
                maximum_bytes: 6,
                range: None,
            })
            .await?
            .into_inner();
        assert_eq!(read.contents, b"second");
        let sealed = client
            .seal_source(wire::SourceOperationRequest {
                workspace: Some(workspace.clone()),
                operation: operation(4),
            })
            .await?
            .into_inner();
        assert_eq!(sealed.state, wire::SourceState::Sealed as i32);
        let generation = sealed.generation.ok_or("sealed generation missing")?;
        std::fs::write(directory.join("value"), b"later")?;
        let read = client
            .read(wire::ReadRequest {
                generation: Some(generation.clone()),
                path: "/value".into(),
                maximum_bytes: 5,
                range: None,
            })
            .await?
            .into_inner();
        assert_eq!(read.contents, b"third");
        let state = client
            .get_source_state(wire::SourceStateRequest {
                workspace: Some(workspace),
            })
            .await?
            .into_inner();
        assert_eq!(state.state, wire::SourceState::Sealed as i32);
        assert_eq!(state.generation, Some(generation));
        drop(client);
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    let _ = stop.send(());
    serving.await??;
    result
}
