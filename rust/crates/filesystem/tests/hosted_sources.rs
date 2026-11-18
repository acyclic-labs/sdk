//! Real native sources retain observation and bind each operation to public providers.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_fs::simulation::sites;
use acyclic_fs_wire::wire::filesystem::v2 as wire;
use acyclic_native_runtime::testing::Schedule;
use std::{collections::BTreeMap, sync::Arc};
use {
    acyclic_fs::{
        CancellationToken, FilesystemNativeSources, FilesystemSourceProvider, FilesystemWireLimits,
        FilesystemWireService, Fs, IdempotencyKey, ReconcileOutcome, SourceError, SourceOptions,
        SourceState,
    },
    acyclic_fs_work::performance::WorkBudget,
};

#[tokio::test]
async fn source_rebinding_preserves_pending_watch_changes_and_rejects_another_workspace()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let streams = Arc::new(acyclic_stream::MemoryStream::default());
    let objects = Arc::new(acyclic_objects::MemoryObjects::memory());
    // Separate schedules attribute every storage call to exactly one store.
    let (original_schedule, _original_events) = Schedule::new();
    let original_store = acyclic_fs::Simulation::wrap(
        &original_schedule,
        acyclic_fs::StreamAuthorityStore::new(streams.clone()),
        acyclic_fs::LogicalObjectStore::new(objects.clone()),
    );
    let (authority, blobs) = original_store.stores();
    let original = Fs::new(authority, blobs, acyclic_fs::EmbeddedCapabilities::MEMORY);
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("value"), b"before")?;
    let workspace = Box::pin(original.attach_directory(
        "retained-source",
        root.path(),
        SourceOptions::default(),
    ))
    .await?;
    let source = workspace.source().ok_or("source missing")?.clone();
    let (fresh_schedule, _fresh_events) = Schedule::new();
    let fresh_store = acyclic_fs::Simulation::wrap(
        &fresh_schedule,
        acyclic_fs::StreamAuthorityStore::new(streams),
        acyclic_fs::LogicalObjectStore::new(objects),
    );
    let (authority, blobs) = fresh_store.stores();
    let fresh = Fs::new(authority, blobs, acyclic_fs::EmbeddedCapabilities::MEMORY);
    let admitted = fresh
        .open_workspace(
            "retained-source",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    let other_root = tempfile::tempdir()?;
    let other = Box::pin(fresh.attach_directory(
        "other-source",
        other_root.path(),
        SourceOptions::default(),
    ))
    .await?;
    assert!(matches!(
        source.bind_workspace(other).await,
        Err(SourceError::BindingMismatch)
    ));

    std::fs::write(root.path().join("value"), b"after")?;
    let cancellation = CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    source.changed_with_native_work(&work).await?;
    for site in [
        sites::AUTHORITY_HEAD,
        sites::AUTHORITY_APPEND,
        sites::OBJECT_PUT,
        sites::OBJECT_READ,
        sites::OBJECT_READ_MANY,
    ] {
        original_schedule.inject_next(site, acyclic_fs::SimulationFault::RejectBefore);
    }
    let fresh_before = fresh_schedule.site_calls().len();
    let mut bound = source.bind_workspace(admitted).await?;
    let key = IdempotencyKey::new();
    let ReconcileOutcome::Clean(generation) = bound.reconcile_with_key(key).await? else {
        return Err("retained watcher interval was not reconciled".into());
    };
    assert_eq!(
        generation
            .read("/value", 5, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value,
        acyclic_stream::OwnedBytes::from_static(b"after")
    );
    let ReconcileOutcome::Clean(replayed) = bound.reconcile_with_key(key).await? else {
        return Err("source retry lost its original outcome".into());
    };
    assert_eq!(generation.id(), replayed.id());
    assert_eq!(bound.state(), SourceState::Clean);
    assert!(
        fresh_schedule.site_calls()[fresh_before..]
            .iter()
            .any(|call| call.site == sites::AUTHORITY_APPEND)
    );
    assert!(
        original_schedule
            .site_calls()
            .iter()
            .all(|call| call.fault::<acyclic_fs::SimulationFault>().is_none())
    );
    Ok(())
}

fn operation(key: u8) -> Option<wire::OperationOptions> {
    Some(wire::OperationOptions {
        idempotency_key: vec![key; 16],
    })
}

#[tokio::test]
async fn native_source_rpcs_keep_read_only_observation_lazy_and_capture_replay_and_seal_real_files()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    let directory = root.path().join("native");
    let provider = Arc::new(FilesystemNativeSources::new(BTreeMap::from([(
        "observed".to_owned(),
        directory.clone(),
    )]))?);
    let filesystem = Fs::memory();
    let service = FilesystemWireService::new(filesystem.clone(), FilesystemWireLimits::default())?
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
        let original_reconcile = reconciled.clone();
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
        let original_rescan = rescanned.clone();
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
        let original_seal = sealed.clone();
        let generation = sealed.generation.ok_or("sealed generation missing")?;
        std::fs::write(directory.join("value"), b"later")?;
        let replayed = client
            .reconcile_source(wire::SourceOperationRequest {
                workspace: Some(workspace.clone()),
                operation: operation(2),
            })
            .await?
            .into_inner();
        assert_eq!(replayed.generation, original_reconcile.generation);
        let replayed = client
            .rescan_source(wire::SourceOperationRequest {
                workspace: Some(workspace.clone()),
                operation: operation(3),
            })
            .await?
            .into_inner();
        assert_eq!(replayed.generation, original_rescan.generation);
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
        let admitted = filesystem
            .open_workspace("observed", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value;
        let before = admitted
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value
            .id();
        std::fs::remove_dir_all(&directory)?;
        let missing = FilesystemNativeSources::new(BTreeMap::from([(
            "observed".to_owned(),
            directory.clone(),
        )]))?;
        assert_eq!(
            missing
                .rescan(&admitted, IdempotencyKey::from_bytes([5; 16]))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable,
            "a new operation must still require the native root"
        );
        for (key, expected) in [
            (2, original_reconcile),
            (3, original_rescan),
            (4, original_seal),
        ] {
            // Completed retries require neither a retained watcher nor the native root.
            let restarted = FilesystemNativeSources::new(BTreeMap::from([(
                "observed".to_owned(),
                directory.clone(),
            )]))?;
            let operation = IdempotencyKey::from_bytes([key; 16]);
            let replayed = match key {
                2 => restarted.reconcile(&admitted, operation).await?,
                3 => restarted.rescan(&admitted, operation).await?,
                _ => restarted.seal(&admitted, operation).await?,
            };
            assert_eq!(
                replayed.generation_id.map(|id| id.to_bytes().to_vec()),
                expected
                    .generation
                    .map(|generation| generation.generation_id)
            );
            let after = admitted
                .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map_err(|failure| failure.error)?
                .value
                .id();
            assert_eq!(
                after, before,
                "source retry must not capture later native bytes"
            );
        }
        drop(client);
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    let _ = stop.send(());
    serving.await??;
    result
}
