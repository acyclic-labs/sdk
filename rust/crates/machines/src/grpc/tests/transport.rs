use super::*;

#[tokio::test]
async fn bearer_crosses_every_generated_rpc_and_client_clone()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000001")?;
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000002")?;
    let machine = MachineId::parse("00000000-0000-0000-0000-000000000003")?;
    let state = operation_state(operation, wire::OperationStatus::Succeeded);
    let service = OperationService {
        expected_key: key,
        expected_operation: operation,
        recovered: recovered_suspend(operation, operation, machine),
        inspected: state.clone(),
        cancelled: state,
        watch: WatchReply::Items(Vec::new()),
    };
    let (machines, shutdown_tx, server) = serve_operation_service(service).await?;
    let client = machines.client;
    macro_rules! observe {
        ($method:ident, $request:ident) => {
            let error = client
                .clone()
                .$method(wire::$request::default())
                .await
                .unwrap_err();
            assert_ne!(error.code(), Code::Unauthenticated, stringify!($method));
        };
    }
    observe!(qualify_image, QualifyImageRequest);
    observe!(create, CreateMachineRequest);
    observe!(checkpoint, CheckpointMachineRequest);
    observe!(fork, ForkCheckpointRequest);
    observe!(fork_machine, ForkMachineRequest);
    observe!(suspend, MachineMutationRequest);
    observe!(wake, MachineMutationRequest);
    observe!(set_suspension_policy, SetSuspensionPolicyRequest);
    observe!(destroy_machine, MachineMutationRequest);
    observe!(destroy_checkpoint, CheckpointMutationRequest);
    observe!(recover, RecoverRequest);
    observe!(inspect_machine, InspectMachineRequest);
    observe!(inspect_checkpoint, InspectCheckpointRequest);
    observe!(list_machines, ListMachinesRequest);
    observe!(events, EventsRequest);
    observe!(usage, UsageRequest);
    observe!(cancel, OperationRequest);
    observe!(inspect_operation, OperationRequest);
    let mut stream = client
        .clone()
        .watch_operation(operation_request(operation))
        .await?
        .into_inner();
    assert!(stream.message().await?.is_none());
    let _ = shutdown_tx.send(());
    server.await??;
    Ok(())
}

#[tokio::test]
async fn remote_bearer_crosses_mtls_constructor_and_operation_clones()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tonic::transport::ServerTlsConfig;

    let server_identity = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
    let server_pem = server_identity.cert.pem();
    let server_key = server_identity.signing_key.serialize_pem();
    let ca_key = KeyPair::generate()?;
    let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key)?;
    let issuer = Issuer::new(ca_params, ca_key);
    let client_key = KeyPair::generate()?;
    let mut client_params = CertificateParams::new(Vec::<String>::new())?;
    client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client_cert = client_params.signed_by(&client_key, &issuer)?;
    let client_pem = client_cert.pem();
    let client_key_pem = client_key.serialize_pem();
    let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000001")?;
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000002")?;
    let machine = MachineId::parse("00000000-0000-0000-0000-000000000003")?;
    let state = operation_state(operation, wire::OperationStatus::Succeeded);
    let service = OperationService {
        expected_key: key,
        expected_operation: operation,
        recovered: recovered_suspend(operation, operation, machine),
        inspected: state.clone(),
        cancelled: operation_state(operation, wire::OperationStatus::Cancelled),
        watch: WatchReply::Items(vec![WatchItem::State(state)]),
    };
    let observed = Arc::new(AtomicUsize::new(0));
    let received = observed.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("https://localhost:{}", listener.local_addr()?.port());
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let mut builder = Server::builder().tls_config(
        ServerTlsConfig::new()
            .identity(Identity::from_pem(&server_pem, &server_key))
            .client_ca_root(Certificate::from_pem(ca_cert.pem())),
    )?;
    let server = tokio::spawn(async move {
        builder
            .add_service(MachinesServiceServer::with_interceptor(
                service,
                move |request: Request<()>| {
                    let values: Vec<_> =
                        request.metadata().get_all("authorization").iter().collect();
                    if values.len() != 1
                        || values
                            .first()
                            .is_none_or(|value| *value != "Bearer opaque.account+/=")
                    {
                        return Err(Status::unauthenticated("missing or substituted bearer"));
                    }
                    received.fetch_add(1, Ordering::SeqCst);
                    Ok(request)
                },
            ))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    let tls = || Tls {
        ca: server_pem.as_bytes(),
        certificate: client_pem.as_bytes(),
        private_key: client_key_pem.as_bytes(),
    };
    let result: Result<(), Box<dyn std::error::Error + Send + Sync>> = async {
        let denied = GrpcProvider::connect(&endpoint, tls(), None).await?;
        assert!(denied.recover_operation(key).await.is_err());
        let machines = GrpcProvider::connect(&endpoint, tls(), Some("opaque.account+/=")).await?;
        assert_eq!(machines.clone().recover_operation(key).await?, operation);
        assert_eq!(
            machines.clone().inspect_operation(operation).await?.phase,
            OperationPhase::Succeeded
        );
        assert_eq!(
            machines.clone().cancel(operation).await?.phase,
            OperationPhase::Cancelled
        );
        let mut watch = machines.clone().watch_operation(operation).await?;
        assert_eq!(
            watch.next().await.transpose()?.map(|value| value.phase),
            Some(OperationPhase::Succeeded)
        );
        assert!(watch.next().await.is_none());
        assert_eq!(observed.load(Ordering::SeqCst), 4);
        Ok(())
    }
    .await;
    let _ = shutdown_tx.send(());
    server.await??;
    result
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn local_bearer_crosses_owner_confined_socket_and_operation_clones()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::os::unix::fs::PermissionsExt as _;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::UnixListener;
    use tokio_stream::wrappers::UnixListenerStream;

    let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000001")?;
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000002")?;
    let machine = MachineId::parse("00000000-0000-0000-0000-000000000003")?;
    let state = operation_state(operation, wire::OperationStatus::Succeeded);
    let service = OperationService {
        expected_key: key,
        expected_operation: operation,
        recovered: recovered_suspend(operation, operation, machine),
        inspected: state.clone(),
        cancelled: operation_state(operation, wire::OperationStatus::Cancelled),
        watch: WatchReply::Items(vec![WatchItem::State(state)]),
    };
    let directory = std::env::temp_dir().join(format!("machines-bearer-{}", Uuid::new_v4()));
    std::fs::create_dir(&directory)?;
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
    let socket = directory.join("machines.sock");
    let listener = UnixListener::bind(&socket)?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    let observed = Arc::new(AtomicUsize::new(0));
    let received = observed.clone();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(MachinesServiceServer::with_interceptor(
                service,
                move |request: Request<()>| {
                    let values: Vec<_> =
                        request.metadata().get_all("authorization").iter().collect();
                    if values.len() != 1
                        || values
                            .first()
                            .is_none_or(|value| *value != "Bearer opaque.account+/=")
                    {
                        return Err(Status::unauthenticated("missing or substituted bearer"));
                    }
                    received.fetch_add(1, Ordering::SeqCst);
                    Ok(request)
                },
            ))
            .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    let result: Result<(), Box<dyn std::error::Error + Send + Sync>> = async {
        let denied = GrpcProvider::connect_local(&socket, None).await?;
        assert!(denied.recover_operation(key).await.is_err());
        let machines = GrpcProvider::connect_local(&socket, Some("opaque.account+/=")).await?;
        assert_eq!(machines.clone().recover_operation(key).await?, operation);
        assert_eq!(
            machines.clone().inspect_operation(operation).await?.phase,
            OperationPhase::Succeeded
        );
        assert_eq!(
            machines.clone().cancel(operation).await?.phase,
            OperationPhase::Cancelled
        );
        let mut watch = machines.clone().watch_operation(operation).await?;
        assert_eq!(
            watch.next().await.transpose()?.map(|value| value.phase),
            Some(OperationPhase::Succeeded)
        );
        assert!(watch.next().await.is_none());
        assert_eq!(observed.load(Ordering::SeqCst), 4);
        Ok(())
    }
    .await;
    let _ = shutdown_tx.send(());
    let server_result = server.await;
    // Attempt both removals before propagating server or cleanup errors.
    let socket_cleanup = std::fs::remove_file(&socket);
    let directory_cleanup = std::fs::remove_dir(&directory);
    server_result??;
    socket_cleanup?;
    directory_cleanup?;
    result
}

#[test]
fn bearer_is_opaque_sensitive_and_replaces_existing_authorization()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut auth = BearerAuth::new("opaque.account+/=")?;
    let mut request = Request::new(());
    request
        .metadata_mut()
        .append("authorization", MetadataValue::from_static("stale"));
    let request = auth.call(request)?;
    let value = request
        .metadata()
        .get("authorization")
        .ok_or("missing authorization")?;
    assert_eq!(value, "Bearer opaque.account+/=");
    assert!(value.is_sensitive());
    assert_eq!(
        request.metadata().get_all("authorization").iter().count(),
        1
    );
    for invalid in [
        "",
        "   ",
        "opaque\nsecret",
        "opaque\rsecret",
        "opaque\0secret",
    ] {
        assert!(matches!(
            BearerAuth::new(invalid),
            Err(ProviderError::Invalid(_))
        ));
    }
    Ok(())
}

#[tokio::test]
async fn remote_transport_rejects_plaintext_before_connecting() {
    let result = GrpcProvider::connect(
        "http://example.invalid",
        Tls {
            ca: b"",
            certificate: b"",
            private_key: b"",
        },
        None,
    )
    .await;
    assert!(matches!(result, Err(ProviderError::Invalid(_))));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn local_socket_metadata_must_be_owner_private() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::PermissionsExt as _;

    let directory = std::env::temp_dir().join(format!("acyclic-machines-{}", Uuid::new_v4()));
    std::fs::create_dir(&directory)?;
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
    let path = directory.join("service.sock");
    let listener = tokio::net::UnixListener::bind(&path)?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    let uid = rustix::process::geteuid().as_raw();
    assert!(owner_private_socket(&path, uid).is_ok());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o660))?;
    assert!(owner_private_socket(&path, uid).is_err());
    drop(listener);
    std::fs::remove_file(&path)?;
    std::fs::remove_dir(&directory)?;
    Ok(())
}
