use super::*;

#[tokio::test]
async fn operation_for_rejects_a_substituted_nested_admission_operation()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000001")?;
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000002")?;
    let substituted = OperationId::parse("00000000-0000-0000-0000-000000000003")?;
    let machine = MachineId::parse("00000000-0000-0000-0000-000000000004")?;
    let service = OperationService {
        expected_key: key,
        expected_operation: operation,
        recovered: recovered_suspend(operation, substituted, machine),
        inspected: operation_state(operation, wire::OperationStatus::Pending),
        cancelled: operation_state(operation, wire::OperationStatus::Cancelled),
        watch: WatchReply::Items(Vec::new()),
    };
    let (machines, shutdown, server) = serve_operation_service(service).await?;

    assert!(matches!(
        machines.recover_operation(key).await,
        Err(ProviderError::Rejected(_))
    ));

    let _ = shutdown.send(());
    server.await??;
    Ok(())
}

#[tokio::test]
async fn operation_control_crosses_grpc_with_exact_identity_replay_and_terminal_state()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000011")?;
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000012")?;
    let machine = MachineId::parse("00000000-0000-0000-0000-000000000013")?;
    let pending = operation_state(operation, wire::OperationStatus::Pending);
    let succeeded = operation_state(operation, wire::OperationStatus::Succeeded);
    let service = OperationService {
        expected_key: key,
        expected_operation: operation,
        recovered: recovered_suspend(operation, operation, machine),
        inspected: pending.clone(),
        cancelled: operation_state(operation, wire::OperationStatus::Cancelled),
        watch: WatchReply::Items(vec![
            WatchItem::State(pending.clone()),
            WatchItem::State(pending.clone()),
            WatchItem::State(succeeded),
            WatchItem::State(pending),
        ]),
    };
    let (machines, shutdown, server) = serve_operation_service(service).await?;

    assert_eq!(machines.recover_operation(key).await?, operation);
    assert_eq!(
        machines.inspect_operation(operation).await?,
        OperationObservation {
            id: operation,
            phase: OperationPhase::Pending,
        }
    );
    assert_eq!(
        machines.cancel(operation).await?,
        OperationObservation {
            id: operation,
            phase: OperationPhase::Cancelled,
        }
    );
    let mut watch = machines.watch_operation(operation).await?;
    assert_eq!(
        watch.next().await.transpose()?.map(|value| value.phase),
        Some(OperationPhase::Pending)
    );
    assert_eq!(
        watch.next().await.transpose()?.map(|value| value.phase),
        Some(OperationPhase::Pending)
    );
    assert_eq!(
        watch.next().await.transpose()?.map(|value| value.phase),
        Some(OperationPhase::Succeeded)
    );
    assert!(watch.next().await.is_none());

    let _ = shutdown.send(());
    server.await??;
    Ok(())
}

#[tokio::test]
async fn rpcs_emit_spans_with_status_codes_and_no_credentials()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use acyclic_grpc_observability::tests::Capture;
    use tracing_subscriber::layer::SubscriberExt as _;

    let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000031")?;
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000032")?;
    let machine = MachineId::parse("00000000-0000-0000-0000-000000000033")?;
    let pending = operation_state(operation, wire::OperationStatus::Pending);
    let service = OperationService {
        expected_key: key,
        expected_operation: operation,
        recovered: recovered_suspend(operation, operation, machine),
        inspected: pending.clone(),
        cancelled: pending,
        watch: WatchReply::Items(Vec::new()),
    };
    let (machines, shutdown, server) = serve_operation_service(service).await?;
    // Two dispatchers make concurrent callsites recheck this subscriber.
    let _second = tracing::Dispatch::new(tracing_subscriber::registry());
    let seen = Capture::default();
    let default =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(seen.clone()));
    machines.inspect_operation(operation).await?;
    let substituted = OperationId::parse("00000000-0000-0000-0000-000000000034")?;
    assert!(machines.inspect_operation(substituted).await.is_err());
    drop(default);
    let _ = shutdown.send(());
    server.await??;

    let evidence = seen.0.lock().unwrap();
    let seen = &evidence.records;
    let span = "acyclic.machines.inspect_operation";
    let records = |field: &str| {
        seen.iter()
            .filter(|(s, f, _)| s == span && f == field)
            .map(|(_, _, value)| value.trim_matches('"'))
            .collect::<Vec<_>>()
    };
    assert_eq!(records("outcome"), ["ok", "err"], "{seen:?}");
    let invalid_argument = i32::from(tonic::Code::InvalidArgument).to_string();
    assert_eq!(records("rpc.code"), ["0", invalid_argument.as_str()]);
    assert_eq!(records("error.kind"), ["rejected"]);
    for (span, field, value) in seen.iter() {
        assert!(
            !["path", "token", "content", "body", "authorization"].contains(&field.as_str()),
            "{span} records {field}"
        );
        assert!(!value.contains("substituted"), "{value}");
    }
    Ok(())
}

#[tokio::test]
async fn all_operation_observation_paths_reject_substituted_identity()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000021")?;
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000022")?;
    let substituted = OperationId::parse("00000000-0000-0000-0000-000000000023")?;
    let machine = MachineId::parse("00000000-0000-0000-0000-000000000024")?;
    let substituted_state = operation_state(substituted, wire::OperationStatus::Pending);
    let service = OperationService {
        expected_key: key,
        expected_operation: operation,
        recovered: recovered_suspend(operation, operation, machine),
        inspected: substituted_state.clone(),
        cancelled: substituted_state.clone(),
        watch: WatchReply::Items(vec![WatchItem::State(substituted_state)]),
    };
    let (machines, shutdown, server) = serve_operation_service(service).await?;

    assert!(matches!(
        machines.inspect_operation(operation).await,
        Err(ProviderError::Rejected(_))
    ));
    assert!(matches!(
        machines.cancel(operation).await,
        Err(ProviderError::Rejected(_))
    ));
    let mut watch = machines.watch_operation(operation).await?;
    assert!(matches!(
        watch.next().await,
        Some(Err(ProviderError::Rejected(_)))
    ));

    let _ = shutdown.send(());
    server.await??;
    Ok(())
}

#[tokio::test]
async fn operation_watch_distinguishes_pre_admission_rejection_from_post_admission_ambiguity()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000031")?;
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000032")?;
    let machine = MachineId::parse("00000000-0000-0000-0000-000000000033")?;
    let base = OperationService {
        expected_key: key,
        expected_operation: operation,
        recovered: recovered_suspend(operation, operation, machine),
        inspected: operation_state(operation, wire::OperationStatus::Pending),
        cancelled: operation_state(operation, wire::OperationStatus::Cancelled),
        watch: WatchReply::Reject(Code::PermissionDenied),
    };
    let (machines, shutdown, server) = serve_operation_service(base.clone()).await?;
    assert!(matches!(
        machines.watch_operation(operation).await,
        Err(ProviderError::Rejected(_))
    ));
    let _ = shutdown.send(());
    server.await??;

    for code in [Code::Internal, Code::ResourceExhausted, Code::Unavailable] {
        let service = OperationService {
            watch: WatchReply::Items(vec![
                WatchItem::State(operation_state(operation, wire::OperationStatus::Pending)),
                WatchItem::Error(code),
            ]),
            ..base.clone()
        };
        let (machines, shutdown, server) = serve_operation_service(service).await?;
        let mut watch = machines.watch_operation(operation).await?;
        assert!(matches!(
            watch.next().await,
            Some(Ok(OperationObservation { id, phase: OperationPhase::Pending })) if id == operation
        ));
        assert_eq!(
            watch.next().await,
            Some(Err(ProviderError::OperationIndeterminate(operation)))
        );
        assert!(watch.next().await.is_none());
        let _ = shutdown.send(());
        server.await??;
    }

    let service = OperationService {
        watch: WatchReply::Items(vec![WatchItem::State(operation_state(
            operation,
            wire::OperationStatus::Pending,
        ))]),
        ..base
    };
    let (machines, shutdown, server) = serve_operation_service(service).await?;
    let mut watch = machines.watch_operation(operation).await?;
    assert!(matches!(
        watch.next().await,
        Some(Ok(OperationObservation { id, phase: OperationPhase::Pending })) if id == operation
    ));
    assert_eq!(
        watch.next().await,
        Some(Err(ProviderError::OperationIndeterminate(operation)))
    );
    assert!(watch.next().await.is_none());
    let _ = shutdown.send(());
    server.await??;
    Ok(())
}
