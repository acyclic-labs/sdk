use super::*;

#[tokio::test]
async fn recovered_forks_reject_empty_and_duplicate_child_sets_before_fanout() {
    let provider = GrpcProvider {
        client: wire::machines_service_client::MachinesServiceClient::with_interceptor(
            TonicEndpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            BearerAuth::default(),
        ),
    };
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000001")
        .unwrap_or_else(|_| unreachable!());
    let child =
        MachineId::parse("00000000-0000-0000-0000-000000000002").unwrap_or_else(|_| unreachable!());
    let recovered = |children| wire::RecoveredAdmission {
        operation: Some(wire::OperationId {
            value: operation.as_bytes().to_vec(),
        }),
        result: Some(wire::recovered_admission::Result::Fork(
            wire::ForkAdmission {
                checkpoint: Some(encode_checkpoint(
                    CheckpointId::parse("00000000-0000-0000-0000-000000000003")
                        .unwrap_or_else(|_| unreachable!()),
                )),
                children,
                operation: Some(wire::OperationId {
                    value: operation.as_bytes().to_vec(),
                }),
                contract: None,
            },
        )),
    };
    assert!(
        decode_recovered(&provider, recovered(Vec::new()))
            .await
            .is_err()
    );
    assert!(
        decode_recovered(
            &provider,
            recovered(vec![encode_machine(child), encode_machine(child)])
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn servers_without_live_fork_report_unsupported()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000041")?;
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000042")?;
    let machine = MachineId::parse("00000000-0000-0000-0000-000000000043")?;
    let service = OperationService {
        expected_key: key,
        expected_operation: operation,
        recovered: recovered_suspend(operation, operation, machine),
        inspected: operation_state(operation, wire::OperationStatus::Pending),
        cancelled: operation_state(operation, wire::OperationStatus::Cancelled),
        watch: WatchReply::Items(Vec::new()),
    };
    let (machines, shutdown, server) = serve_operation_service(service).await?;
    assert!(matches!(
        machines.fork_machine(machine, NonZeroU32::MIN, key).await,
        Err(ProviderError::Unsupported(_))
    ));
    let _ = shutdown.send(());
    server.await??;
    Ok(())
}

#[tokio::test]
async fn accepted_mutation_resumes_after_eof_or_interruption_without_resubmission()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000042")?;
    for first_watch in [
        WatchReply::Items(Vec::new()),
        WatchReply::Items(vec![WatchItem::Error(Code::Unavailable)]),
        WatchReply::Reject(Code::Unknown),
    ] {
        let (service, _script, machine) = scripted_operation_service(
            vec![
                first_watch,
                WatchReply::Items(vec![WatchItem::State(operation_state(
                    operation,
                    wire::OperationStatus::Succeeded,
                ))]),
            ],
            vec![Ok(operation_state(
                operation,
                wire::OperationStatus::Pending,
            ))],
        );
        let key = service.expected_key;
        let (machines, shutdown, server) = serve_operation_service(service).await?;
        let outcome =
            tokio::time::timeout(Duration::from_secs(10), machines.suspend(machine, key)).await?;
        let _ = shutdown.send(());
        server.await??;
        outcome?;
    }
    Ok(())
}

#[tokio::test]
async fn ambiguous_watch_always_inspects_before_returning_original_recovery_key()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000042")?;
    for final_inspection in [
        Ok(operation_state(operation, wire::OperationStatus::Succeeded)),
        Err(Code::Unknown),
    ] {
        let expected_success = final_inspection.is_ok();
        let (service, _script, machine) = scripted_operation_service(
            vec![WatchReply::Reject(Code::Unknown); 3],
            vec![Err(Code::Unknown), Err(Code::Unknown), final_inspection],
        );
        let key = service.expected_key;
        let (machines, shutdown, server) = serve_operation_service(service).await?;
        let outcome =
            tokio::time::timeout(Duration::from_secs(10), machines.suspend(machine, key)).await?;
        let _ = shutdown.send(());
        server.await??;
        if expected_success {
            outcome?;
        } else {
            assert_eq!(outcome, Err(ProviderError::Indeterminate(key)));
        }
    }
    Ok(())
}

#[tokio::test]
async fn silent_accepted_mutation_reconciles_the_same_operation()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000042")?;
    let (service, _script, machine) = scripted_operation_service(
        vec![
            WatchReply::Silent,
            WatchReply::Items(vec![WatchItem::State(operation_state(
                operation,
                wire::OperationStatus::Succeeded,
            ))]),
        ],
        vec![Ok(operation_state(
            operation,
            wire::OperationStatus::Pending,
        ))],
    );
    let key = service.expected_key;
    let (machines, shutdown, server) = serve_operation_service(service).await?;
    let outcome =
        tokio::time::timeout(Duration::from_secs(75), machines.suspend(machine, key)).await?;
    let _ = shutdown.send(());
    server.await??;
    outcome?;
    Ok(())
}

#[tokio::test]
async fn dropping_accepted_wait_does_not_cancel_native_operation()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (service, script, machine) = scripted_operation_service(vec![WatchReply::Silent], vec![]);
    let key = service.expected_key;
    let (machines, shutdown, server) = serve_operation_service(service).await?;
    let mut wait = Box::pin(machines.suspend(machine, key));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! {
                result = &mut wait => panic!("silent wait completed: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(10)) => {
                    if script.lock().expect("script").calls.contains(&"watch") {
                        break;
                    }
                }
            }
        }
    })
    .await?;
    drop(wait);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!script.lock().expect("script").calls.contains(&"cancel"));
    let _ = shutdown.send(());
    server.await??;
    Ok(())
}

#[tokio::test]
async fn permanent_observation_errors_retain_key_and_terminal_inspection_is_authoritative()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000042")?;
    for (watch, inspection, expected) in [
        (WatchReply::Reject(Code::Internal), vec![], None),
        (
            WatchReply::Reject(Code::Unknown),
            vec![Err(Code::Internal)],
            None,
        ),
        (
            WatchReply::Items(vec![]),
            vec![Ok(operation_state(
                operation,
                wire::OperationStatus::Failed,
            ))],
            Some(ProviderError::Failed),
        ),
        (
            WatchReply::Items(vec![]),
            vec![Ok(operation_state(
                operation,
                wire::OperationStatus::Cancelled,
            ))],
            Some(ProviderError::Cancelled),
        ),
    ] {
        let (service, _script, machine) = scripted_operation_service(vec![watch], inspection);
        let key = service.expected_key;
        let (machines, shutdown, server) = serve_operation_service(service).await?;
        let outcome =
            tokio::time::timeout(Duration::from_secs(5), machines.suspend(machine, key)).await?;
        let _ = shutdown.send(());
        server.await??;
        assert_eq!(
            outcome,
            Err(expected.unwrap_or(ProviderError::Indeterminate(key)))
        );
    }
    Ok(())
}
