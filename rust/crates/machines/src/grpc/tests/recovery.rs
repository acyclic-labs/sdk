use super::*;
use proptest::prelude::*;

#[test]
fn credential_failure_preserves_admitted_recovery_keys() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(16))
        .run(&(0..2usize, any::<u32>()), |(fail_at, seed)| {
            runtime.block_on(async {
                let (mut service, script, machine) =
                    scripted_operation_service(vec![WatchReply::Items(vec![])], vec![]);
                let key = IdempotencyKey::parse(&format!("{seed:08x}-0000-0000-0000-000000000041"))
                    .unwrap();
                service.expected_key = key;
                let (provider, shutdown, server) = serve_operation_service(service).await.unwrap();
                let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let next = calls.clone();
                let provider =
                    provider.with_credential_provider(CredentialProvider::new(move || {
                        let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        std::future::ready(if index == fail_at {
                            Err(acyclic_auth::credential::CredentialError::Invalid)
                        } else {
                            Ok("opaque.account+/=".into())
                        })
                    }));
                let outcome = provider.suspend(machine, key).await;
                let _ = shutdown.send(());
                server.await.unwrap().unwrap();
                if fail_at == 0 {
                    prop_assert!(matches!(outcome, Err(ProviderError::Rejected(_))));
                } else {
                    prop_assert_eq!(outcome, Err(ProviderError::Indeterminate(key)));
                }
                prop_assert_eq!(
                    calls.load(std::sync::atomic::Ordering::Relaxed),
                    fail_at + 1
                );
                prop_assert_eq!(script.lock().unwrap().calls.clone(), Vec::<&str>::new());
                // Rejected discards the key; the same post-admission assertion rejects it.
                if fail_at > 0 {
                    prop_assert_ne!(
                        Err::<(), _>(ProviderError::Rejected("renewal failed".into())),
                        Err(ProviderError::Indeterminate(key))
                    );
                }
                Ok(())
            })
        })
        .unwrap();
}

#[tokio::test]
async fn recovered_forks_reject_empty_and_duplicate_child_sets_before_fanout() {
    let provider = GrpcProvider::grpc_with_auth(
        TonicEndpoint::from_static("http://127.0.0.1:1").connect_lazy(),
        BearerAuth::default(),
    );
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
        guest: None,
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
async fn interrupted_mutation_retains_key_until_explicit_recovery()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000042")?;
    for first_watch in [
        WatchReply::Items(Vec::new()),
        WatchReply::Items(vec![WatchItem::Error(Code::Unavailable)]),
        WatchReply::Reject(Code::Unknown),
        WatchReply::Items(vec![WatchItem::State(operation_state(
            OperationId::parse("00000000-0000-0000-0000-000000000043")?,
            wire::OperationStatus::Succeeded,
        ))]),
    ] {
        let (service, script, machine) = scripted_operation_service(
            vec![
                first_watch,
                WatchReply::Items(vec![WatchItem::State(operation_state(
                    operation,
                    wire::OperationStatus::Succeeded,
                ))]),
            ],
            vec![],
        );
        let key = service.expected_key;
        let (provider, shutdown, server) = serve_operation_service(service).await?;
        assert_eq!(
            provider.suspend(machine, key).await,
            Err(ProviderError::Indeterminate(key))
        );
        assert_eq!(script.lock().expect("script").calls, ["watch"]);
        assert_eq!(
            provider.recover(key).await?,
            MutationOutcome::Suspended(machine)
        );
        assert_eq!(script.lock().expect("script").calls, ["watch", "watch"]);
        assert!(script.lock().expect("script").inspections.is_empty());
        let _ = shutdown.send(());
        server.await??;
    }
    Ok(())
}

#[tokio::test]
async fn watch_errors_never_consume_an_inspection_or_reopen()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for code in [
        Code::Unknown,
        Code::Unavailable,
        Code::DeadlineExceeded,
        Code::Cancelled,
        Code::Internal,
        Code::PermissionDenied,
    ] {
        let (service, script, machine) =
            scripted_operation_service(vec![WatchReply::Reject(code)], vec![Err(Code::Unknown)]);
        let key = service.expected_key;
        let (provider, shutdown, server) = serve_operation_service(service).await?;
        assert_eq!(
            provider.suspend(machine, key).await,
            Err(ProviderError::Indeterminate(key))
        );
        let _ = shutdown.send(());
        server.await??;
        let script = script.lock().expect("script");
        assert_eq!(script.calls, ["watch"]);
        assert_eq!(script.inspections.len(), 1);
        assert!(script.watches.is_empty());
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn quiet_watch_receives_its_terminal_producer_event_without_inspection()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000042")?;
    let (service, script, machine) = scripted_operation_service(
        vec![WatchReply::Delayed(
            operation_state(operation, wire::OperationStatus::Succeeded),
            Duration::from_secs(61),
        )],
        vec![],
    );
    let key = service.expected_key;
    let (provider, shutdown, server) = serve_operation_service(service).await?;
    let before = tokio::time::Instant::now();
    assert_eq!(
        provider.suspend(machine, key).await?,
        MutationOutcome::Suspended(machine)
    );
    assert!(before.elapsed() >= Duration::from_secs(61));
    let _ = shutdown.send(());
    server.await??;
    let script = script.lock().expect("script");
    assert_eq!(script.calls, ["watch"]);
    assert!(script.inspections.is_empty());
    assert!(script.watches.is_empty());
    Ok(())
}

#[tokio::test]
async fn dropping_accepted_wait_does_not_cancel_native_operation()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (service, script, machine) = scripted_operation_service(vec![WatchReply::Silent], vec![]);
    let key = service.expected_key;
    let started = script.lock().expect("script").started.clone();
    let (provider, shutdown, server) = serve_operation_service(service).await?;
    let mut wait = Box::pin(provider.suspend(machine, key));
    tokio::select! {
        result = &mut wait => panic!("silent wait completed: {result:?}"),
        () = started.notified() => {}
    }
    drop(wait);
    drop(provider);
    assert!(!script.lock().expect("script").calls.contains(&"cancel"));
    let _ = shutdown.send(());
    server.await??;
    Ok(())
}

#[tokio::test]
async fn terminal_watch_failures_remain_authoritative()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000042")?;
    for (status, expected) in [
        (wire::OperationStatus::Failed, ProviderError::Failed),
        (wire::OperationStatus::Cancelled, ProviderError::Cancelled),
    ] {
        let (service, script, machine) = scripted_operation_service(
            vec![WatchReply::Items(vec![WatchItem::State(operation_state(
                operation, status,
            ))])],
            vec![],
        );
        let key = service.expected_key;
        let (provider, shutdown, server) = serve_operation_service(service).await?;
        assert_eq!(provider.suspend(machine, key).await, Err(expected));
        let _ = shutdown.send(());
        server.await??;
        assert_eq!(script.lock().expect("script").calls, ["watch"]);
    }
    Ok(())
}
