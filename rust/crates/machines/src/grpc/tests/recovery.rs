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
                let (mut service, script, machine) = scripted_operation_service(vec![]);
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
                prop_assert_eq!(
                    script.lock().unwrap().calls.clone(),
                    if fail_at == 1 {
                        vec!["suspend"]
                    } else {
                        vec![]
                    }
                );
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
        finish_admission(&provider, IdempotencyKey::new(), recovered(Vec::new()))
            .await
            .is_err()
    );
    assert!(
        finish_admission(
            &provider,
            IdempotencyKey::new(),
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
        join: JoinReply::Reject(Code::Unimplemented),
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

// These are internal protocol vectors, not provider response captures.
#[tokio::test]
async fn accepted_join_resumes_after_interruption_without_resubmission()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000042")?;
    let machine = MachineId::parse("00000000-0000-0000-0000-000000000043")?;
    for code in [
        Code::Unavailable,
        Code::Unknown,
        Code::Cancelled,
        Code::DeadlineExceeded,
        Code::Internal,
        Code::PermissionDenied,
    ] {
        for interrupted in [
            JoinReply::Reject(code),
            JoinReply::Stream(vec![Ok(settled_suspend(operation, machine)), Err(code)]),
        ] {
            let (service, script, _) = scripted_operation_service(vec![
                interrupted,
                JoinReply::Settlement(settled_suspend(operation, machine)),
            ]);
            let key = service.expected_key;
            let (machines, shutdown, server) = serve_operation_service(service).await?;
            assert_eq!(
                machines.suspend(machine, key).await,
                Err(ProviderError::Indeterminate(key))
            );
            assert_eq!(script.lock().expect("script").calls, ["suspend", "join"]);
            assert_eq!(
                machines.recover(key).await?,
                MutationOutcome::Suspended(machine)
            );
            assert_eq!(
                script.lock().expect("script").calls,
                ["suspend", "join", "recover", "join"]
            );
            let _ = shutdown.send(());
            server.await??;
        }
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn silent_join_has_no_client_polling_or_native_cancellation()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (service, script, machine) = scripted_operation_service(vec![JoinReply::Silent]);
    let key = service.expected_key;
    let (machines, shutdown, server) = serve_operation_service(service).await?;
    let mut wait = Box::pin(machines.suspend(machine, key));
    // The fixture uses the production physical RPC deadline. Headers precede the
    // wait: observation survives both that deadline and the deleted polling timer.
    let result = tokio::time::timeout(Duration::from_secs(75), &mut wait).await;
    assert!(result.is_err());
    assert_eq!(script.lock().expect("script").calls, ["suspend", "join"]);
    drop(wait);
    let _ = shutdown.send(());
    server.await??;
    Ok(())
}

#[tokio::test]
async fn dropping_accepted_join_does_not_cancel_native_operation()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (service, script, machine) = scripted_operation_service(vec![JoinReply::Silent]);
    let key = service.expected_key;
    let (machines, shutdown, server) = serve_operation_service(service).await?;
    let mut wait = Box::pin(machines.suspend(machine, key));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! {
                result = &mut wait => panic!("silent join completed: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(10)) => {
                    if script.lock().expect("script").calls.contains(&"join") { break; }
                }
            }
        }
    })
    .await?;
    drop(wait);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(script.lock().expect("script").calls, ["suspend", "join"]);
    let _ = shutdown.send(());
    server.await??;
    Ok(())
}

#[tokio::test]
async fn join_terminal_refusals_are_typed_and_transport_failures_retain_the_key()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000042")?;
    for (phase, expected) in [
        (wire::OperationStatus::Failed, ProviderError::Failed),
        (wire::OperationStatus::Cancelled, ProviderError::Cancelled),
    ] {
        let (service, script, machine) =
            scripted_operation_service(vec![JoinReply::Settlement(wire::OperationSettlement {
                operation: Some(operation_state(operation, phase)),
                settlement_commit_id: vec![7; 32],
                outcome: None,
            })]);
        let key = service.expected_key;
        let (machines, shutdown, server) = serve_operation_service(service).await?;
        assert_eq!(machines.suspend(machine, key).await, Err(expected));
        assert_eq!(script.lock().expect("script").calls, ["suspend", "join"]);
        let _ = shutdown.send(());
        server.await??;
    }
    Ok(())
}

#[tokio::test]
async fn join_rejects_substituted_operation_outcome_and_commit_shape()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000042")?;
    let machine = MachineId::parse("00000000-0000-0000-0000-000000000043")?;
    let mut changed_operation = settled_suspend(operation, machine);
    changed_operation.operation = Some(operation_state(
        OperationId::new(),
        wire::OperationStatus::Succeeded,
    ));
    let changed_machine = settled_suspend(operation, MachineId::new());
    let mut changed_kind = settled_suspend(operation, machine);
    changed_kind.outcome = Some(wire::MutationOutcome {
        result: Some(wire::mutation_outcome::Result::Woken(encode_machine(
            machine,
        ))),
    });
    let mut missing_commit = settled_suspend(operation, machine);
    missing_commit.settlement_commit_id.clear();
    let mut zero_commit = settled_suspend(operation, machine);
    zero_commit.settlement_commit_id = vec![0; 32];
    let mut missing_outcome = settled_suspend(operation, machine);
    missing_outcome.outcome = None;
    let mut pending = settled_suspend(operation, machine);
    pending.operation = Some(operation_state(operation, wire::OperationStatus::Pending));
    let mut contradictory = settled_suspend(operation, machine);
    contradictory.operation = Some(operation_state(operation, wire::OperationStatus::Cancelled));
    for reply in [
        changed_operation,
        changed_machine,
        changed_kind,
        missing_commit,
        zero_commit,
        missing_outcome,
        pending,
        contradictory,
    ] {
        let (service, script, _) = scripted_operation_service(vec![JoinReply::Settlement(reply)]);
        let key = service.expected_key;
        let (machines, shutdown, server) = serve_operation_service(service).await?;
        assert!(matches!(
            machines.suspend(machine, key).await,
            Err(ProviderError::Rejected(_))
        ));
        assert_eq!(script.lock().expect("script").calls, ["suspend", "join"]);
        let _ = shutdown.send(());
        server.await??;
    }
    Ok(())
}

#[tokio::test]
async fn grpc_join_returns_original_settlement_after_resource_change_and_journal_reopen()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::core::{Admission, OperationRegistry};
    use acyclic_stream::{LocalStream, LocalStreamLimits};
    let dir = tempfile::tempdir()?;
    let key = IdempotencyKey::new();
    let request = CreateMachine::new(key, Image::custom([7; 32])?, [8; 32]);
    let outcome = SimulatedMachines::default().create(request.clone()).await?;
    let MutationOutcome::Created(original) = &outcome else {
        return Err("expected machine".into());
    };
    let journal = OperationRegistry::new(
        Arc::new(LocalStream::open(dir.path(), LocalStreamLimits::default()).await?),
        "grpc-account",
    )?;
    let Admission::Fresh(operation) = journal.admit(key, &MutationIntent::Create(request)).await?
    else {
        return Err("expected fresh admission".into());
    };
    journal
        .observe_machine(original.clone(), original.created_at_unix_ms)
        .await?;
    journal.complete(operation, outcome.clone()).await?;
    let original_settlement = journal.settled(key).await?;
    journal
        .mark_destroyed(original.id, original.changed_at_unix_ms + 1)
        .await?;
    drop(journal);
    let journal = OperationRegistry::new(
        Arc::new(LocalStream::open(dir.path(), LocalStreamLimits::default()).await?),
        "grpc-account",
    )?;
    let wire::mutation_outcome::Result::Created(encoded) =
        encode_outcome(&outcome)?.result.ok_or("missing outcome")?
    else {
        return Err("expected encoded create".into());
    };
    let (mut service, _, _) = scripted_operation_service(vec![]);
    service.expected_key = key;
    service.expected_operation = operation;
    service.recovered = wire::RecoveredAdmission {
        operation: operation_request(operation).operation.clone(),
        result: Some(wire::recovered_admission::Result::Create(
            wire::MachineAdmission {
                machine: Some(encode_machine(original.id)),
                operation: operation_request(operation).operation,
                contract: encoded.contract,
            },
        )),
    };
    service.join = JoinReply::Core(journal);
    // All inspect-machine/checkpoint RPCs are unimplemented: a reconstruction fails.
    let (machines, shutdown, server) = serve_operation_service(service).await?;
    assert_eq!(machines.recover(key).await?, outcome);
    assert_eq!(machines.join(key, operation).await?, original_settlement);
    let _ = shutdown.send(());
    server.await??;
    Ok(())
}

#[tokio::test]
async fn join_requires_one_complete_settlement_stream()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000042")?;
    let machine = MachineId::parse("00000000-0000-0000-0000-000000000043")?;
    for values in [vec![], vec![settled_suspend(operation, machine); 2]] {
        let empty = values.is_empty();
        let (service, script, _) = scripted_operation_service(vec![JoinReply::Stream(
            values.into_iter().map(Ok).collect(),
        )]);
        let key = service.expected_key;
        let (machines, shutdown, server) = serve_operation_service(service).await?;
        let result = machines.suspend(machine, key).await;
        if empty {
            assert_eq!(result, Err(ProviderError::Indeterminate(key)));
        } else {
            assert!(matches!(result, Err(ProviderError::Rejected(_))));
        }
        assert_eq!(script.lock().expect("script").calls, ["suspend", "join"]);
        let _ = shutdown.send(());
        server.await??;
    }
    Ok(())
}
