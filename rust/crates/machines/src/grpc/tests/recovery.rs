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
async fn snapshot_recovery_uses_retained_results_without_resource_reads()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use wire::{mutation_outcome::Result as Outcome, recovered_admission::Result as Admission};
    let checkpoint = CheckpointId::parse("00000000-0000-0000-0000-000000000051")?;
    let child = MachineId::parse("00000000-0000-0000-0000-000000000052")?;
    for kind in 0..3 {
        let (mut service, script, source) = scripted_operation_service(Vec::new(), Vec::new());
        let key = service.expected_key;
        let operation = service.recovered.operation.clone();
        let contract = codecs::wire_contract(&[wire::Capability::LiveFork]);
        let machine = wire::MachineState {
            machine: Some(encode_machine(child)),
            status: wire::MachineStatus::Running as i32,
            contract: Some(contract.clone()),
            endpoints: Vec::new(),
            last_checkpoint: None,
            created_at_unix_ms: 7,
            changed_at_unix_ms: 7,
        };
        let (admission, result, expected) = match kind {
            0 => {
                let state = wire::CheckpointState {
                    checkpoint: Some(encode_checkpoint(checkpoint)),
                    source: Some(encode_machine(source)),
                    contract: Some(contract.clone()),
                    forkable: true,
                    created_at_unix_ms: 7,
                };
                (
                    Admission::Checkpoint(wire::CheckpointAdmission {
                        checkpoint: state.checkpoint.clone(),
                        source: state.source.clone(),
                        operation,
                        contract: Some(contract.clone()),
                    }),
                    Outcome::Checkpointed(state),
                    MutationOutcome::Checkpointed(CheckpointObservation {
                        id: checkpoint,
                        source,
                        contract: decode_contract(Some(&contract))?,
                        forkable: true,
                        created_at_unix_ms: 7,
                    }),
                )
            }
            1 => (
                Admission::Fork(wire::ForkAdmission {
                    checkpoint: Some(encode_checkpoint(checkpoint)),
                    children: vec![encode_machine(child)],
                    operation,
                    contract: Some(contract.clone()),
                }),
                Outcome::Forked(wire::ForkedMachines {
                    machines: vec![machine.clone()],
                }),
                MutationOutcome::Forked(vec![decode_machine_observation(machine.clone(), child)?]),
            ),
            _ => (
                Admission::ForkMachine(wire::ForkMachineAdmission {
                    source: Some(encode_machine(source)),
                    children: vec![encode_machine(child)],
                    operation,
                    contract: Some(contract.clone()),
                    fidelity: wire::ForkFidelity::MemoryAndDisk as i32,
                }),
                Outcome::MachineForked(wire::ForkedLiveMachines {
                    source: Some(encode_machine(source)),
                    fidelity: wire::ForkFidelity::MemoryAndDisk as i32,
                    children: vec![machine.clone()],
                }),
                MutationOutcome::MachineForked {
                    source,
                    fidelity: ForkFidelity::MemoryAndDisk,
                    children: vec![decode_machine_observation(machine.clone(), child)?],
                },
            ),
        };
        service.recovered.result = Some(admission.clone());
        let mut terminal =
            operation_state(service.expected_operation, wire::OperationStatus::Succeeded);
        terminal.outcome = Some(wire::MutationOutcome {
            result: Some(result.clone()),
        });
        let mut replies = vec![
            Some(result.clone()),
            None,
            Some(Outcome::Created(machine.clone())),
        ];
        for altered in 0..3 {
            let mut result = result.clone();
            match &mut result {
                Outcome::Checkpointed(state) => match altered {
                    0 => state.source = Some(encode_machine(child)),
                    1 => {
                        state
                            .contract
                            .as_mut()
                            .expect("contract")
                            .network_policy_digest = vec![2; 32]
                    }
                    _ => state.checkpoint = None,
                },
                Outcome::Forked(states) => match altered {
                    0 => states.machines[0].machine = Some(encode_machine(source)),
                    1 => {
                        states.machines[0]
                            .contract
                            .as_mut()
                            .expect("contract")
                            .network_policy_digest = vec![2; 32]
                    }
                    _ => states.machines.clear(),
                },
                Outcome::MachineForked(states) => match altered {
                    0 => states.source = Some(encode_machine(child)),
                    1 => states.fidelity = wire::ForkFidelity::DiskOnly as i32,
                    _ => states.children.clear(),
                },
                _ => unreachable!(),
            }
            replies.push(Some(result));
        }
        // Internal wire vectors. The server deliberately has no resource read
        // implementation, as resources may be changed or deleted after success.
        for reply in &replies {
            let mut state = terminal.clone();
            state.outcome = reply.clone().map(|result| wire::MutationOutcome {
                result: Some(result),
            });
            script
                .lock()
                .expect("script")
                .watches
                .push_back(WatchReply::Items(vec![WatchItem::State(state)]));
        }
        let (provider, shutdown, server) = serve_operation_service(service).await?;
        assert_eq!(provider.recover(key).await?, expected);
        for _ in 1..replies.len() {
            assert_eq!(
                provider.recover(key).await,
                Err(ProviderError::Indeterminate(key))
            );
        }
        assert_eq!(
            script.lock().expect("script").calls,
            vec!["watch"; replies.len()]
        );
        let _ = shutdown.send(());
        server.await??;
        if let Admission::Fork(mut value) = admission {
            for children in [
                Vec::new(),
                vec![encode_machine(child), encode_machine(child)],
            ] {
                value.children = children;
                assert!(settled_fork(value.clone(), terminal.outcome.clone()).is_err());
            }
        }
    }
    Ok(())
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
