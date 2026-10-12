use super::*;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn native_worker_observation_rejects_a_genuine_foreign_owner() -> Result<()> {
    let fixture = created_resource_access::CreationFixture::new().await?;
    let foreign = Authority {
        kind: AggregateKind::Task,
        id: "native-foreign-owner".into(),
    };
    let issuer = AuthorityIssuer::new("native-foreign-owner", [92; 32], foreign.clone());
    let signed = issuer
        .issue(
            "foreign",
            fixture.host.owner_scope.capabilities(),
            Some(AgentId::from_bytes([92; 16])),
            None,
            &mut None,
        )
        .expect("scope proof");
    let host = CoordinatorTaskHost::new(
        DistributedCoordinator::open(
            &fixture.stream,
            fixture.host.payloads.clone(),
            None,
            &mut None,
        )
        .await?,
        fixture.stream.clone(),
        fixture.host.payloads.clone(),
        fixture.host.reader.clone(),
        &(foreign),
        signed,
        issuer.verifier(&mut None).expect("verifier copy"),
        fixture.root_scope.clone(),
        fixture.host.tasks.clone(),
        fixture.host.machines.clone(),
        fixture.host.clock.clone(),
        None,
    )?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = test_work(
        acyclic_fs::performance::work_budget(64 * 1024 * 1024, 64 * 1024 * 1024),
        &cancellation,
    )?;
    let (pending, pending_guard) = fixture
        .host
        .outcome_with_native_work(fixture.root, &work)
        .await?;
    assert!(pending.is_none());
    drop(pending);
    drop(pending_guard);
    let before = work.receipt();
    assert!(
        host.outcome_with_native_work(fixture.root, &work)
            .await
            .is_err()
    );
    assert_eq!(
        work.receipt().backend_write_operations,
        before.backend_write_operations
    );
    cancellation.cancel();
    let before = work.receipt();
    assert!(
        fixture
            .host
            .outcome_with_native_work(fixture.root, &work)
            .await
            .is_err()
    );
    assert_eq!(
        work.receipt().backend_read_operations,
        before.backend_read_operations
    );
    Ok(())
}

#[cfg(feature = "filesystem")]
#[tokio::test]
async fn wake_discovery_pages_filter_owners_and_recheck_current_cancellation() -> Result<()> {
    use crate::scheduler::{LeaseFence, Reservation, SchedulerEvent};
    let stream = Arc::new(MemoryStream::default());
    let payloads = Arc::new(MemoryPayloads::new()?);
    let owner = Authority {
        kind: AggregateKind::Task,
        id: "discovery-owner".into(),
    };
    let foreign = Authority {
        kind: AggregateKind::Task,
        id: "foreign-owner".into(),
    };
    let issuer = AuthorityIssuer::new("discovery", [7; 32], owner.clone());
    let foreign_issuer = AuthorityIssuer::new("discovery", [8; 32], foreign.clone());
    let scope = issuer
        .issue(
            "owner",
            &Capabilities::new(["operation:declare", "operation:observe", "operation:cancel"]),
            None,
            None,
            &mut None,
        )
        .expect("scope proof");
    let foreign_scope = foreign_issuer
        .issue("owner", scope.capabilities(), None, None, &mut None)
        .expect("scope proof");
    let mut coordinator =
        DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?;
    for index in 1..=18u8 {
        let operation = OperationId::from_bytes([index; 16]);
        let state = payloads.stage(operation, "state", b"null", None)?.await?;
        let fence = LeaseFence {
            reservation_id: format!("lease-{index}"),
            placement: "worker".into(),
        };
        let events = [
            SchedulerEvent::Declared {
                spec: Box::new(OperationSpec {
                    operation_id: operation,
                    owner_scope: if index % 2 == 0 {
                        foreign_scope.clone()
                    } else {
                        scope.clone()
                    },
                    parent: None,
                    owner: DurableOwner::Attached {
                        authority: if index % 2 == 0 {
                            foreign.clone()
                        } else {
                            owner.clone()
                        },
                    },
                    entrypoint: EntrypointRef {
                        name: "test.discovery".into(),
                        version: "1".into(),
                        digest: [2; 32],
                        result_schema: serde_json::json!({}),
                    },
                    dependencies: BTreeSet::new(),
                    resources: ResourceRequest::default(),
                    placement: BTreeMap::new(),
                    orchestration: Orchestration::Leaf,
                    state,
                }),
            },
            SchedulerEvent::WaitingForCapacity {
                operation_id: operation,
            },
            SchedulerEvent::Admitted {
                operation_id: operation,
                reservation: Reservation {
                    id: fence.reservation_id.clone(),
                    placement: fence.placement.clone(),
                    admitted: ResourceRequest::default(),
                },
            },
            SchedulerEvent::Started {
                operation_id: operation,
                fence: fence.clone(),
            },
            SchedulerEvent::WorkflowSuspended {
                operation_id: operation,
                fence,
                workflow_revision: 1,
                waiting_command: (index != 1).then_some(OperationId::from_bytes([99; 16])),
            },
        ];
        for (step, event) in events.into_iter().enumerate() {
            let key = IdempotencyKey::new(format!("discovery-{index}-{step}"))?;
            if let SchedulerEvent::Declared { spec } = event {
                let (authority, signed, verifier) = if index % 2 == 0 {
                    (
                        &foreign,
                        &foreign_scope,
                        foreign_issuer.verifier(&mut None).expect("verifier copy"),
                    )
                } else {
                    (
                        &owner,
                        &scope,
                        issuer.verifier(&mut None).expect("verifier copy"),
                    )
                };
                coordinator
                    .declare_operation(authority, signed, &verifier, *spec, key, None, &mut None)
                    .await?;
            } else {
                coordinator.apply(key, event, &mut None).await?;
            }
        }
    }
    let cancel = |index| SchedulerEvent::CancellationRequested {
        operation_id: OperationId::from_bytes([index; 16]),
        recursive: false,
    };
    coordinator
        .apply(
            IdempotencyKey::new("discovery-cancel-3")?,
            cancel(3),
            &mut None,
        )
        .await?;
    let host = CoordinatorTaskHost::new(
        coordinator,
        stream.clone(),
        payloads.clone(),
        payloads,
        &(owner),
        scope.clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        RuntimeScope::new(scope.capabilities().clone(), Limits::default())?,
        TaskRegistry::default(),
        MachineRegistry::default(),
        Arc::new(SystemUnixMillisClock),
        None,
    )?;
    let first = host.workflow_wait_page(0, None, 64).await?;
    assert_eq!(first.2, 64);
    assert_eq!(first.0, 64);
    assert_eq!(first.1, 91);
    assert_eq!(
        first.3,
        [5, 7, 9, 11].map(|index| TaskId::from_bytes([index; 16]))
    );
    host.coordinator
        .lock()
        .await
        .apply(
            IdempotencyKey::new("discovery-cancel-17")?,
            cancel(17),
            &mut None,
        )
        .await?;
    let second = host.workflow_wait_page(first.0, Some(first.1), 64).await?;
    assert_eq!(second.2, 27);
    assert_eq!(second.0, 91);
    assert_eq!(second.1, 91);
    assert_eq!(
        second.3,
        [13, 15].map(|index| TaskId::from_bytes([index; 16]))
    );
    let finished = host.workflow_wait_page(91, Some(91), 64).await?;
    assert_eq!(finished.2, 0);
    assert!(finished.3.is_empty());
    let retry = host.workflow_wait_page(64, Some(91), 64).await?;
    assert_eq!(retry.3, second.3);
    let new_snapshot = host.workflow_wait_page(0, None, 1).await?;
    assert_eq!(new_snapshot.1, 92);
    Ok(())
}
