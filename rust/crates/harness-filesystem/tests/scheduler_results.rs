//! Ref-only scheduler completion and aggregate publication against real providers.
#![allow(clippy::too_many_lines)]

use acyclic_fs::Fs;
use acyclic_harness::conversation::{
    ContentGrant, FileDescriptor, FileRef, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
};
use acyclic_harness::core::{AggregateKind, Authority, AuthorityIssuer};
use acyclic_harness::distributed::{
    CoordinatorApply, DistributedCoordinator, DurableReducer, ReducerRegistry,
    SchedulerPayloadStore,
};
use acyclic_harness::resources::ProviderRef;
use acyclic_harness::scheduler::{
    DurableOwner, EntrypointRef, LeaseFence, OperationSpec, Orchestration, OrchestrationDecision,
    ParentLink, Reservation, ResourceRequest, SchedulerEvent, assembly_invocation_digest,
};
use acyclic_harness::{AgentId, Capabilities, IdempotencyKey, OperationId, Outcome, Result};
use acyclic_harness_filesystem::{
    FilesystemContentVerifier, FilesystemHost, FilesystemSchedulerPayloadStore,
};
use acyclic_stream::{MemoryStream, StreamClient};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

fn id(byte: u8) -> OperationId {
    OperationId::from_bytes([byte; 16])
}

struct SumReducer(EntrypointRef);

impl DurableReducer for SumReducer {
    fn entrypoint(&self) -> &EntrypointRef {
        &self.0
    }

    fn reduce(&self, values: &[(String, serde_json::Value)]) -> Result<Outcome<serde_json::Value>> {
        let sum = values
            .iter()
            .map(|(_, value)| {
                value.as_i64().ok_or_else(|| {
                    acyclic_harness::Error::Invalid("reducer expected integer children".into())
                })
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .sum::<i64>();
        Ok(Outcome::Succeeded(serde_json::json!(sum)))
    }
}

fn spec(
    operation_id: OperationId,
    owner: &Authority,
    state: &acyclic_harness::conversation::FileRef,
    orchestration: Orchestration,
    parent: Option<ParentLink>,
    result_type: &str,
) -> OperationSpec {
    OperationSpec {
        operation_id,
        parent,
        owner: DurableOwner::Attached {
            authority: owner.clone(),
        },
        entrypoint: EntrypointRef {
            name: "test.scheduler".into(),
            version: "1".into(),
            digest: [7; 32],
            result_schema: serde_json::json!({"type": result_type}),
        },
        dependencies: BTreeSet::new(),
        resources: ResourceRequest::default(),
        placement: BTreeMap::new(),
        orchestration,
        state: state.clone(),
    }
}

async fn start(
    coordinator: &mut DistributedCoordinator<MemoryStream>,
    operation_id: OperationId,
    waiting_for_children: bool,
) -> Result<LeaseFence> {
    let reservation = Reservation {
        id: format!("lease-{operation_id}"),
        placement: "worker".into(),
        admitted: ResourceRequest::default(),
    };
    let fence = LeaseFence::from(&reservation);
    coordinator
        .apply(
            operation_id,
            IdempotencyKey::new(format!("admit-{operation_id}"))?,
            SchedulerEvent::Admitted {
                operation_id,
                reservation,
            },
        )
        .await?;
    coordinator
        .apply(
            operation_id,
            IdempotencyKey::new(format!("start-{operation_id}"))?,
            SchedulerEvent::Started {
                operation_id,
                fence: fence.clone(),
            },
        )
        .await?;
    if waiting_for_children {
        coordinator
            .apply(
                operation_id,
                IdempotencyKey::new(format!("wait-{operation_id}"))?,
                SchedulerEvent::WaitingForChildren {
                    operation_id,
                    fence: fence.clone(),
                },
            )
            .await?;
    }
    Ok(fence)
}

#[tokio::test]
async fn join_stages_ref_only_output_and_replays_verified_bytes() -> Result<()> {
    let provider = ProviderRef::new("qualification", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
    let volume = VolumeRef::new(
        provider,
        "scheduler-private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([6; 16])),
    )?;
    host.create_volume(&volume).await?;
    let owner = Authority {
        kind: AggregateKind::Task,
        id: "scheduler-owner".into(),
    };
    let issuer = AuthorityIssuer::new("qualification", [5; 32], owner.clone());
    let scope = issuer.root_for_agent(
        AgentId::from_bytes([6; 16]),
        "scheduler",
        Capabilities::new([
            "operation:declare".to_owned(),
            volume.capability(VolumeOperation::Read)?,
            volume.capability(VolumeOperation::Write)?,
        ]),
    );
    let write = ContentGrant::verify(&issuer.verifier(), &scope, &volume, VolumeOperation::Write)?;
    let read = ContentGrant::verify(&issuer.verifier(), &scope, &volume, VolumeOperation::Read)?;
    let state = host
        .put_content(
            &volume,
            &write,
            "state/initial.json",
            b"null",
            "application/json",
            "initial.json",
            1_024,
            &IdempotencyKey::new("scheduler-initial")?,
        )
        .await?;
    let verifier = Arc::new(FilesystemContentVerifier::new(
        host.clone(),
        issuer.verifier(),
        scope.clone(),
        1_024,
    )?);
    let stager = Arc::new(FilesystemSchedulerPayloadStore::new(
        host.clone(),
        volume.clone(),
        &issuer.verifier(),
        &scope,
        1_024,
    )?);
    let stream = StreamClient::new(Arc::new(MemoryStream::default()));
    let mut coordinator = DistributedCoordinator::open(&stream, verifier.clone())
        .await?
        .with_payload_store(stager.clone());
    let parent = id(1);
    coordinator
        .declare_operation(
            &owner,
            &scope,
            &issuer.verifier(),
            spec(parent, &owner, &state, Orchestration::Join, None, "array"),
            IdempotencyKey::new("declare-parent")?,
        )
        .await?;
    start(&mut coordinator, parent, true).await?;

    for (byte, slot, body) in [(2, "a", b"2".as_slice()), (3, "b", b"3".as_slice())] {
        let child = id(byte);
        coordinator
            .declare_operation(
                &owner,
                &scope,
                &issuer.verifier(),
                spec(
                    child,
                    &owner,
                    &state,
                    Orchestration::Leaf,
                    Some(ParentLink {
                        operation_id: parent,
                        slot: slot.into(),
                    }),
                    "number",
                ),
                IdempotencyKey::new(format!("declare-{slot}"))?,
            )
            .await?;
        let fence = start(&mut coordinator, child, false).await?;
        let result = host
            .put_content(
                &volume,
                &write,
                &format!("child/{slot}.json"),
                body,
                "application/json",
                "child.json",
                1_024,
                &IdempotencyKey::new(format!("child-upload-{slot}"))?,
            )
            .await?;
        if slot == "a" {
            let corrupt = FileRef::new(
                result.volume().clone(),
                result.path(),
                result.version(),
                FileDescriptor::from_bytes(b"999", "application/json")?,
                "child.json",
            )?;
            assert!(
                coordinator
                    .apply(
                        child,
                        IdempotencyKey::new("corrupt-child")?,
                        SchedulerEvent::Completed {
                            operation_id: child,
                            outcome: Outcome::Succeeded(corrupt),
                            fence: Some(fence.clone())
                        }
                    )
                    .await
                    .is_err()
            );
            assert!(
                coordinator
                    .scheduler()
                    .operation(child)
                    .is_some_and(|state| state.outcome.is_none())
            );
            let missing = FileRef::new(
                result.volume().clone(),
                "child/missing.json",
                result.version(),
                result.descriptor().clone(),
                "missing.json",
            )?;
            assert!(
                coordinator
                    .apply(
                        child,
                        IdempotencyKey::new("missing-child")?,
                        SchedulerEvent::Completed {
                            operation_id: child,
                            outcome: Outcome::Succeeded(missing),
                            fence: Some(fence.clone())
                        }
                    )
                    .await
                    .is_err()
            );
            let invalid_type = host
                .put_content(
                    &volume,
                    &write,
                    "child/invalid-type.json",
                    b"\"text\"",
                    "application/json",
                    "invalid.json",
                    1_024,
                    &IdempotencyKey::new("invalid-type-upload")?,
                )
                .await?;
            assert!(
                coordinator
                    .apply(
                        child,
                        IdempotencyKey::new("schema-child")?,
                        SchedulerEvent::Completed {
                            operation_id: child,
                            outcome: Outcome::Succeeded(invalid_type),
                            fence: Some(fence.clone())
                        }
                    )
                    .await
                    .is_err()
            );
            assert!(
                coordinator
                    .scheduler()
                    .operation(child)
                    .is_some_and(|state| state.outcome.is_none())
            );
        }
        coordinator
            .apply(
                child,
                IdempotencyKey::new(format!("finish-{slot}"))?,
                SchedulerEvent::Completed {
                    operation_id: child,
                    outcome: Outcome::Succeeded(result),
                    fence: Some(fence),
                },
            )
            .await?;
    }
    let OrchestrationDecision::Assemble {
        assembly,
        values,
        cancel,
    } = coordinator.scheduler().orchestration(parent)
    else {
        return Err(acyclic_harness::Error::Invalid("join not ready".into()));
    };
    let forged = host
        .put_content(
            &volume,
            &write,
            "result/forged-join.json",
            b"[]",
            "application/json",
            "forged-join.json",
            1_024,
            &IdempotencyKey::new("forged-join-upload")?,
        )
        .await?;
    let expected_revision = coordinator
        .scheduler()
        .operation(parent)
        .ok_or_else(|| acyclic_harness::Error::NotFound("join parent".into()))?
        .revision;
    let forged_result = coordinator
        .apply(
            parent,
            IdempotencyKey::new("forged-join")?,
            SchedulerEvent::Orchestrated {
                operation_id: parent,
                expected_revision,
                outcome: Outcome::Succeeded(forged),
                cancel,
                reducer: None,
                reduction_digest: Some(assembly_invocation_digest(assembly, &values)?),
            },
        )
        .await;
    assert!(
        matches!(forged_result, Err(acyclic_harness::Error::Unauthorized(_))),
        "{forged_result:?}"
    );
    assert!(
        coordinator
            .scheduler()
            .operation(parent)
            .is_some_and(|state| state.outcome.is_none())
    );
    assert!(
        coordinator
            .orchestrate(parent, IdempotencyKey::new("join-once")?)
            .await?
    );
    assert!(
        coordinator
            .orchestrate(parent, IdempotencyKey::new("join-once")?)
            .await?
    );
    assert!(
        !coordinator
            .orchestrate(parent, IdempotencyKey::new("join-again")?)
            .await?
    );
    let outcome = coordinator
        .scheduler()
        .operation(parent)
        .and_then(|state| state.outcome.as_ref())
        .cloned()
        .ok_or_else(|| acyclic_harness::Error::NotFound("join result".into()))?;
    let Outcome::Succeeded(result) = &outcome else {
        return Err(acyclic_harness::Error::Invalid(
            "join did not succeed".into(),
        ));
    };
    let bytes = host.read_content(result, &read, 1_024).await?;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes)
            .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?,
        serde_json::json!([{"slot":"a","value":2},{"slot":"b","value":3}])
    );
    assert!(matches!(
        stager.stage(parent, "join-once", b"different").await,
        Err(acyclic_harness::Error::Conflict(_))
    ));

    let quorum = id(4);
    coordinator
        .declare_operation(
            &owner,
            &scope,
            &issuer.verifier(),
            spec(
                quorum,
                &owner,
                &state,
                Orchestration::Quorum { required: 2 },
                None,
                "array",
            ),
            IdempotencyKey::new("declare-quorum")?,
        )
        .await?;
    start(&mut coordinator, quorum, true).await?;
    for (byte, slot) in [(5, "first"), (6, "second"), (7, "loser")] {
        let child = id(byte);
        coordinator
            .declare_operation(
                &owner,
                &scope,
                &issuer.verifier(),
                spec(
                    child,
                    &owner,
                    &state,
                    Orchestration::Leaf,
                    Some(ParentLink {
                        operation_id: quorum,
                        slot: slot.into(),
                    }),
                    "number",
                ),
                IdempotencyKey::new(format!("declare-{slot}"))?,
            )
            .await?;
        let fence = start(&mut coordinator, child, false).await?;
        if slot == "loser" {
            continue;
        }
        let body = byte.to_string();
        let result = host
            .put_content(
                &volume,
                &write,
                &format!("child/{slot}.json"),
                body.as_bytes(),
                "application/json",
                "child.json",
                1_024,
                &IdempotencyKey::new(format!("child-upload-{slot}"))?,
            )
            .await?;
        coordinator
            .apply(
                child,
                IdempotencyKey::new(format!("finish-{slot}"))?,
                SchedulerEvent::Completed {
                    operation_id: child,
                    outcome: Outcome::Succeeded(result),
                    fence: Some(fence),
                },
            )
            .await?;
    }
    assert!(
        coordinator
            .orchestrate(quorum, IdempotencyKey::new("quorum-once")?)
            .await?
    );
    assert!(
        coordinator
            .orchestrate(quorum, IdempotencyKey::new("quorum-once")?)
            .await?
    );
    let Some(Outcome::Succeeded(quorum_result)) = coordinator
        .scheduler()
        .operation(quorum)
        .and_then(|state| state.outcome.as_ref())
    else {
        return Err(acyclic_harness::Error::Invalid(
            "quorum did not succeed".into(),
        ));
    };
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &host.read_content(quorum_result, &read, 1_024).await?
        )
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?,
        serde_json::json!([5, 6])
    );
    assert!(
        coordinator
            .scheduler()
            .operation(id(7))
            .is_some_and(|state| state.cancellation_requested)
    );

    let reducer = EntrypointRef {
        name: "test.sum".into(),
        version: "1".into(),
        digest: [9; 32],
        result_schema: serde_json::json!({"type":"number"}),
    };
    let mut reducers = ReducerRegistry::new();
    reducers.register(Arc::new(SumReducer(reducer.clone())))?;
    let reduction = id(8);
    coordinator
        .declare_operation(
            &owner,
            &scope,
            &issuer.verifier(),
            spec(
                reduction,
                &owner,
                &state,
                Orchestration::Reduce { reducer },
                None,
                "number",
            ),
            IdempotencyKey::new("declare-reduction")?,
        )
        .await?;
    start(&mut coordinator, reduction, true).await?;
    let child = id(9);
    coordinator
        .declare_operation(
            &owner,
            &scope,
            &issuer.verifier(),
            spec(
                child,
                &owner,
                &state,
                Orchestration::Leaf,
                Some(ParentLink {
                    operation_id: reduction,
                    slot: "value".into(),
                }),
                "number",
            ),
            IdempotencyKey::new("declare-reduce-child")?,
        )
        .await?;
    let fence = start(&mut coordinator, child, false).await?;
    let result = host
        .put_content(
            &volume,
            &write,
            "child/reduce.json",
            b"11",
            "application/json",
            "child.json",
            1_024,
            &IdempotencyKey::new("reduce-child-upload")?,
        )
        .await?;
    coordinator
        .apply(
            child,
            IdempotencyKey::new("finish-reduce-child")?,
            SchedulerEvent::Completed {
                operation_id: child,
                outcome: Outcome::Succeeded(result),
                fence: Some(fence),
            },
        )
        .await?;
    coordinator
        .complete_reduction(&reducers, reduction, IdempotencyKey::new("reduce-once")?)
        .await?;
    assert_eq!(
        coordinator
            .complete_reduction(&reducers, reduction, IdempotencyKey::new("reduce-once")?)
            .await?,
        CoordinatorApply::Replayed
    );
    assert!(matches!(
        coordinator
            .complete_reduction(&reducers, reduction, IdempotencyKey::new("reduce-again")?)
            .await,
        Err(acyclic_harness::Error::Conflict(_))
    ));
    let Some(Outcome::Succeeded(reduced)) = coordinator
        .scheduler()
        .operation(reduction)
        .and_then(|state| state.outcome.as_ref())
    else {
        return Err(acyclic_harness::Error::Invalid(
            "reducer did not succeed".into(),
        ));
    };
    assert_eq!(
        host.read_content(reduced, &read, 1_024).await?.as_ref(),
        b"11"
    );

    let mut reopened = DistributedCoordinator::open(&stream, verifier)
        .await?
        .with_payload_store(stager);
    assert!(
        reopened
            .orchestrate(parent, IdempotencyKey::new("join-once")?)
            .await?
    );
    assert!(
        reopened
            .orchestrate(quorum, IdempotencyKey::new("quorum-once")?)
            .await?
    );
    assert_eq!(
        reopened
            .complete_reduction(&reducers, reduction, IdempotencyKey::new("reduce-once")?)
            .await?,
        CoordinatorApply::Replayed
    );
    assert_eq!(
        reopened
            .scheduler()
            .operation(parent)
            .and_then(|state| state.outcome.as_ref()),
        Some(&outcome)
    );
    assert_eq!(
        reopened
            .scheduler()
            .operation(quorum)
            .and_then(|state| state.outcome.as_ref()),
        coordinator
            .scheduler()
            .operation(quorum)
            .and_then(|state| state.outcome.as_ref())
    );
    assert_eq!(
        reopened
            .scheduler()
            .operation(reduction)
            .and_then(|state| state.outcome.as_ref()),
        coordinator
            .scheduler()
            .operation(reduction)
            .and_then(|state| state.outcome.as_ref())
    );
    Ok(())
}
