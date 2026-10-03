//! Native durable prerequisites use the production owner/task/scheduler path.
#![cfg(feature = "filesystem-local")]
#![allow(clippy::too_many_lines)]

use acyclic_fs::{LocalFs, LocalOptions};
use acyclic_harness::{
    Admission, AgentId, Capabilities, Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
    conversation::{Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    distributed::{DistributedCoordinator, SchedulerPayloadStore, WorkLease, Worker},
    durable_host::CoordinatorTaskHost,
    filesystem::{FilesystemContentVerifier, FilesystemHost, FilesystemSchedulerPayloadStore},
    resources::ProviderRef,
    runtime::{
        DurableTaskHost, RuntimeScope, TaskAdmissionRecord, TaskDefinition, TaskRegistry,
        TaskRunLimits,
    },
    scheduler::{
        DurableOwner, EntrypointRef, LeaseFence, OperationPhase, OperationSpec, Orchestration,
        ResourceRequest, ResourceSnapshot, SchedulerEvent,
    },
    workflow::{
        MachineIdentity, MachineRegistry, MachineStatus, MachineTransition, ResumableMachine,
    },
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient, SystemUnixMillisClock};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};
use uuid::Uuid;

struct Machine {
    identity: MachineIdentity,
    schema: Value,
}
impl ResumableMachine for Machine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        Ok(input.clone())
    }
    fn transition(&self, state: &Value, _: &Value) -> Result<MachineTransition> {
        Ok(MachineTransition {
            state: state.clone(),
            commands: Vec::new(),
            status: MachineStatus::Suspended,
        })
    }
}

fn id(value: u8) -> OperationId {
    OperationId::from_bytes([value; 16])
}
fn owner() -> Authority {
    Authority {
        kind: AggregateKind::Task,
        id: "native-deferred-owner".into(),
    }
}
fn issuer() -> AuthorityIssuer {
    AuthorityIssuer::new("native-deferred", [7; 32], owner())
}
fn volume() -> Result<VolumeRef> {
    VolumeRef::new(
        ProviderRef::new("native-deferred", "filesystem", "2")?,
        "private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([8; 16])),
    )
}
fn scope() -> Result<acyclic_harness::core::Scope> {
    let volume = volume()?;
    Ok(issuer().root_for_agent(
        AgentId::from_bytes([8; 16]),
        "owner",
        Capabilities::new([
            "operation:declare".to_owned(),
            "operation:observe".to_owned(),
            "operation:cancel".to_owned(),
            "task:spawn:test.deferred@1".to_owned(),
            volume.capability(VolumeOperation::Read)?,
            volume.capability(VolumeOperation::Write)?,
        ]),
    ))
}
fn machine() -> Arc<Machine> {
    Arc::new(Machine {
        identity: MachineIdentity {
            name: "test.deferred".into(),
            version: "1".into(),
            digest: [9; 32],
        },
        schema: json!({"type":"integer"}),
    })
}
fn admission(operation: OperationId, parent: Option<TaskId>) -> Result<TaskAdmissionRecord> {
    TaskAdmissionRecord::from_parts(
        operation,
        "test.deferred",
        "1",
        json!(7),
        json!({"type":"integer"}),
        json!({"type":"integer"}),
        &BTreeSet::new(),
        &[9; 32],
        parent,
        scope()?.capabilities().clone(),
        Limits::default(),
        TaskRunLimits::default(),
        None,
        None,
        None,
    )
}

async fn open(
    root: &Path,
) -> Result<(
    CoordinatorTaskHost<LocalStream>,
    DistributedCoordinator<LocalStream>,
    Arc<dyn SchedulerPayloadStore>,
)> {
    let stream = StreamClient::new(Arc::new(
        LocalStream::open(root.join("stream"), LocalStreamLimits::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    ));
    let fs = LocalFs::local(LocalOptions::new(root.join("filesystem")))
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let host = Arc::new(FilesystemHost::new(fs, volume()?.provider().clone())?);
    host.create_volume(&volume()?).await?;
    let scope = scope()?;
    let payloads = Arc::new(FilesystemSchedulerPayloadStore::new(
        host.clone(),
        volume()?,
        &issuer().verifier(),
        &scope,
        Limits::default().file_bytes,
    )?);
    let reader = Arc::new(FilesystemContentVerifier::new(
        host,
        issuer().verifier(),
        scope.clone(),
        Limits::default().file_bytes,
    )?);
    let mut tasks = TaskRegistry::default();
    tasks.register(TaskDefinition::<u32, u32>::resumable(
        machine(),
        json!({"type":"integer"}),
        json!({"type":"integer"}),
    )?)?;
    let mut machines = MachineRegistry::default();
    machines.register(machine())?;
    let coordinator = DistributedCoordinator::open(&stream, reader.clone()).await?;
    let worker_coordinator = DistributedCoordinator::open(&stream, reader.clone()).await?;
    let task_host = CoordinatorTaskHost::new(
        coordinator,
        stream,
        payloads.clone(),
        reader,
        owner(),
        scope.clone(),
        issuer().verifier(),
        RuntimeScope::new(scope.capabilities().clone(), Limits::default())?,
        tasks,
        machines,
        Arc::new(SystemUnixMillisClock),
    )?;
    Ok((task_host, worker_coordinator, payloads))
}

fn worker() -> Worker {
    Worker {
        id: "native-worker".into(),
        available: ResourceSnapshot::default(),
        labels: BTreeMap::new(),
    }
}

async fn start(
    coordinator: &mut DistributedCoordinator<LocalStream>,
    lease: &WorkLease,
) -> Result<()> {
    let operation_id = lease.operation.operation_id;
    coordinator
        .apply(
            operation_id,
            IdempotencyKey::new(format!("start-{operation_id}"))?,
            SchedulerEvent::Started {
                operation_id,
                fence: LeaseFence::from(&lease.reservation),
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn native_child_cannot_dispatch_before_its_pinned_dependency_survives_reopen() -> Result<()> {
    let root = std::env::temp_dir().join(format!("harness-deferred-{}", Uuid::new_v4()));
    let child = admission(id(3), Some(TaskId::from_bytes(id(1).into_bytes())))?
        .with_dependencies(BTreeSet::from([id(2)]))?;
    let barrier_lease = {
        let (host, mut coordinator, _) = open(&root).await?;
        assert!(host.supports_admission_dependencies());
        for record in [
            admission(id(1), None)?,
            admission(id(2), None)?,
            child.clone(),
        ] {
            assert!(matches!(host.admit(record).await?, Admission::Accepted(_)));
        }
        let root_lease = coordinator
            .pull(&worker())
            .await?
            .ok_or_else(|| Error::NotFound("root lease".into()))?;
        assert_eq!(root_lease.operation.operation_id, id(1));
        start(&mut coordinator, &root_lease).await?;
        coordinator
            .apply(
                id(1),
                IdempotencyKey::new("root-waiting")?,
                SchedulerEvent::WaitingForChildren {
                    operation_id: id(1),
                    fence: LeaseFence::from(&root_lease.reservation),
                },
            )
            .await?;
        let barrier = coordinator
            .pull(&worker())
            .await?
            .ok_or_else(|| Error::NotFound("barrier lease".into()))?;
        assert_eq!(barrier.operation.operation_id, id(2));
        start(&mut coordinator, &barrier).await?;
        assert!(coordinator.pull(&worker()).await?.is_none());
        assert_eq!(
            coordinator
                .scheduler()
                .operation(id(3))
                .ok_or_else(|| Error::Storage("child admission disappeared".into()))?
                .phase,
            OperationPhase::WaitingForDependencies
        );
        barrier
    };
    {
        let (host, mut coordinator, payloads) = open(&root).await?;
        let retained = host
            .observe_admission(TaskId::from_bytes(id(3).into_bytes()))
            .await?;
        assert!(retained == child);
        assert!(coordinator.pull(&worker()).await?.is_none());
        assert!(matches!(
            host.admit(child.clone()).await?,
            Admission::Accepted(_)
        ));
        let changed = child.clone().with_dependencies(BTreeSet::from([id(1)]))?;
        assert!(matches!(host.admit(changed).await, Err(Error::Conflict(_))));
        assert!(coordinator.pull(&worker()).await?.is_none());
        let result = payloads.stage(id(2), "publication-complete", b"7").await?;
        coordinator
            .apply(
                id(2),
                IdempotencyKey::new("publication-complete")?,
                SchedulerEvent::Completed {
                    operation_id: id(2),
                    outcome: Outcome::Succeeded(result),
                    fence: Some(LeaseFence::from(&barrier_lease.reservation)),
                    execution_duration_ns: None,
                },
            )
            .await?;
        let lease = coordinator
            .pull(&worker())
            .await?
            .ok_or_else(|| Error::NotFound("child lease".into()))?;
        assert_eq!(lease.operation.operation_id, id(3));
        assert_eq!(lease.operation.dependencies, child.dependencies);
        assert!(coordinator.pull(&worker()).await?.is_none());
    }
    std::fs::remove_dir_all(root).map_err(|error| Error::Storage(error.to_string()))?;
    Ok(())
}

#[tokio::test]
async fn native_admission_refuses_missing_foreign_and_fails_cancelled_prerequisites() -> Result<()>
{
    let root = std::env::temp_dir().join(format!("harness-deferred-refusal-{}", Uuid::new_v4()));
    {
        let (host, mut coordinator, payloads) = open(&root).await?;
        let missing = admission(id(4), None)?.with_dependencies(BTreeSet::from([id(99)]))?;
        assert!(matches!(host.admit(missing).await, Err(Error::NotFound(_))));
        let foreign = Authority {
            kind: AggregateKind::Task,
            id: "foreign".into(),
        };
        let foreign_issuer = AuthorityIssuer::new("foreign", [5; 32], foreign.clone());
        let foreign_scope =
            foreign_issuer.root("foreign", Capabilities::new(["operation:declare"]));
        let state = payloads.stage(id(5), "foreign", b"7").await?;
        coordinator
            .declare_operation(
                &foreign,
                &foreign_scope,
                &foreign_issuer.verifier(),
                OperationSpec {
                    operation_id: id(5),
                    parent: None,
                    owner: DurableOwner::Attached {
                        authority: foreign.clone(),
                    },
                    entrypoint: EntrypointRef {
                        name: "test.deferred".into(),
                        version: "1".into(),
                        digest: [9; 32],
                        result_schema: json!({"type":"integer"}),
                    },
                    dependencies: BTreeSet::new(),
                    resources: ResourceRequest::default(),
                    placement: BTreeMap::new(),
                    orchestration: Orchestration::Leaf,
                    state,
                },
                IdempotencyKey::new("declare-foreign")?,
            )
            .await?;
        let invalid = admission(id(6), None)?.with_dependencies(BTreeSet::from([id(5)]))?;
        assert!(matches!(host.admit(invalid).await, Err(Error::NotFound(_))));
        assert!(matches!(
            host.admit(admission(id(7), None)?).await?,
            Admission::Accepted(_)
        ));
        let blocked = admission(id(8), None)?.with_dependencies(BTreeSet::from([id(7)]))?;
        assert!(matches!(host.admit(blocked).await?, Admission::Accepted(_)));
        host.cancel(TaskId::from_bytes(id(7).into_bytes())).await?;
        // Pull records the explicit failed-dependency outcome before leasing other work.
        let _ = coordinator.pull(&worker()).await?;
        assert!(matches!(
            host.outcome(TaskId::from_bytes(id(8).into_bytes())).await?,
            Some(Outcome::Failed { .. })
        ));
        coordinator.refresh().await?;
        assert!(coordinator.scheduler().operation(id(4)).is_none());
        assert!(coordinator.scheduler().operation(id(6)).is_none());
    }
    std::fs::remove_dir_all(root).map_err(|error| Error::Storage(error.to_string()))?;
    Ok(())
}
