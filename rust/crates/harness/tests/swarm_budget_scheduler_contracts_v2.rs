#![cfg(feature = "filesystem-local")]

use acyclic_harness::{
    Capabilities, Error, IdempotencyKey, OperationId, Result,
    conversation::{
        ContentResidencyVerifier, FileDescriptor, FileRef, Limits, VolumeClass, VolumeOwner,
        VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer, Scope},
    distributed::{CoordinatorApply, DistributedCoordinator, Worker},
    resources::ProviderRef,
    runtime::{TaskAdmissionRecord, TaskRunLimits},
    scheduler::{
        DurableOwner, EntrypointRef, LeaseFence, OperationSpec, Orchestration, ParentLink,
        Reservation, ResourceRequest, ResourceSnapshot, SchedulerEvent, canonical_swarm_resources,
    },
    swarm_budget::{SwarmBudgetLimits, SwarmForkRequest, SwarmOwnerFence, SwarmResourceRequest},
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};
use tempfile::tempdir;

fn operation(byte: u8) -> OperationId {
    OperationId::from_bytes([byte; 16])
}

fn authority() -> Authority {
    Authority {
        kind: AggregateKind::Task,
        id: "budget-owner".into(),
    }
}

fn admission(
    operation_id: OperationId,
    parent: Option<OperationId>,
) -> Result<TaskAdmissionRecord> {
    TaskAdmissionRecord::from_parts(
        operation_id,
        "budget.task",
        "1",
        json!(7),
        json!({"type":"integer"}),
        json!({"type":"string"}),
        &BTreeSet::new(),
        &[9; 32],
        parent.map(|value| acyclic_harness::TaskId::from_bytes(value.into_bytes())),
        Capabilities::new([] as [&str; 0]),
        Limits::default(),
        TaskRunLimits::default(),
        None,
        None,
        None,
    )
}

fn state_ref(admission: &TaskAdmissionRecord) -> Result<(FileRef, Vec<u8>)> {
    let bytes = serde_json::to_vec(&admission.canonical_value())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let volume = VolumeRef::new(
        ProviderRef::new("budget-test", "filesystem", "2")?,
        "project",
        VolumeClass::Project,
        VolumeOwner::Project("budget-test".into()),
    )?;
    FileRef::new(
        volume,
        format!("state/admission-{}.json", admission.operation_id),
        "generation-1",
        FileDescriptor::from_bytes(&bytes, "application/json")?,
        "admission.json",
    )
    .map(|reference| (reference, bytes))
}

fn spec(
    admission: &TaskAdmissionRecord,
    state: FileRef,
    parent: Option<ParentLink>,
    resources: Option<SwarmResourceRequest>,
) -> OperationSpec {
    OperationSpec {
        operation_id: admission.operation_id,
        parent,
        owner: DurableOwner::Detached {
            authority: authority(),
        },
        entrypoint: EntrypointRef {
            name: admission.task.name.clone(),
            version: admission.task.version.clone(),
            digest: admission.task.digest,
            result_schema: admission.output_schema.clone(),
        },
        dependencies: admission.dependencies.clone(),
        resources: resources.map_or_else(ResourceRequest::default, canonical_swarm_resources),
        placement: BTreeMap::new(),
        orchestration: Orchestration::Leaf,
        state,
    }
}

struct ContentVerifier {
    contents: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
}

impl ContentResidencyVerifier for ContentVerifier {
    fn verify<'a>(
        &'a self,
        reference: &'a FileRef,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let contents = self
                .contents
                .lock()
                .map_err(|_| Error::Storage("content lock poisoned".into()))?;
            let bytes = contents
                .get(reference.path())
                .ok_or_else(|| Error::NotFound(reference.path().into()))?;
            reference.descriptor().verify(bytes)
        })
    }

    fn read<'a>(
        &'a self,
        reference: &'a FileRef,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>>> + Send + 'a>> {
        Box::pin(async move {
            let contents = self
                .contents
                .lock()
                .map_err(|_| Error::Storage("content lock poisoned".into()))?;
            let bytes = contents
                .get(reference.path())
                .ok_or_else(|| Error::NotFound(reference.path().into()))?;
            reference.descriptor().verify(bytes)?;
            Ok(bytes.clone())
        })
    }
}

struct Fixture {
    _root: tempfile::TempDir,
    coordinator: DistributedCoordinator<LocalStream>,
    contents: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    owner: Authority,
    issuer: AuthorityIssuer,
    scope: Scope,
    session_id: OperationId,
}

impl Fixture {
    async fn new(session_id: OperationId) -> Result<Self> {
        let root = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let stream = LocalStream::open(root.path().join("stream"), LocalStreamLimits::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let contents = Arc::new(Mutex::new(BTreeMap::new()));
        let client = StreamClient::new(Arc::new(stream));
        let coordinator = DistributedCoordinator::open(
            &client,
            Arc::new(ContentVerifier {
                contents: contents.clone(),
            }),
        )
        .await?;
        let owner = authority();
        let issuer = AuthorityIssuer::new("budget-test", [9; 32], owner.clone());
        let scope = issuer.root(
            "swarm",
            Capabilities::new(["operation:declare", "operation:admit"]),
        );
        Ok(Self {
            _root: root,
            coordinator,
            contents,
            owner,
            issuer,
            scope,
            session_id,
        })
    }

    async fn declare(
        &mut self,
        admission: &TaskAdmissionRecord,
        parent: Option<ParentLink>,
        resources: Option<SwarmResourceRequest>,
        key: &str,
    ) -> Result<FileRef> {
        let (state, bytes) = state_ref(admission)?;
        self.contents
            .lock()
            .map_err(|_| Error::Storage("content lock poisoned".into()))?
            .insert(state.path().to_owned(), bytes);
        self.coordinator
            .declare_operation(
                &self.owner,
                &self.scope,
                &self.issuer.verifier(),
                spec(admission, state.clone(), parent, resources),
                IdempotencyKey::new(key)?,
            )
            .await?;
        Ok(state)
    }

    async fn start_session(&mut self) -> Result<()> {
        let lease = self
            .coordinator
            .pull(&Worker {
                id: "budget-worker".into(),
                available: ResourceSnapshot(BTreeMap::from([
                    ("model_steps".into(), 64),
                    ("output_bytes".into(), 640),
                    ("execution_time_ms".into(), 6_400),
                ])),
                labels: BTreeMap::new(),
            })
            .await?
            .ok_or_else(|| Error::NotFound("session lease".into()))?;
        if lease.operation.operation_id != self.session_id {
            return Err(Error::Conflict(
                "session was not first scheduler lease".into(),
            ));
        }
        self.coordinator
            .apply(
                self.session_id,
                IdempotencyKey::new("start-session")?,
                SchedulerEvent::Started {
                    operation_id: self.session_id,
                    fence: LeaseFence::from(&lease.reservation),
                },
            )
            .await?;
        Ok(())
    }

    async fn admit(
        &mut self,
        admission: &TaskAdmissionRecord,
        state: FileRef,
        parent_operation_id: Option<OperationId>,
        depth: u32,
        limits: SwarmBudgetLimits,
        resources: SwarmResourceRequest,
        key: &str,
        lease_id: &str,
    ) -> Result<CoordinatorApply> {
        let request = SwarmForkRequest::from_task_admission(
            admission,
            IdempotencyKey::new(key)?,
            parent_operation_id,
            depth,
            resources,
        )?;
        self.coordinator
            .admit_swarm_child(
                &self.owner,
                &self.scope,
                &self.issuer.verifier(),
                admission.operation_id,
                IdempotencyKey::new(format!("coordinator-{key}"))?,
                self.session_id,
                limits,
                SwarmOwnerFence::new(self.owner.id.clone(), 0)?,
                request,
                state,
                Reservation {
                    id: lease_id.into(),
                    placement: "budget-worker".into(),
                    admitted: canonical_swarm_resources(resources),
                },
            )
            .await
    }
}

fn limits(max_active_agents: u64, max_total_agents: u64) -> SwarmBudgetLimits {
    SwarmBudgetLimits {
        max_active_agents,
        max_total_agents,
        max_recursion_depth: 2,
        max_model_steps: 32,
        max_output_bytes: 320,
        max_execution_time_ms: 3_200,
    }
}

#[tokio::test]
async fn production_admission_enforces_distinct_active_and_total_limits() -> Result<()> {
    let session_id = operation(1);
    let mut fixture = Fixture::new(session_id).await?;
    let session = admission(session_id, None)?;
    fixture
        .declare(
            &session,
            None,
            None,
            "declare-session",
        )
        .await?;
    fixture.start_session().await?;

    let child_resources = SwarmResourceRequest {
        model_steps: 4,
        output_bytes: 40,
        execution_time_ms: 400,
    };
    let first = admission(operation(2), None)?;
    let first_state = fixture
        .declare(&first, None, Some(child_resources), "declare-child-1")
        .await?;
    assert!(matches!(
        fixture
            .admit(
                &first,
                first_state,
                None,
                1,
                limits(2, 2),
                child_resources,
                "fork-child-1",
                "lease-child-1",
            )
            .await?,
        CoordinatorApply::Applied
    ));

    let second = admission(operation(3), None)?;
    let second_state = fixture
        .declare(&second, None, Some(child_resources), "declare-child-2")
        .await?;
    assert!(matches!(
        fixture
            .admit(
                &second,
                second_state,
                None,
                1,
                limits(2, 2),
                child_resources,
                "fork-child-2",
                "lease-child-2",
            )
            .await,
        Err(Error::Conflict(_))
    ));
    let usage = fixture
        .coordinator
        .scheduler()
        .swarm_budget_usage()?
        .expect("session budget");
    assert_eq!(usage.active_agents, 2);
    assert_eq!(usage.total_agents, 2);
    assert_eq!(usage.reserved.model_steps, child_resources.model_steps);
    Ok(())
}

#[tokio::test]
async fn production_admission_rejects_descendant_over_each_ancestor_dimension() -> Result<()> {
    let session_id = operation(10);
    let parent_id = operation(11);
    let mut fixture = Fixture::new(session_id).await?;
    let session = admission(session_id, None)?;
    fixture
        .declare(
            &session,
            None,
            None,
            "declare-session",
        )
        .await?;
    fixture.start_session().await?;

    let parent_resources = SwarmResourceRequest {
        model_steps: 8,
        output_bytes: 80,
        execution_time_ms: 800,
    };
    let parent = admission(parent_id, None)?;
    let parent_state = fixture
        .declare(&parent, None, Some(parent_resources), "declare-parent")
        .await?;
    assert!(matches!(
        fixture
            .admit(
                &parent,
                parent_state,
                None,
                1,
                limits(8, 16),
                parent_resources,
                "fork-parent",
                "lease-parent",
            )
            .await?,
        CoordinatorApply::Applied
    ));

    for (byte, resources) in [
        (
            12,
            SwarmResourceRequest {
                model_steps: 9,
                output_bytes: 80,
                execution_time_ms: 800,
            },
        ),
        (
            13,
            SwarmResourceRequest {
                model_steps: 8,
                output_bytes: 81,
                execution_time_ms: 800,
            },
        ),
        (
            14,
            SwarmResourceRequest {
                model_steps: 8,
                output_bytes: 80,
                execution_time_ms: 801,
            },
        ),
    ] {
        let child = admission(operation(byte), Some(parent_id))?;
        let child_state = fixture
            .declare(
                &child,
                Some(ParentLink {
                    operation_id: parent_id,
                    slot: format!("child-{byte}"),
                }),
                Some(resources),
                &format!("declare-child-{byte}"),
            )
            .await?;
        assert!(matches!(
            fixture
                .admit(
                    &child,
                    child_state,
                    Some(parent_id),
                    2,
                    limits(8, 16),
                    resources,
                    &format!("fork-child-{byte}"),
                    &format!("lease-child-{byte}"),
                )
                .await,
            Err(Error::Conflict(_))
        ));
    }

    let usage = fixture
        .coordinator
        .scheduler()
        .swarm_budget_usage()?
        .expect("session budget");
    assert_eq!(usage.active_agents, 2);
    assert_eq!(usage.total_agents, 2);
    assert_eq!(usage.reserved.model_steps, parent_resources.model_steps);
    assert_eq!(usage.reserved.output_bytes, parent_resources.output_bytes);
    assert_eq!(
        usage.reserved.execution_time_ms,
        parent_resources.execution_time_ms
    );
    Ok(())
}
