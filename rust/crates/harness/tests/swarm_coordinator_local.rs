#![cfg(feature = "filesystem-local")]

use acyclic_harness::{
    Capabilities, IdempotencyKey, OperationId, Result,
    conversation::{FileDescriptor, FileRef, Limits, VolumeClass, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    distributed::{CoordinatorApply, DistributedCoordinator},
    resources::ProviderRef,
    runtime::{TaskAdmissionRecord, TaskRunLimits},
    scheduler::{
        DurableOwner, EntrypointRef, OperationSpec, Orchestration, Reservation,
        canonical_swarm_resources,
    },
    swarm_budget::{SwarmBudgetLimits, SwarmForkRequest, SwarmOwnerFence, SwarmResourceRequest},
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use futures::future::join_all;
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tempfile::tempdir;

struct ContentVerifier {
    contents: Arc<BTreeMap<String, Vec<u8>>>,
}

impl acyclic_harness::conversation::ContentResidencyVerifier for ContentVerifier {
    fn verify<'a>(
        &'a self,
        reference: &'a FileRef,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let bytes = self
                .contents
                .get(reference.path())
                .ok_or_else(|| acyclic_harness::Error::NotFound(reference.path().into()))?;
            reference.descriptor().verify(bytes)
        })
    }

    fn read<'a>(
        &'a self,
        reference: &'a FileRef,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>>> + Send + 'a>> {
        Box::pin(async move {
            let bytes = self
                .contents
                .get(reference.path())
                .ok_or_else(|| acyclic_harness::Error::NotFound(reference.path().into()))?;
            reference.descriptor().verify(bytes)?;
            Ok(bytes.clone())
        })
    }
}

fn admission(operation_id: OperationId) -> Result<TaskAdmissionRecord> {
    TaskAdmissionRecord::from_parts(
        operation_id,
        "example.task",
        "1",
        json!(7),
        json!({"type":"integer"}),
        json!({"type":"string"}),
        &BTreeSet::new(),
        &[9; 32],
        None,
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
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    let path = format!("state/admission-{}.json", admission.operation_id);
    let volume = VolumeRef::new(
        ProviderRef::new("test", "filesystem", "2")?,
        "project",
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
    )?;
    FileRef::new(
        volume,
        path,
        "generation-1",
        FileDescriptor::from_bytes(&bytes, "application/json")?,
        "admission.json",
    )
    .map(|reference| (reference, bytes))
}

fn owner() -> Authority {
    Authority {
        kind: AggregateKind::Task,
        id: "owner".into(),
    }
}

fn child_spec(
    operation_id: OperationId,
    admission: &TaskAdmissionRecord,
    state: FileRef,
) -> Result<OperationSpec> {
    Ok(OperationSpec {
        operation_id,
        parent: None,
        owner: DurableOwner::Detached { authority: owner() },
        entrypoint: EntrypointRef {
            name: admission.task.name.clone(),
            version: admission.task.version.clone(),
            digest: admission.task.digest,
            result_schema: admission.output_schema.clone(),
        },
        dependencies: BTreeSet::new(),
        resources: canonical_swarm_resources(SwarmResourceRequest {
            model_steps: 4,
            output_bytes: 64,
            execution_time_ms: 100,
        }),
        placement: Default::default(),
        orchestration: Orchestration::Leaf,
        state,
    })
}

#[tokio::test]
async fn local_stream_coordinator_same_operation_race_is_one_applied_and_fifteen_replayed() {
    let root = tempdir().expect("temporary root");
    let stream_path = root.path().join("stream");
    let operation_id = OperationId::new();
    let session_id = OperationId::new();
    let child_admission = admission(operation_id).expect("child admission");
    let session_admission = admission(session_id).expect("session admission");
    let (child_state, child_bytes) = state_ref(&child_admission).expect("child state");
    let (session_state, session_bytes) = state_ref(&session_admission).expect("session state");
    let contents = Arc::new(BTreeMap::from([
        (child_state.path().to_owned(), child_bytes),
        (session_state.path().to_owned(), session_bytes),
    ]));
    let initial_client = StreamClient::new(Arc::new(
        LocalStream::open(&stream_path, LocalStreamLimits::default())
            .await
            .expect("local stream provider"),
    ));
    let verifier = Arc::new(ContentVerifier {
        contents: contents.clone(),
    });
    let mut coordinator = DistributedCoordinator::open(&initial_client, verifier.clone())
        .await
        .expect("open coordinator");
    let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner());
    let scope = issuer.root(
        "swarm",
        Capabilities::new(["operation:declare", "operation:admit"]),
    );
    let mut session_spec =
        child_spec(session_id, &session_admission, session_state.clone()).expect("session spec");
    session_spec.resources = Default::default();
    coordinator
        .declare_operation(
            &owner(),
            &scope,
            &issuer.verifier(),
            session_spec,
            IdempotencyKey::new("declare-session").expect("key"),
        )
        .await
        .expect("declare session root");
    coordinator
        .declare_operation(
            &owner(),
            &scope,
            &issuer.verifier(),
            child_spec(operation_id, &child_admission, child_state.clone()).expect("spec"),
            IdempotencyKey::new("declare-child").expect("key"),
        )
        .await
        .expect("declare child");

    let limits = SwarmBudgetLimits {
        max_active_agents: 2,
        max_total_agents: 2,
        max_recursion_depth: 1,
        max_model_steps: 8,
        max_output_bytes: 128,
        max_execution_time_ms: 200,
    };
    let request = SwarmForkRequest::from_task_admission(
        &child_admission,
        IdempotencyKey::new("fork-child").expect("fork key"),
        None,
        1,
        SwarmResourceRequest {
            model_steps: 4,
            output_bytes: 64,
            execution_time_ms: 100,
        },
    )
    .expect("canonical fork request");
    let reservation = Reservation {
        id: "lease-child".into(),
        placement: "worker".into(),
        admitted: canonical_swarm_resources(request.resources),
    };
    let coordinators = join_all((0..16).map(|_| {
        let stream_path = stream_path.clone();
        let verifier = Arc::new(ContentVerifier {
            contents: contents.clone(),
        });
        async move {
            let provider = LocalStream::open(stream_path, LocalStreamLimits::default())
                .await
                .expect("independent local stream provider");
            let client = StreamClient::new(Arc::new(provider));
            DistributedCoordinator::open(&client, verifier).await
        }
    }))
    .await
    .into_iter()
    .map(|coordinator| coordinator.expect("open racing coordinator"));
    let coordinators = coordinators.collect::<Vec<_>>();
    let results = join_all(coordinators.into_iter().map(|mut coordinator| {
        let scope = scope.clone();
        let issuer = issuer.clone();
        let request = request.clone();
        let reservation = reservation.clone();
        async move {
            coordinator
                .admit_swarm_child(
                    &owner(),
                    &scope,
                    &issuer.verifier(),
                    operation_id,
                    IdempotencyKey::new("coordinator-admit").expect("key"),
                    session_id,
                    limits,
                    SwarmOwnerFence::new("owner", 0).expect("owner fence"),
                    request,
                    child_state.clone(),
                    reservation,
                )
                .await
        }
    }))
    .await;
    let applied = results
        .iter()
        .filter(|result| matches!(result, Ok(CoordinatorApply::Applied)))
        .count();
    let replayed = results
        .iter()
        .filter(|result| matches!(result, Ok(CoordinatorApply::Replayed)))
        .count();
    assert_eq!(applied, 1);
    assert_eq!(replayed, 15);

    let provider = LocalStream::open(&stream_path, LocalStreamLimits::default())
        .await
        .expect("reopen local stream provider");
    let client = StreamClient::new(Arc::new(provider));
    let reopened = DistributedCoordinator::open(&client, Arc::new(ContentVerifier { contents }))
        .await
        .expect("reopen coordinator");
    let usage = reopened
        .scheduler()
        .swarm_budget_usage()
        .expect("budget projection")
        .expect("swarm session");
    assert_eq!(usage.active_agents, 2);
    assert_eq!(usage.total_agents, 2);
    assert_eq!(usage.reserved.model_steps, 4);
}
