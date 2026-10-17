use super::*;
use crate::task_volume::{ChildState, ChildStatus, FileQuerySource};
use crate::{
    AgentId, Capabilities, OperationId, TaskId,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    resources::ProviderRef,
};
use acyclic_stream::MemoryStream;
use proptest::prelude::*;
use std::sync::Arc;

async fn wait<P: StreamProvider + ?Sized>(
    files: &TaskFiles<P>,
    input: WaitInput,
    limits: Limits,
    operation: OperationId,
    block: bool,
) -> Result<Option<Value>> {
    super::wait(
        files,
        input,
        limits,
        operation,
        block,
        TaskId::from_bytes([2; 16]),
        None,
    )
    .await
}

async fn files() -> TaskFiles<MemoryStream> {
    files_with(Arc::new(MemoryStream::default())).await
}

async fn files_with(provider: Arc<MemoryStream>) -> TaskFiles<MemoryStream> {
    let agent = AgentId::from_bytes([1; 16]);
    let volume = VolumeRef::new(
        ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None).unwrap(),
        "agent-tool".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )
    .unwrap();
    let issuer = AuthorityIssuer::new(
        "agent-tool",
        [1; 32],
        Authority {
            kind: AggregateKind::Task,
            id: TaskId::from_bytes([2; 16]).to_string(),
        },
    );
    let grants = [VolumeOperation::Read, VolumeOperation::Write]
        .map(|operation| volume.capability(operation, &mut None).unwrap());
    let scope = issuer
        .issue(
            "owner",
            &Capabilities::new(grants),
            Some(agent),
            None,
            &mut None,
        )
        .unwrap();
    TaskFiles::open(
        provider,
        &volume,
        &issuer.verifier(&mut None).unwrap(),
        &scope,
        4096,
    )
    .await
    .unwrap()
}

fn limits() -> Limits {
    Limits {
        file_bytes: 4096,
        render_bytes: 4096,
        attachments: 16,
        ..Limits::default()
    }
}

async fn acknowledge(files: &TaskFiles<MemoryStream>, reference: &Component, state: ChildState) {
    acknowledge_answer(files, reference, state, "child answer").await
}

async fn acknowledge_answer(
    files: &TaskFiles<MemoryStream>,
    reference: &Component,
    state: ChildState,
    answer: &str,
) {
    let snapshot = files
        .observe(Query::Child(reference.clone()), None, 16)
        .await
        .unwrap()
        .snapshot
        .unwrap();
    let previous = snapshot.files.iter().find_map(|file| match &file.key {
        FileKey::Child {
            file: ChildFile::Status,
            ..
        } => Some(ChildStatus::parse(&file.bytes).unwrap()),
        _ => None,
    });
    let inputs = if matches!(state, ChildState::Cancelled | ChildState::Archived) {
        previous.map(|status| status.inputs)
    } else {
        None
    }
    .unwrap_or_else(|| {
        snapshot
            .files
            .iter()
            .filter(|file| {
                matches!(
                    &file.key,
                    FileKey::Child {
                        file: ChildFile::Request | ChildFile::Inbox(_),
                        ..
                    }
                )
            })
            .count() as u64
    });
    let status = ChildStatus {
        task: previous
            .map(|status| status.task)
            .unwrap_or(crate::TaskId::from_bytes([7; 16])),
        state,
        settled: true,
        inputs,
    };
    let reference = reference.clone();
    let status_key = FileKey::Child {
        reference: reference.clone(),
        file: ChildFile::Status,
    };
    let mut generation = snapshot.generation;
    if state == ChildState::Running {
        let pending = ChildStatus {
            settled: false,
            ..status
        };
        generation = files
            .inner
            .append(
                generation,
                &FileValue::new(status_key.clone(), serde_json::to_vec(&pending).unwrap()),
            )
            .await
            .unwrap();
        for turn in 1..=inputs {
            let key = FileKey::Child {
                reference: reference.clone(),
                file: ChildFile::Result(turn),
            };
            if snapshot.files.iter().any(|file| file.key == key) {
                continue;
            }
            generation = files.inner.append(generation, &FileValue::new(key,
                serde_json::to_vec(&json!({"task":status.task,"turn":turn,
                    "outcome":{"Succeeded":{"text":answer,"metadata":null,"steps":1,"attachments":[]}}})).unwrap(),
            )).await.unwrap();
        }
    }
    files
        .inner
        .append(
            generation,
            &FileValue::new(status_key, serde_json::to_vec(&status).unwrap()),
        )
        .await
        .unwrap();
}

async fn publish(
    files: &TaskFiles<MemoryStream>,
    reference: &Component,
    kind: ChildFile,
    bytes: &[u8],
    operation: OperationId,
) -> Result<bool> {
    let _ = operation;
    super::files::write(
        files,
        reference,
        FileValue::new(
            FileKey::Child {
                reference: reference.clone(),
                file: kind,
            },
            bytes.to_vec(),
        ),
        limits(),
        None,
        false,
    )
    .await
}

#[tokio::test]
async fn child_files_replay_exactly_and_archive_preserves_history() {
    let files = files().await;
    let worker = Component::new("worker").unwrap();
    let operation = OperationId::new();
    for _ in 0..2 {
        publish(&files, &worker, ChildFile::Request, b"work", operation)
            .await
            .unwrap();
        publish(
            &files,
            &worker,
            ChildFile::Inbox(Component::new(operation.to_string()).unwrap()),
            b"more",
            operation,
        )
        .await
        .unwrap();
    }
    let snapshot = files
        .observe(Query::Child(worker.clone()), None, 16)
        .await
        .unwrap()
        .snapshot
        .unwrap();
    assert_eq!(snapshot.files.len(), 2);
    assert!(
        publish(&files, &worker, ChildFile::Request, b"different", operation)
            .await
            .is_err()
    );
    let mut admitted = Box::pin(super::files::follow(&files, &worker, limits(), true, false));
    assert!(futures::poll!(&mut admitted).is_pending());
    acknowledge(&files, &worker, ChildState::Running).await;
    assert_eq!(admitted.await.unwrap().unwrap()["execution"]["inputs"], 2);
    publish(
        &files,
        &worker,
        ChildFile::Control,
        Control::Archive.bytes(),
        operation,
    )
    .await
    .unwrap();
    let mut settled = Box::pin(super::files::follow(&files, &worker, limits(), true, true));
    assert!(futures::poll!(&mut settled).is_pending());
    acknowledge(&files, &worker, ChildState::Archived).await;
    let result = settled.await.unwrap().unwrap();
    assert_eq!(result["state"], json!(ChildState::Archived));
    assert_eq!(
        result["result"]["outcome"]["Succeeded"]["text"],
        "child answer"
    );
    assert!(
        publish(
            &files,
            &worker,
            ChildFile::Inbox(Component::new("late").unwrap()),
            b"late",
            operation
        )
        .await
        .is_err()
    );
    let archived = files
        .observe(Query::Child(worker), None, 16)
        .await
        .unwrap()
        .snapshot
        .unwrap();
    assert_eq!(
        archived
            .files
            .iter()
            .filter(|file| matches!(
                file.key,
                FileKey::Child {
                    file: ChildFile::Request | ChildFile::Inbox(_),
                    ..
                }
            ))
            .count(),
        2
    );
}

#[tokio::test]
async fn wait_any_returns_the_settled_answer_and_all_follows_the_remaining_child() {
    let files = files().await;
    let workers = [
        Component::new("first").unwrap(),
        Component::new("second").unwrap(),
    ];
    let operation = OperationId::new();
    for worker in &workers {
        publish(&files, worker, ChildFile::Request, b"work", operation)
            .await
            .unwrap();
    }
    acknowledge(&files, &workers[0], ChildState::Running).await;
    let input = |mode| WaitInput {
        agents: workers
            .iter()
            .map(|worker| worker.as_str().into())
            .collect(),
        mode,
    };
    let first = wait(
        &files,
        input(Some(WaitMode::Any)),
        limits(),
        operation,
        true,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(first["agents"].as_array().unwrap().len(), 1);
    assert_eq!(first["agents"][0]["reference"], "first");
    assert_eq!(
        first["agents"][0]["result"]["outcome"]["Succeeded"]["text"],
        "child answer"
    );
    assert!(
        wait(&files, input(None), limits(), operation, false)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        wait(
            &files,
            input(Some(WaitMode::Any)),
            limits(),
            operation,
            false
        )
        .await
        .unwrap()
        .unwrap()["agents"][0]["reference"],
        "first"
    );
    let mut all = Box::pin(wait(&files, input(None), limits(), operation, true));
    assert!(futures::poll!(&mut all).is_pending());
    acknowledge(&files, &workers[1], ChildState::Running).await;
    let all = all.await.unwrap().unwrap();
    assert_eq!(all["agents"].as_array().unwrap().len(), 2);
    assert_eq!(all["agents"][1]["reference"], "second");
    for agents in [
        vec![],
        vec!["first".into(), "first".into()],
        vec!["../first".into()],
    ] {
        assert!(
            wait(
                &files,
                WaitInput { agents, mode: None },
                limits(),
                operation,
                true,
            )
            .await
            .is_err()
        );
    }
}

#[tokio::test]
async fn cancelled_child_can_only_advance_to_an_archive_acknowledgement() {
    let files = files().await;
    let worker = Component::new("worker").unwrap();
    let operation = OperationId::new();
    publish(&files, &worker, ChildFile::Request, b"work", operation)
        .await
        .unwrap();
    acknowledge(&files, &worker, ChildState::Running).await;
    publish(
        &files,
        &worker,
        ChildFile::Control,
        Control::Cancel.bytes(),
        operation,
    )
    .await
    .unwrap();
    acknowledge(&files, &worker, ChildState::Cancelled).await;
    for control in [Control::Resume, Control::Pause, Control::Interrupt] {
        assert!(
            publish(
                &files,
                &worker,
                ChildFile::Control,
                control.bytes(),
                operation,
            )
            .await
            .is_err()
        );
    }
    publish(
        &files,
        &worker,
        ChildFile::Control,
        Control::Archive.bytes(),
        operation,
    )
    .await
    .unwrap();
    acknowledge(&files, &worker, ChildState::Archived).await;
    let archived = super::files::follow(&files, &worker, limits(), false, true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(archived["state"], json!(ChildState::Archived));
    assert_eq!(
        archived["result"]["outcome"]["Succeeded"]["text"],
        "child answer"
    );
    assert!(
        publish(
            &files,
            &worker,
            ChildFile::Control,
            Control::Cancel.bytes(),
            operation
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn new_messages_and_controls_wait_for_a_matching_settled_ack() {
    let files = files().await;
    let worker = Component::new("worker").unwrap();
    let operation = OperationId::new();
    publish(&files, &worker, ChildFile::Request, b"work", operation)
        .await
        .unwrap();
    acknowledge(&files, &worker, ChildState::Running).await;
    publish(
        &files,
        &worker,
        ChildFile::Inbox(Component::new("next").unwrap()),
        b"more",
        operation,
    )
    .await
    .unwrap();
    let mut waiting = Box::pin(super::files::follow(&files, &worker, limits(), true, true));
    assert!(futures::poll!(&mut waiting).is_pending());
    publish(
        &files,
        &worker,
        ChildFile::Control,
        Control::Interrupt.bytes(),
        operation,
    )
    .await
    .unwrap();
    assert!(futures::poll!(&mut waiting).is_pending());
    acknowledge(&files, &worker, ChildState::Interrupted).await;
    assert_eq!(
        waiting.await.unwrap().unwrap()["state"],
        json!(ChildState::Interrupted)
    );
    let before = files.inner.head().await.unwrap();
    let request = FileValue::new(
        FileKey::Child {
            reference: worker.clone(),
            file: ChildFile::Request,
        },
        b"work".to_vec(),
    );
    assert!(
        super::files::write(&files, &worker, request.clone(), limits(), None, true)
            .await
            .unwrap()
    );
    let different = FileValue::new(request.key.clone(), b"different".to_vec());
    assert!(
        !super::files::write(&files, &worker, different, limits(), None, true)
            .await
            .unwrap()
    );
    assert_eq!(files.inner.head().await.unwrap(), before);
}

#[tokio::test]
async fn child_files_preserve_the_callers_file_and_snapshot_bounds() {
    let files = files().await;
    let worker = Component::new("worker").unwrap();
    let request = FileValue::new(
        FileKey::Child {
            reference: worker.clone(),
            file: ChildFile::Request,
        },
        b"four".to_vec(),
    );
    let before = files.inner.head().await.unwrap();
    assert!(
        super::files::write(
            &files,
            &worker,
            request.clone(),
            Limits {
                file_bytes: 3,
                ..limits()
            },
            None,
            false
        )
        .await
        .is_err()
    );
    assert_eq!(files.inner.head().await.unwrap(), before);
    super::files::write(&files, &worker, request, limits(), None, false)
        .await
        .unwrap();
    assert!(
        super::files::follow(
            &files,
            &worker,
            Limits {
                render_bytes: 3,
                ..limits()
            },
            false,
            false
        )
        .await
        .is_err()
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn four_tool_inputs_round_trip_and_reject_the_removed_command_field(
        agent in "[a-z][a-z0-9_-]{0,20}", task in ".{1,100}", any_mode in any::<bool>(),
    ) {
        macro_rules! round_trip {
            ($value:expr, $ty:ty) => {{
                let value = $value;
                let mut encoded = serde_json::to_value(&value).unwrap();
                prop_assert_eq!(serde_json::from_value::<$ty>(encoded.clone()).unwrap(), value);
                let name = if stringify!($ty) == "SpawnInput" { "spawn" } else if stringify!($ty) == "SendInput" { "send" } else if stringify!($ty) == "WaitInput" { "wait" } else { "archive" };
                let spec = crate::tools::catalog().find(|spec| spec.name == name).unwrap();
                let contract = spec.definition().unwrap().input_schema;
                let validator = jsonschema::validator_for(&contract).unwrap();
                prop_assert!(validator.is_valid(&encoded));
                encoded.as_object_mut().unwrap().insert("command".into(), json!("spawn"));
                prop_assert!(!validator.is_valid(&encoded));
                prop_assert!(serde_json::from_value::<$ty>(encoded).is_err());
            }};
        }
        round_trip!(SpawnInput { task: task.clone(), name: Some(agent.clone()), grants: Some(vec!["model:generate".into(), "tool:call:read".into()]) }, SpawnInput);
        round_trip!(SendInput { agent: agent.clone(), message: task }, SendInput);
        round_trip!(WaitInput { agents: vec![agent.clone()], mode: Some(if any_mode { WaitMode::Any } else { WaitMode::All }) }, WaitInput);
        round_trip!(ArchiveInput { agent }, ArchiveInput);
    }
}

mod review;
