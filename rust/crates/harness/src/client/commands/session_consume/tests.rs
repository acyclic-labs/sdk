use super::*;
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;

#[tokio::test]
async fn a_settled_prompt_already_has_its_selected_task() {
    use futures::StreamExt as _;
    let volume = Arc::new(
        files::TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("selection-before-answer").unwrap(),
        )
        .await
        .unwrap(),
    );
    let session = uuid::Uuid::from_bytes([1; 16]);
    let task = TaskId::from_bytes([2; 16]);
    let mut commands = CommandSession::open(volume.clone(), session, 65536, 128)
        .await
        .unwrap();
    let operation = commands
        .publish_resolution(CommandResolution::Prompt {
            text: "hello".into(),
            source: None,
            mentions: Vec::new(),
        })
        .await
        .unwrap();
    let request = commands.next_request().await.unwrap();
    let mut observed = volume
        .observe(
            files::Query::CommandSession {
                session: component(session.to_string()).unwrap(),
                recent: 32,
            },
            None,
            128,
        )
        .await
        .unwrap();
    commands
        .consume(&request, |_, _, _| async {
            Ok(json!({"text":"done", "task":task, "selected_task":task}))
        })
        .await
        .unwrap();
    let mut selected = false;
    while let Some(change) = observed.changes.next().await {
        let change = change.unwrap();
        for file in change.file.into_iter().chain(change.files) {
            match &file.key {
                files::FileKey::Ui { name, .. } if name.as_str() == "layout.json" => {
                    let value: Value = serde_json::from_slice(&file.bytes).unwrap();
                    selected = value["selected_task"] == json!(task);
                }
                files::FileKey::Session {
                    file: files::TaskFile::ExecutionResult(id),
                    ..
                } if files::command_query::identity(id).is_some_and(|(_, id)| id == operation) => {
                    assert!(selected, "settlement must follow durable task selection");
                    return;
                }
                _ => {}
            }
        }
    }
    panic!("command follow ended before settlement");
}

#[tokio::test]
async fn clear_outcome_hides_itself_and_prior_output_after_reopen() {
    clear_boundary::<true>().await;
}

#[tokio::test]
#[should_panic(expected = "clear retained its own outcome")]
async fn layout_only_clear_boundary_retains_its_completion_negative_control() {
    clear_boundary::<false>().await;
}

async fn clear_boundary<const RETAIN_COMPLETION: bool>() {
    let volume = Arc::new(
        files::TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("clear-boundary").unwrap(),
        )
        .await
        .unwrap(),
    );
    let session = uuid::Uuid::from_bytes([9; 16]);
    let mut commands = CommandSession::open(volume.clone(), session, 65536, 128)
        .await
        .unwrap();
    for (index, cleared) in [false, true, false].into_iter().enumerate() {
        let request = files::FileValue::new(
            files::FileKey::Session {
                session: component(session.to_string()).unwrap(),
                file: files::TaskFile::Request(
                    component(
                        OperationId::from_bytes([u8::try_from(index).unwrap() + 1; 16]).to_string(),
                    )
                    .unwrap(),
                ),
            },
            serde_json::to_vec(&CommandResolution::Prompt {
                text: "fixture".into(),
                source: None,
                mentions: Vec::new(),
            })
            .unwrap(),
        );
        volume
            .append(volume.head().await.unwrap(), &request)
            .await
            .unwrap();
        commands
            .consume(&request, |_, _, _| async move {
                Ok(json!({"selected_task":null,"cleared":cleared,"value":index}))
            })
            .await
            .unwrap();
        if !RETAIN_COMPLETION {
            commands.cleared_output = 0;
        }
        if cleared {
            assert!(
                commands.output_page(0).unwrap().is_none(),
                "clear retained its own outcome"
            );
        } else {
            assert_eq!(
                commands.output_page(0).unwrap().unwrap()["ok"]["value"],
                index
            );
            assert!(commands.output_page(1).unwrap().is_none());
        }
        commands = CommandSession::open(volume.clone(), session, 65536, 128)
            .await
            .unwrap();
        assert_eq!(commands.output_page(0).unwrap().is_none(), cleared);
    }
}

async fn framed_result<const EXACT: bool>(extra: usize, task_root: bool) {
    let volume = Arc::new(
        files::TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("framed-result").unwrap(),
        )
        .await
        .unwrap(),
    );
    let session = uuid::Uuid::from_bytes([1; 16]);
    let operation = OperationId::from_bytes([2; 16]);
    let id = component(operation.to_string()).unwrap();
    let key = |file| {
        if task_root {
            files::FileKey::Task {
                task: component(TaskId::from_bytes([3; 16]).to_string()).unwrap(),
                file,
            }
        } else {
            files::FileKey::Session {
                session: component(session.to_string()).unwrap(),
                file,
            }
        }
    };
    let result_key = key(files::TaskFile::ExecutionResult(id.clone()));
    let framing =
        files::record_bytes(&files::FileValue::new(result_key.clone(), Vec::new())).unwrap();
    let budget = acyclic_stream::MAX_RECORD_BYTES - framing;
    let empty = archive::output_bytes(0, &Ok(json!({"stdout":""})), u64::MAX)
        .unwrap()
        .len();
    let result = json!({"stdout":"x".repeat(budget + extra - empty)});
    let mut commands = CommandSession::open(
        volume.clone(),
        session,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        32,
    )
    .await
    .unwrap();
    let request = files::FileValue::new(
        key(files::TaskFile::Request(id)),
        serde_json::to_vec(&CommandResolution::Prompt {
            text: "hi".into(),
            source: None,
            mentions: Vec::new(),
        })
        .unwrap(),
    );
    let cut = volume.head().await.unwrap();
    volume.append(cut, &request).await.unwrap();
    if EXACT {
        let mut calls = 0;
        commands
            .consume(&request, |_, _, _| {
                calls += 1;
                async { Ok(result) }
            })
            .await
            .unwrap();
        assert_eq!(calls, 1);
        let saved = volume
            .snapshot(&files::Query::File(result_key), 1)
            .await
            .unwrap();
        let saved = &saved.files[0];
        assert!(files::record_bytes(saved).unwrap() <= acyclic_stream::MAX_RECORD_BYTES);
        let body: Value = serde_json::from_slice(&saved.bytes).unwrap();
        assert_eq!(body["output_omitted"], true);
        assert_eq!(body["completion"], "completed");
        drop(commands);
        let mut reopened =
            CommandSession::open(volume, session, acyclic_stream::MAX_RECORD_BYTES as u64, 32)
                .await
                .unwrap();
        assert!(
            reopened
                .consume(&request, |_, _, _| async {
                    panic!("saved outcome dispatched twice")
                })
                .await
                .unwrap()
                .is_none()
        );
    } else {
        // Original policy: the body fits, so omission never runs. Its metadata
        // then overflows the actual record and the terminal publication fails.
        let bytes =
            archive::output_bytes(0, &Ok(result), acyclic_stream::MAX_RECORD_BYTES as u64).unwrap();
        assert!(bytes.len() <= acyclic_stream::MAX_RECORD_BYTES);
        assert!(
            serde_json::from_slice::<Value>(&bytes)
                .unwrap()
                .get("output_omitted")
                .is_none()
        );
        commands
            .publish(result_key, bytes, true)
            .await
            .expect("old payload-only budget stranded the result");
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn near_limit_results_include_record_framing_and_never_replay(extra in 8usize..64, task_root in any::<bool>()) {
        futures::executor::block_on(framed_result::<true>(extra,task_root));
    }
}
#[test]
#[should_panic(expected = "old payload-only budget stranded the result")]
fn payload_only_result_budget_is_detected_negative_control() {
    futures::executor::block_on(framed_result::<false>(8, false));
}

async fn selection_before_completion<const ORDERED: bool>(id: [u8; 16], failed: bool) {
    use futures::StreamExt as _;
    let volume = Arc::new(
        files::TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("selection-order").unwrap(),
        )
        .await
        .unwrap(),
    );
    let mut commands =
        CommandSession::open(volume.clone(), uuid::Uuid::from_bytes([1; 16]), 65536, 128)
            .await
            .unwrap();
    let request = files::FileValue::new(
        files::FileKey::Session {
            session: component(commands.session.to_string()).unwrap(),
            file: files::TaskFile::Request(
                component(OperationId::from_bytes([2; 16]).to_string()).unwrap(),
            ),
        },
        serde_json::to_vec(&CommandResolution::Prompt {
            text: "first".into(),
            source: None,
            mentions: Vec::new(),
        })
        .unwrap(),
    );
    volume
        .append(volume.head().await.unwrap(), &request)
        .await
        .unwrap();
    let mut observation = volume.observe(files::Query::Root, None, 128).await.unwrap();
    let selected = TaskId::from_bytes(id);
    SELECTION_AFTER_OUTCOME.with(|flag| flag.set(!ORDERED));
    let outcome = commands
        .consume(&request, |_, _, _| async {
            if failed {
                Err(Error::TaskFailed {
                    task: selected,
                    message: "interrupted stream".into(),
                })
            } else {
                Ok(json!({"selected_task":selected}))
            }
        })
        .await;
    SELECTION_AFTER_OUTCOME.with(|flag| flag.set(false));
    assert_eq!(outcome.is_err(), failed);
    assert_eq!(commands.active_task(), None);
    assert_eq!(commands.presentation_task().unwrap(), Some(selected));
    let mut seen_selection = false;
    while let Some(change) = observation.changes.next().await {
        let change = change.unwrap();
        for file in change.file.iter().chain(&change.files) {
            match &file.key {
                files::FileKey::Ui { name, .. } if name.as_str() == "layout.json" => {
                    seen_selection = serde_json::from_slice::<Value>(&file.bytes).unwrap()["selected_task"]
                        == json!(selected);
                }
                files::FileKey::Session {
                    file: files::TaskFile::ExecutionResult(_),
                    ..
                } => {
                    let outcome: Value = serde_json::from_slice(&file.bytes).unwrap();
                    assert_eq!(
                        outcome["completion"],
                        if failed { "failed" } else { "completed" }
                    );
                    assert!(
                        seen_selection,
                        "completion must follow its selected conversation"
                    );
                    return;
                }
                _ => {}
            }
        }
    }
    panic!("missing completion");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn every_completion_observer_has_the_selection(id in prop::array::uniform16(any::<u8>()), failed in any::<bool>()) {
        futures::executor::block_on(selection_before_completion::<true>(id, failed));
    }
}

#[test]
#[should_panic(expected = "completion must follow its selected conversation")]
fn completion_before_selection_negative_control() {
    futures::executor::block_on(selection_before_completion::<false>([3; 16], false));
}

#[test]
fn failed_task_keeps_its_selected_transcript() {
    futures::executor::block_on(selection_before_completion::<true>([3; 16], true));
}

fn cancelled_execution<const RETAIN: bool>(task: TaskId) -> crate::client::CommandExecution {
    let failure = Err(Error::InteractionRejected(
        crate::InteractionRejection::Cancelled,
    ));
    if RETAIN {
        crate::client::CommandExecution::admitted(task, failure)
    } else {
        // The former uncontextual error discarded an admitted task.
        crate::client::CommandExecution::from(failure)
    }
}

async fn cancelled_selection<const RETAIN: bool>(task_root: bool, recovering: bool) {
    let volume = Arc::new(
        files::TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("cancelled-selection").unwrap(),
        )
        .await
        .unwrap(),
    );
    let session = uuid::Uuid::from_bytes([1; 16]);
    let operation = OperationId::from_bytes([2; 16]);
    let selected = crate::runtime::task_id_for_admission(operation);
    let id = component(operation.to_string()).unwrap();
    let request = files::FileValue::new(
        if task_root {
            files::FileKey::Task {
                task: component(TaskId::from_bytes([3; 16]).to_string()).unwrap(),
                file: files::TaskFile::Request(id),
            }
        } else {
            files::FileKey::Session {
                session: component(session.to_string()).unwrap(),
                file: files::TaskFile::Request(id),
            }
        },
        serde_json::to_vec(&CommandResolution::Prompt {
            text: "retain my interrupted answer".into(),
            source: None,
            mentions: Vec::new(),
        })
        .unwrap(),
    );
    volume
        .append(volume.head().await.unwrap(), &request)
        .await
        .unwrap();
    let mut commands = CommandSession::open(volume.clone(), session, 65536, 128)
        .await
        .unwrap();
    if recovering {
        let claim = related(&request.key, files::TaskFile::ExecutionClaim).unwrap();
        commands
            .publish(claim, request.bytes.to_vec(), true)
            .await
            .unwrap();
    }
    let outcome = commands
        .consume_with_recovery::<true, _, _, _, _>(
            &request,
            |_, _, _| async {
                assert!(!recovering, "claimed cancellation redispatched");
                cancelled_execution::<RETAIN>(selected)
            },
            |_, _, _| async {
                assert!(recovering, "unclaimed cancellation recovered");
                cancelled_execution::<RETAIN>(selected)
            },
        )
        .await;
    assert!(matches!(
        outcome,
        Err(Error::InteractionRejected(
            crate::InteractionRejection::Cancelled
        ))
    ));
    assert_eq!(commands.active_task(), None);
    assert_eq!(
        commands.presentation_task().unwrap(),
        Some(selected),
        "cancelled completion lost its admitted conversation"
    );
    assert_eq!(
        commands.output_page(0).unwrap().unwrap()["completion"],
        "cancelled"
    );
    drop(commands);
    let mut reopened = CommandSession::open(volume, session, 65536, 128)
        .await
        .unwrap();
    assert_eq!(reopened.presentation_task().unwrap(), Some(selected));
    assert!(
        reopened
            .consume(&request, |_, _, _| async {
                panic!("cancelled task redispatched")
            })
            .await
            .unwrap()
            .is_none()
    );
}

#[test]
fn cancelled_admissions_retain_selection_and_never_redispatch() {
    for task_root in [false, true] {
        for recovering in [false, true] {
            futures::executor::block_on(cancelled_selection::<true>(task_root, recovering));
        }
    }
}

#[test]
#[should_panic(expected = "cancelled completion lost its admitted conversation")]
fn uncontextual_cancellation_is_detected_negative_control() {
    futures::executor::block_on(cancelled_selection::<false>(false, false));
}

#[test]
#[should_panic(expected = "completion must follow its selected conversation")]
fn failed_completion_before_selection_negative_control() {
    futures::executor::block_on(selection_before_completion::<false>([3; 16], true));
}
