use super::{Scenario, request};
use acyclic_harness::{
    external_executor::HookBinding,
    task_volume::{Component, Error, FileKey, FileValue, Owner, Query, TaskFile, TaskVolume},
};
use acyclic_stream::{MemoryStream, StreamPath};
use futures::StreamExt as _;
use proptest::prelude::*;
use serde_json::json;
use std::sync::Arc;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn volumes() -> Result<(TaskVolume<MemoryStream>, TaskVolume<MemoryStream>)> {
    let provider = Arc::new(MemoryStream::default());
    Ok((
        TaskVolume::open(provider.clone(), StreamPath::new("actors/requests")?).await?,
        TaskVolume::open(provider, StreamPath::new("actors/results")?).await?,
    ))
}

async fn publish(volume: &TaskVolume<MemoryStream>, file: &FileValue) -> Result {
    let snapshot = volume.snapshot(&Query::File(file.key.clone()), 1).await?;
    volume.append(snapshot.generation, file).await?;
    Ok(())
}

#[tokio::test]
async fn every_native_hook_round_trips_through_request_and_answer_files() -> Result {
    let scenarios: Vec<Scenario> = serde_json::from_str(include_str!("../external_hooks.json"))?;
    for scenario in scenarios {
        let (requests, results) = volumes().await?;
        let task = Component::new("task")?;
        let original = request(scenario.host, &scenario.event, scenario.input)?;
        let expected = original.clone();
        let expected_answer = json!({"operation":original.operation_id,"action":scenario.action});
        let mut incoming = requests
            .observe(Query::Requests(task.clone()), None, 1)
            .await?;
        let call = original.send(&requests, &results, task.clone());
        assert!(std::mem::size_of_val(&call) < 16 * 1024);
        let actor = async {
            let change = incoming.changes.next().await.ok_or("missing request")??;
            let file = change.file.ok_or("missing request file")?;
            assert_eq!(file.key.owner(), Owner::Caller);
            let answer = expected_answer.clone();
            let received =
                acyclic_harness::external_executor::ExternalRequest::from_file(&task, &file)?;
            assert_eq!(received, expected);
            let response = received.result_file(task.clone(), &answer)?;
            assert_eq!(response.key.owner(), Owner::Harness);
            assert_eq!(FileKey::parse(&response.key.path())?, response.key);
            publish(&results, &response).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(response)
        };
        let (answer, response) = tokio::join!(call, actor);
        let response = response?;
        assert_eq!(answer?, expected_answer);
        // A lost hook acknowledgement reuses the original file and answer.
        let before = requests.snapshot(&Query::Requests(task.clone()), 1).await?;
        assert_eq!(
            original.send(&requests, &results, task.clone()).await?,
            expected_answer
        );
        let after = requests.snapshot(&Query::Requests(task), 1).await?;
        assert_eq!(after.generation, before.generation);
        assert_eq!(after.files.len(), 1);
        // The hook's writable volume never contains an actor answer.
        assert!(
            requests
                .snapshot(&Query::File(response.key), 1)
                .await?
                .files
                .is_empty()
        );
    }
    Ok(())
}

#[tokio::test]
async fn changed_retry_arguments_cannot_return_an_earlier_answer() -> Result {
    let (requests, results) = volumes().await?;
    let task = Component::new("task")?;
    let first = request(
        super::ExternalHost::ClaudeCode,
        "PreToolUse",
        json!({
            "session_id":"session", "tool_use_id":"call", "tool_name":"Bash",
            "tool_input":{"command":"pwd"}
        }),
    )?;
    publish(&requests, &first.to_file(task.clone())?).await?;
    publish(
        &results,
        &first.result_file(task.clone(), &json!({"old":true}))?,
    )
    .await?;
    let changed = request(
        super::ExternalHost::ClaudeCode,
        "PreToolUse",
        json!({
            "session_id":"session", "tool_use_id":"call", "tool_name":"Bash",
            "tool_input":{"command":"write different data"}
        }),
    )?;
    assert_eq!(changed.operation_id, first.operation_id);
    assert!(matches!(
        changed.send(&requests, &results, task).await,
        Err(Error::ImmutableConflict(_))
    ));
    Ok(())
}

#[tokio::test]
async fn completed_answer_is_available_without_a_new_handler_or_request_append() -> Result {
    let (requests, results) = volumes().await?;
    let task = Component::new("task")?;
    let request = request(
        super::ExternalHost::Codex,
        "SessionEnd",
        json!({"session_id":"session"}),
    )?;
    let file = request.to_file(task.clone())?;
    publish(&requests, &file).await?;
    publish(
        &results,
        &request.result_file(task.clone(), &json!({"detached":true}))?,
    )
    .await?;
    let before = requests.snapshot(&Query::Requests(task.clone()), 1).await?;
    assert_eq!(
        request.send(&requests, &results, task.clone()).await?,
        json!({"detached":true})
    );
    assert_eq!(
        requests
            .snapshot(&Query::Requests(task), 1)
            .await?
            .generation,
        before.generation
    );
    Ok(())
}

#[test]
fn native_client_future_is_small() {
    let native = acyclic_harness::external_executor::run_hook("codex", "SessionEnd");
    let bytes = std::mem::size_of_val(&native);
    eprintln!("external native: {bytes} bytes");
    assert!(bytes < 16 * 1024, "external native: {bytes} bytes");
}

proptest! {
    // Property: result files have one canonical path, remain owner-only and
    // immutable, and request lifecycle queries include them without changing their owner.
    #[test]
    fn result_paths_round_trip_without_widening_request_authority(
        task in "[a-zA-Z0-9_-]{1,24}", session in "[a-zA-Z0-9_-]{1,24}", answer in ".{0,64}",
    ) {
        let task = Component::new(task)?;
        let request = request(super::ExternalHost::Codex, "SessionEnd", json!({"session_id":session}))?;
        let file = request.result_file(task.clone(), &json!(answer))?;
        let started = FileKey::Task {task:task.clone(), file:TaskFile::RequestStarted(
            Component::new(request.operation_id.to_string())?)};
        let ready = FileKey::Task {task:task.clone(), file:TaskFile::RequestReady(
            Component::new(request.operation_id.to_string())?)};
        for key in [file.key, started, ready] {
            prop_assert_eq!(FileKey::parse(&key.path())?, key.clone());
            prop_assert_eq!(key.owner(), Owner::Harness);
            prop_assert!(Query::Task(task.clone()).matches(&key));
            prop_assert!(Query::Requests(task.clone()).matches(&key));
        }
    }

    // Property: platform binding parsing round trips valid paths and rejects a
    // shared write/read path; the client cannot treat caller files as answers.
    #[test]
    fn platform_bindings_preserve_separate_owner_paths(part in "[a-zA-Z0-9_-]{1,24}") {
        let mut input = json!({"provider":"platform", "endpoint":"https://example.test", "bearer_file":"token",
            "requests":format!("{part}/requests"), "results":format!("{part}/results"), "task":part});
        let binding = HookBinding::from_bytes(&serde_json::to_vec(&input)?)?;
        let round_trip = HookBinding::from_bytes(&serde_json::to_vec(&binding)?)?;
        prop_assert_eq!(round_trip, binding);
        input.as_object_mut().unwrap().remove("provider");
        prop_assert!(HookBinding::from_bytes(&serde_json::to_vec(&input)?).is_err());
        input["provider"] = json!("platform");
        input["workspace"] = json!("/another-provider");
        prop_assert!(HookBinding::from_bytes(&serde_json::to_vec(&input)?).is_err());
        input.as_object_mut().unwrap().remove("workspace");
        input["results"] = input["requests"].clone();
        prop_assert!(HookBinding::from_bytes(&serde_json::to_vec(&input)?).is_err());
    }
}

#[tokio::test]
async fn dispatch_claims_and_answers_reject_changed_contents() -> Result {
    let (_, results) = volumes().await?;
    let task = Component::new("task")?;
    let operation = Component::new("operation")?;
    for file in [
        TaskFile::RequestStarted(operation.clone()),
        TaskFile::RequestResult(operation),
    ] {
        let key = FileKey::Task {
            task: task.clone(),
            file,
        };
        let original = FileValue::new(key.clone(), b"original".to_vec());
        let before = results.snapshot(&Query::File(key.clone()), 1).await?;
        let generation = results.append(before.generation, &original).await?;
        let changed = FileValue::new(key, b"changed".to_vec());
        assert!(matches!(
            results.append(generation, &changed).await,
            Err(Error::ImmutableConflict(_))
        ));
    }
    Ok(())
}
