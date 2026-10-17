use super::{Scenario, request};
use acyclic_harness::{
    OperationId,
    external_executor::{ExternalAction, ExternalHost, ExternalRequest},
    task_volume::{Component, Error, FileKey, FileValue, Generation, Query, TaskFile, TaskVolume},
};
use acyclic_stream::{MemoryStream, StreamPath};
use futures::{FutureExt as _, StreamExt as _};
use serde_json::{Value, json};
use std::sync::Arc;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn volume() -> Result<TaskVolume<MemoryStream>> {
    Ok(TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("actors/task/inbox")?,
    )
    .await?)
}

fn tool() -> Result<ExternalRequest> {
    Ok(request(
        ExternalHost::ClaudeCode,
        "PreToolUse",
        json!({"session_id":"session","tool_use_id":"call","tool_name":"Bash","tool_input":{"command":"pwd"}}),
    )?)
}

fn file(task: &Component, request: &ExternalRequest) -> Result<FileValue> {
    Ok(FileValue::new(
        FileKey::Task {
            task: task.clone(),
            file: TaskFile::Request(Component::new(request.operation_id.to_string())?),
        },
        serde_json::to_vec(request)?,
    ))
}

async fn publish(
    volume: &TaskVolume<MemoryStream>,
    request: &ExternalRequest,
    task: Component,
) -> std::result::Result<Generation, Error> {
    let file = request.to_file(task)?;
    let snapshot = volume.snapshot(&Query::File(file.key.clone()), 1).await?;
    volume.append(snapshot.generation, &file).await
}

#[tokio::test]
async fn every_host_hook_is_an_immediate_immutable_request_file() -> Result {
    let volume = volume().await?;
    let task = Component::new("task")?;
    let query = Query::Requests(task.clone());
    let initial = volume.snapshot(&query, 100).await?;
    assert!(initial.files.is_empty());
    let mut changes = volume.follow(query.clone(), initial.cursor.clone()).await?;
    let scenarios: Vec<Scenario> = serde_json::from_str(include_str!("../external_hooks.json"))?;
    let mut expected = Vec::new();
    let mut cursor = initial.cursor.clone();
    for scenario in scenarios {
        let request = request(scenario.host, &scenario.event, scenario.input)?;
        let generation = publish(&volume, &request, task.clone()).await?;
        assert_eq!(generation.position, cursor.position + 1);
        let change = changes
            .next()
            .now_or_never()
            .expect("an accepted request must wake its follower immediately")
            .expect("live request follow")?;
        let file = change.file.expect("request publication");
        assert_eq!(ExternalRequest::from_file(&task, &file)?, request);
        assert_eq!(change.cursor.position, generation.position);
        assert!(change.cursor.accepted >= generation.position);
        cursor = change.cursor;
        assert_eq!(publish(&volume, &request, task.clone()).await?, generation);
        assert!(changes.next().now_or_never().is_none());
        expected.push(request);
    }
    let snapshot = volume.snapshot(&query, 100).await?;
    assert_eq!(snapshot.files.len(), expected.len());
    assert_eq!(snapshot.cursor.position, cursor.position);
    assert_eq!(snapshot.cursor.commit, cursor.commit);
    for file in &snapshot.files {
        assert!(expected.contains(&ExternalRequest::from_file(&task, file)?));
    }
    // Resume the same logical request files from the snapshot cut, without a second journal.
    let mut replay = volume.follow(query, initial.cursor).await?;
    for request in expected {
        let change = replay.next().await.expect("stored request")?;
        assert_eq!(
            ExternalRequest::from_file(&task, &change.file.expect("request"))?,
            request
        );
    }
    assert!(replay.next().now_or_never().is_none());
    Ok(())
}

#[tokio::test]
async fn conflicting_hook_retries_do_not_publish_another_operation() -> Result {
    let volume = volume().await?;
    let task = Component::new("task")?;
    let query = Query::Requests(task.clone());
    let mut request = tool()?;
    let admitted = publish(&volume, &request, task.clone()).await?;
    request.action = ExternalAction::RunTool {
        call_id: "call".into(),
        name: "Bash".into(),
        arguments: json!({"command":"ls"}),
    };
    assert!(matches!(
        publish(&volume, &request, task).await,
        Err(Error::ImmutableConflict(_))
    ));
    assert_eq!(volume.snapshot(&query, 1).await?.generation, admitted);
    Ok(())
}

#[test]
fn request_decoding_rejects_other_tasks_and_mismatched_or_malformed_identities() -> Result {
    let task = Component::new("task")?;
    let request = tool()?;
    let original = file(&task, &request)?;
    let mut malformed = vec![file(&Component::new("other")?, &request)?];
    let mut wrong_path = original.clone();
    wrong_path.key = FileKey::Task {
        task: task.clone(),
        file: TaskFile::Status,
    };
    malformed.push(wrong_path);
    let mut wrong_id = original.clone();
    wrong_id.key = FileKey::Task {
        task: task.clone(),
        file: TaskFile::Request(Component::new(OperationId::new().to_string())?),
    };
    malformed.push(wrong_id);
    for field in ["session_id", "turn_id", "caller"] {
        let mut value = serde_json::to_value(&request)?;
        value
            .as_object_mut()
            .expect("request")
            .insert(field.into(), json!(""));
        let mut invalid = original.clone();
        invalid.bytes = serde_json::to_vec(&value)?.into();
        malformed.push(invalid);
    }
    let mut invalid = request;
    invalid.operation_id = OperationId::new();
    malformed.push(file(&task, &invalid)?);
    let mut invalid = original.clone();
    invalid.channel = Some(acyclic_harness::task_volume::Channel::Text);
    malformed.push(invalid);
    let mut invalid = original;
    invalid.bytes = b"{not json}".as_slice().into();
    malformed.push(invalid);
    for invalid in malformed {
        assert!(matches!(
            ExternalRequest::from_file(&task, &invalid),
            Err(Error::InvalidSegment)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn oversized_or_deep_requests_never_publish_unreplayable_files() -> Result {
    let volume = volume().await?;
    let task = Component::new("task")?;
    let query = Query::Requests(task.clone());
    let initial = volume.snapshot(&query, 1).await?;
    let mut arguments = Value::Null;
    for _ in 0..256 {
        arguments = Value::Array(vec![arguments]);
    }
    for arguments in [
        json!("x".repeat(acyclic_stream::MAX_RECORD_BYTES)),
        arguments,
    ] {
        let mut request = tool()?;
        request.action = ExternalAction::RunTool {
            call_id: "call".into(),
            name: "Bash".into(),
            arguments,
        };
        assert!(matches!(
            publish(&volume, &request, task.clone()).await,
            Err(Error::LimitExceeded)
        ));
        assert!(matches!(
            ExternalRequest::from_file(&task, &file(&task, &request)?),
            Err(Error::InvalidSegment)
        ));
    }
    let unchanged = volume.snapshot(&query, 1).await?;
    assert_eq!(unchanged.files, initial.files);
    assert_eq!(unchanged.cursor, initial.cursor);
    assert_eq!(unchanged.generation, initial.generation);
    Ok(())
}

#[tokio::test]
async fn admitted_large_nested_requests_replay_through_the_same_decoder() -> Result {
    let volume = volume().await?;
    let task = Component::new("task")?;
    let query = Query::Requests(task.clone());
    let mut arguments = Value::Null;
    for _ in 0..200 {
        arguments = Value::Array(vec![arguments]);
    }
    let mut request = tool()?;
    request.action = ExternalAction::RunTool {
        call_id: "call".into(),
        name: "Bash".into(),
        arguments: json!({
            "large": "x".repeat(acyclic_stream::MAX_RECORD_BYTES - 1024),
            "nested": arguments,
        }),
    };
    let submission = publish(&volume, &request, task.clone());
    assert!(std::mem::size_of_val(&submission) < 16 * 1024);
    submission.await?;
    let snapshot = volume.snapshot(&query, 1).await?;
    assert_eq!(snapshot.files.len(), 1);
    let stored = snapshot.files.first().expect("request");
    assert_eq!(ExternalRequest::from_file(&task, stored)?, request);
    Ok(())
}
