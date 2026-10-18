use super::{TestResult, matchers, quiescent};
use acyclic_harness::{
    LocalHarness, OperationId, TaskId,
    client::{CommandResolution, CommandSession, consume_local_command},
    conversation::ContentResidencyVerifier,
    task_volume::{Component, FileKey, Observation, Query, TaskFile},
};
use futures::StreamExt as _;
use serde_json::Value;

pub(super) async fn submit(
    harness: &LocalHarness,
    prompt: &str,
    reader: &dyn ContentResidencyVerifier,
) -> TestResult<(OperationId, TaskId, Observation)> {
    let session = uuid::Uuid::new_v4();
    let mut client = CommandSession::open_local(harness, session, 65_536, 128).await?;
    let operation = client.submit(prompt, reader).await?;
    let request = client.next_request().await?;
    let key = FileKey::Session {
        session: Component::new(session.to_string())?,
        file: TaskFile::Request(Component::new(format!("command-{session}.{operation}"))?),
    };
    let resolution: CommandResolution = serde_json::from_slice(&request.bytes)?;
    if request.key != key
        || request.channel.is_some()
        || resolution
            != (CommandResolution::Prompt {
                text: prompt.into(),
                source: None,
                mentions: vec![],
            })
    {
        return Err("published request identity or prompt differs".into());
    }
    let result = consume_local_command(&mut client, harness, &request)
        .await?
        .ok_or("request file was not consumed")?;
    let task = serde_json::from_value(result["task"].clone())?;
    if result["admission_operation"] != serde_json::to_value(operation)?
        || client.selected_task()? != Some(task)
    {
        return Err("request result lost its admitted operation or task".into());
    }
    let observation = harness
        .task_files()
        .observe(Query::File(status_key(task)?), None, 1)
        .await?;
    Ok((operation, task, observation))
}

fn status_key(task: TaskId) -> TestResult<FileKey> {
    Ok(FileKey::Task {
        task: Component::new(task.to_string())?,
        file: TaskFile::Status,
    })
}

pub(super) async fn settled(
    harness: &LocalHarness,
    task: TaskId,
    mut observation: Observation,
    expected: &[Value],
) -> TestResult {
    let initial = observation
        .snapshot
        .take()
        .ok_or("missing admission snapshot")?;
    let key = status_key(task)?;
    let target = harness
        .task_files()
        .observe(Query::File(key.clone()), None, 1)
        .await?
        .snapshot
        .ok_or("missing settled snapshot")?;
    if observation.reset
        || initial.files.len() != 1
        || target.files.len() != 1
        || initial
            .files
            .iter()
            .chain(&target.files)
            .any(|file| file.key != key || file.channel.is_some())
    {
        return Err("admitted or settled status file is missing".into());
    }
    let mut statuses = vec![serde_json::from_slice(&initial.files[0].bytes)?];
    let mut cursor = initial.cursor;
    while cursor.position != target.cursor.position {
        let change = super::stall::waiting(
            "status follow before the settled cut",
            observation.changes.next(),
        )
        .await
        .ok_or("status follow ended")??;
        if change.cursor.volume != cursor.volume
            || change.cursor.epoch != cursor.epoch
            || change.cursor.query != cursor.query
            || change.cursor.schema != cursor.schema
            || change.cursor.durable > change.cursor.accepted
            || change.cursor.accepted < change.cursor.position
            || change.cursor.position <= cursor.position
            || change.cursor.position > target.cursor.position
            || !change.removed.is_empty()
        {
            return Err("status follow lost its query identity or accepted cut".into());
        }
        cursor = change.cursor;
        for file in change.file.iter().chain(&change.files) {
            if file.key != key || file.channel.is_some() {
                return Err("status query delivered another file or channel".into());
            }
            statuses.push(serde_json::from_slice(&file.bytes)?);
        }
    }
    if cursor.commit != target.cursor.commit
        || statuses.last() != Some(&serde_json::from_slice(&target.files[0].bytes)?)
    {
        return Err("status follow and final snapshot disagree".into());
    }
    let mut next = std::pin::pin!(observation.changes.next());
    if quiescent::until_idle(next.as_mut()).await.is_some() {
        return Err("status follow delivered beyond the settled cut".into());
    }
    check_statuses(expected, &statuses, task)?;
    // Fault the consumer of real published status files, keeping data unchanged.
    if check_statuses(expected, &statuses[..statuses.len() - 1], task).is_ok() {
        return Err("status oracle accepted a dropped terminal publication".into());
    }
    Ok(())
}

pub(super) fn check_statuses(expected: &[Value], actual: &[Value], task: TaskId) -> TestResult {
    let identity = serde_json::to_value(task)?;
    if expected.len() < 2
        || expected.len() != actual.len()
        || expected.iter().zip(actual).any(|(expected, actual)| {
            actual["task"] != identity || !matchers::subset(expected, actual)
        })
    {
        return Err(
            format!("task status files differ: expected {expected:?}, got {actual:?}").into(),
        );
    }
    Ok(())
}
