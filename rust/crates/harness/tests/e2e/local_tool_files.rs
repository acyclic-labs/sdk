//! Consume the production shell request/result/output files, without a provider wrapper.
use super::{TestResult, local_tool_files_check::check, quiescent};
use acyclic_harness::{
    LocalHarness, OperationId, TaskId,
    client::FileQueryView,
    model::{ModelEvent, ToolCall},
    task_volume::{Component, FileKey, Observation, Query, TaskFile, ToolFile},
    test_support::{ScriptedEvent, ScriptedTurn},
};
use futures::StreamExt as _;
use serde_json::Value;
use std::collections::BTreeMap;

pub(super) fn calls(turns: &[ScriptedTurn]) -> Vec<(u64, ToolCall)> {
    turns
        .iter()
        .enumerate()
        .flat_map(|(step, turn)| {
            let events = match turn {
                ScriptedTurn::Events(events) => events.as_slice(),
                _ => &[],
            };
            events.iter().filter_map(move |event| match event {
                ScriptedEvent::Event(ModelEvent::ToolCall(call)) if call.name == "bash" => {
                    Some((step as u64, call.clone()))
                }
                _ => None,
            })
        })
        .collect()
}

fn query(task: TaskId) -> TestResult<Query> {
    Ok(Query::Turns(Component::new(task.to_string())?))
}

pub(super) async fn observe(
    harness: &LocalHarness,
    task: TaskId,
    calls: &[(u64, ToolCall)],
) -> TestResult<Option<Observation>> {
    if calls.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        harness.task_files().observe(query(task)?, None, 64).await?,
    ))
}

pub(super) async fn verify(
    harness: &LocalHarness,
    task: TaskId,
    operation: OperationId,
    mut observation: Observation,
    calls: &[(u64, ToolCall)],
    results: &BTreeMap<String, Value>,
) -> TestResult {
    let selected = query(task)?;
    if observation.reset {
        return Err("initial tool query unexpectedly reset".into());
    }
    let initial = observation.snapshot.take().ok_or("missing tool snapshot")?;
    let target = harness
        .task_files()
        .observe(selected.clone(), None, 64)
        .await?
        .snapshot
        .ok_or("missing final tool snapshot")?
        .into_heads()?;
    let mut view = FileQueryView::from_snapshot(selected, initial, 64, 65_536)?;
    while view.cursor().position != target.cursor.position {
        let change = observation
            .changes
            .next()
            .await
            .ok_or("tool follow ended")??;
        if change.cursor.position <= view.cursor().position
            || change.cursor.position > target.cursor.position
        {
            return Err("tool follow did not advance to the known cut".into());
        }
        view.apply(change)?;
    }
    if view.cursor().commit != target.cursor.commit || view.files().values().ne(target.files.iter())
    {
        return Err("tool snapshot and follow disagree".into());
    }
    let mut next = std::pin::pin!(observation.changes.next());
    if quiescent::until_idle(next.as_mut()).await.is_some() {
        return Err("tool follow delivered beyond its settled cut".into());
    }
    check(view.files(), task, operation, calls, results)?;
    // Break the consumer's real observed files, retaining the fixture oracle.
    let mut dropped = view.files().clone();
    let result = dropped
        .keys()
        .find(|key| {
            matches!(
                key,
                FileKey::Task {
                    file: TaskFile::Tool {
                        file: ToolFile::Result,
                        ..
                    },
                    ..
                }
            )
        })
        .cloned()
        .ok_or("missing observed bash result")?;
    dropped.remove(&result);
    if check(&dropped, task, operation, calls, results).is_ok() {
        return Err("task-file oracle accepted a dropped result publication".into());
    }
    Ok(())
}
