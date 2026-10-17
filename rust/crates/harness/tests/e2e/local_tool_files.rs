//! Consume the production shell request/result/output files, without a provider wrapper.
use super::{TestResult, quiescent};
use acyclic_harness::{
    LocalHarness, OperationId, TaskId,
    client::FileQueryView,
    model::{ModelEvent, ToolCall},
    task_volume::{Component, FileKey, HeadFile, Observation, Query, TaskFile, ToolFile},
    test_support::{ScriptedEvent, ScriptedTurn},
    tool::ToolInvocation,
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

pub(super) fn check(
    files: &BTreeMap<FileKey, HeadFile>,
    task: TaskId,
    operation: OperationId,
    calls: &[(u64, ToolCall)],
    results: &BTreeMap<String, Value>,
) -> TestResult {
    let task = Component::new(task.to_string())?;
    let mut expected = BTreeMap::new();
    for (step, call) in calls {
        let invocation = ToolInvocation::for_model_call(
            operation,
            u32::try_from(*step)?,
            call.call_id.clone(),
            call.name.clone(),
            call.arguments.clone(),
            None,
        )?;
        let identity = Component::new(invocation.operation_id.to_string())?;
        let result = results
            .get(&call.call_id)
            .ok_or("missing expected bash result")?;
        let value = result["content"]
            .get("value")
            .ok_or("missing expected bash value")?;
        if expected
            .insert(
                (*step, identity),
                (serde_json::to_value(invocation)?, value),
            )
            .is_some()
        {
            return Err("duplicate expected bash identity".into());
        }
    }
    let mut requests = 0;
    let mut completed = 0;
    let mut output: BTreeMap<_, (u64, Vec<u8>)> = BTreeMap::new();
    for (key, value) in files {
        if key != &value.key {
            return Err("task tool map and file identities differ".into());
        }
        let FileKey::Task {
            task: owner,
            file: TaskFile::Tool { turn, call, file },
        } = key
        else {
            continue;
        };
        let (request, result) = expected
            .get(&(*turn, call.clone()))
            .ok_or("unexpected task tool identity")?;
        if owner != &task || value.channel.is_some() {
            return Err("task tool file lost its owning task or channel".into());
        }
        let [bytes] = value.segments.as_slice() else {
            return Err("tool file must retain its single immutable segment".into());
        };
        if value.length != bytes.len() as u64 {
            return Err("tool file length differs from its segment".into());
        }
        match file {
            ToolFile::Request => {
                if serde_json::from_slice::<Value>(bytes)? != *request {
                    return Err("published tool request payload differs".into());
                }
                requests += 1;
            }
            ToolFile::Result => {
                if serde_json::from_slice::<Value>(bytes)? != **result {
                    return Err("published tool result differs".into());
                }
                completed += 1;
            }
            ToolFile::Stdout(sequence) | ToolFile::Stderr(sequence) => {
                if bytes.is_empty() {
                    return Err("empty task output publication".into());
                }
                let channel = if matches!(file, ToolFile::Stdout(_)) {
                    "stdout"
                } else {
                    "stderr"
                };
                let (next, assembled) = output.entry((*turn, call.clone(), channel)).or_default();
                if *sequence != *next {
                    return Err("task output sequence has a gap".into());
                }
                *next += 1;
                assembled.extend_from_slice(bytes);
            }
            ToolFile::Arguments(_) => return Err("unexpected bash argument fragment".into()),
        }
    }
    if requests != expected.len() || completed != expected.len() {
        return Err("missing published tool request or result".into());
    }
    for ((step, call), (_, result)) in expected {
        for channel in ["stdout", "stderr"] {
            let expected = result[channel]
                .as_str()
                .ok_or("missing expected bash output")?;
            let actual = output
                .remove(&(step, call.clone(), channel))
                .unwrap_or_default()
                .1;
            if actual != expected.as_bytes() {
                return Err("published task output bytes differ".into());
            }
        }
    }
    Ok(())
}
