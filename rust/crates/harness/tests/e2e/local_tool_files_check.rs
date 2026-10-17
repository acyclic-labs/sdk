//! Pure identity, payload and output verification shared with the fuzz target.
use crate::TestResult;
use acyclic_harness::{
    OperationId, TaskId,
    model::ToolCall,
    task_volume::{Component, FileKey, HeadFile, TaskFile, ToolFile},
    tool::ToolInvocation,
};
use serde_json::Value;
use std::collections::BTreeMap;

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
