use super::{TestResult, json_matchers, provider::Recording, wire};
use acyclic_harness::{
    OperationId,
    client::jsonl,
    task_volume::{FileKey, TaskFile},
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, process::Output};

pub fn last_line(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .lines()
        .last()
        .unwrap_or("(empty)")
        .to_owned()
}

pub fn output(
    case: &Value,
    operation: OperationId,
    output: &Output,
    replay: bool,
) -> TestResult<Vec<Value>> {
    let indeterminate =
        case["expect"]["outcome"] == "indeterminate" || case["expect"]["indeterminate"] == true;
    let expected_code =
        case["expect"]["exit"]
            .as_i64()
            .unwrap_or(if indeterminate { 4 } else { 0 });
    if output.status.code().map(i64::from) != Some(expected_code) {
        return Err(format!(
            "exit {:?}, expected {expected_code}\nstdout: {}\nstderr: {}",
            output.status.code(),
            last_line(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    let text = std::str::from_utf8(&output.stdout)?;
    let mut lines = text.lines();
    let header_line = lines.next().ok_or("no JSONL header")?;
    let header: Value = serde_json::from_str(header_line)?;
    if header[0]["TaskSession"]["session"] != operation.to_string()
        || header[0]["TaskSession"]["task"] != operation.to_string()
    {
        return Err(format!("wrong session header: {header}").into());
    }
    let mut results = Vec::new();
    let mut statuses = Vec::new();
    let mut files = Vec::new();
    let (_, snapshot) = jsonl::parse_snapshot(header_line)?;
    let mut previous = snapshot.cursor;
    for file in snapshot.files {
        let bytes: Vec<_> = file
            .segments
            .iter()
            .flat_map(|bytes| bytes.iter().copied())
            .collect();
        observe(file.key, &bytes, &mut results, &mut statuses, &mut files)?;
    }
    for line in lines {
        let change = jsonl::parse_change(line)?;
        if change.cursor.volume != previous.volume
            || change.cursor.epoch != previous.epoch
            || change.cursor.schema != previous.schema
            || change.cursor.query != previous.query
            || change.cursor.position <= previous.position
        {
            return Err("JSONL follow reset or repeated its cursor".into());
        }
        previous = change.cursor;
        for file in change.file.into_iter().chain(change.files) {
            observe(
                file.key,
                &file.bytes,
                &mut results,
                &mut statuses,
                &mut files,
            )?;
        }
    }
    if results.len() != 1 {
        return Err(format!("expected exactly one JSONL execution result, got {results:?}").into());
    }
    let expected = if let Some(result) = case["expect"].get("result") {
        result.clone()
    } else if indeterminate {
        json!({"completion":"indeterminate"})
    } else {
        json!({"completion":"completed","ok":{"text":case["expect"]["text"]}})
    };
    if !json_matchers::subset(&expected, &results[0]) {
        return Err(format!(
            "JSONL result differs: expected {expected}, got {}",
            results[0]
        )
        .into());
    }
    if let Some(expected) = case["expect"]["statuses"].as_array() {
        let expected = if replay {
            &expected[expected.len().saturating_sub(1)..]
        } else {
            expected
        };
        json_matchers::ordered(expected, &statuses, "JSONL task statuses")?;
    }
    Ok(files)
}

fn observe(
    key: FileKey,
    bytes: &[u8],
    results: &mut Vec<Value>,
    statuses: &mut Vec<Value>,
    files: &mut Vec<Value>,
) -> TestResult {
    if matches!(
        key,
        FileKey::Session {
            file: TaskFile::ExecutionResult(_),
            ..
        }
    ) {
        results.push(serde_json::from_slice::<Value>(bytes)?);
    }
    if matches!(
        key,
        FileKey::Task {
            file: TaskFile::Status,
            ..
        }
    ) {
        statuses.push(serde_json::from_slice::<Value>(bytes)?);
    }
    files.push(json!({"path":key.path(), "bytes":bytes}));
    Ok(())
}

pub fn recording(case: &Value, protocol: &str, recording: &Recording) -> TestResult {
    let faults = case.get("faults").cloned().unwrap_or_else(|| json!([]));
    if recording.remaining_faults != 0 || serde_json::to_value(&recording.injected)? != faults {
        return Err("scripted HTTP faults were skipped or delivered out of order".into());
    }
    let actions = case["actions"].as_array().map_or(0, Vec::len);
    if recording.actions_applied != actions || !recording.actions_remaining.is_empty() {
        return Err(format!(
            "expected {actions} child file actions, applied {}, remaining {:?}",
            recording.actions_applied, recording.actions_remaining
        )
        .into());
    }
    super::history::check(case, &recording.requests, protocol)?;
    for text in case["expect"]["request_text"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let text = text.as_str().ok_or("request_text must contain strings")?;
        if !recording
            .requests
            .values()
            .flatten()
            .any(|body| json_matchers::subset(&json!({"$contains":text}), body))
        {
            return Err(format!("HTTP model requests omitted {text:?}").into());
        }
    }
    for (name, remaining) in &recording.remaining {
        if *remaining != 0 {
            return Err(format!("{name}: {remaining} unused HTTP replies").into());
        }
    }
    for (name, requests) in &recording.requests {
        let expected = case["expect"]["requests"]
            .get(name)
            .and_then(Value::as_u64)
            .or_else(|| case["expect"]["request_count"].as_u64())
            .or_else(|| {
                case["models"]
                    .get(name)
                    .and_then(Value::as_array)
                    .map(|turns| turns.len() as u64)
            })
            .or_else(|| {
                case.get("responses")
                    .or_else(|| case.get("turns"))
                    .and_then(Value::as_array)
                    .map(|turns| turns.len() as u64)
            });
        if let Some(expected) = expected
            && requests.len() as u64 != expected
        {
            return Err(format!(
                "{name}: expected {expected} HTTP requests, got {}",
                requests.len()
            )
            .into());
        }
        if let Some(expected) = case["expect"]["toolsets"][name].as_array() {
            let mut expected = expected.clone();
            expected.sort_by_key(Value::to_string);
            for request in requests {
                let mut actual: Vec<_> = request["tools"]
                    .as_array()
                    .ok_or("HTTP tools missing")?
                    .iter()
                    .map(|tool| tool["name"].clone())
                    .collect();
                actual.sort_by_key(Value::to_string);
                if actual != expected {
                    return Err(format!(
                        "HTTP toolset {name}: expected {expected:?}, got {actual:?}"
                    )
                    .into());
                }
            }
        }
        let adapter = match protocol {
            "anthropic" => acyclic_harness_providers::Adapter::Anthropic,
            "responses" => acyclic_harness_providers::Adapter::Responses,
            "chat" => acyclic_harness_providers::Adapter::Chat,
            "gemini" => acyclic_harness_providers::Adapter::Gemini,
            _ => return Err(format!("unsupported HTTP protocol: {protocol}").into()),
        };
        for request in requests {
            let violations = acyclic_harness_providers::validate_input(adapter, request);
            if !violations.is_empty() {
                return Err(
                    format!("invalid {protocol} HTTP request: {violations:?}: {request}").into(),
                );
            }
            if protocol != "gemini" && request["stream"] != true {
                return Err("HTTP request did not select streaming".into());
            }
        }
        if case["driver"] == "provider" {
            let endpoint = case["expect"]["endpoint"]
                .as_str()
                .ok_or("provider endpoint expectation missing")?;
            let endpoints = &recording.endpoints[name];
            if endpoints.len() != requests.len()
                || endpoints.iter().any(|actual| actual != endpoint)
            {
                return Err(format!(
                    "HTTP endpoint differs: expected {endpoint}, got {endpoints:?}"
                )
                .into());
            }
            let expected = case["expect"]["requests"]
                .as_array()
                .ok_or("provider request expectations missing")?;
            if expected.len() != requests.len() {
                return Err("provider request count differs".into());
            }
            for (expected, actual) in expected.iter().zip(requests) {
                for (pointer, expected) in expected
                    .as_object()
                    .ok_or("provider request assertion missing")?
                {
                    if !actual
                        .pointer(pointer)
                        .is_some_and(|actual| json_matchers::subset(expected, actual))
                    {
                        return Err(format!(
                            "HTTP request {pointer}: expected {expected}, got {actual}"
                        )
                        .into());
                    }
                }
            }
        }
        if name == "parent"
            && let Some(expected) = case["expect"]["results"].as_object()
        {
            let request = requests.last().ok_or("no final HTTP request")?;
            let outputs = wire::unique_outputs(
                wire::outputs(request, protocol)?
                    .into_iter()
                    .filter(|(id, _)| !id.starts_with("binary_seed_")),
            )?;
            if expected.keys().ne(outputs.keys()) {
                return Err(format!(
                    "HTTP tool result identities differ: expected {expected:?}, got {outputs:?}"
                )
                .into());
            }
            for (call, expected) in expected {
                if let Some(content) = expected.get("content") {
                    let actual = if content["kind"] == "parts" {
                        json!({"kind":"parts","parts":[{"kind":"text","text":outputs[call]}]})
                    } else {
                        json!({"kind":"json","value":outputs[call]})
                    };
                    if !json_matchers::subset(content, &actual) {
                        return Err(format!(
                            "HTTP tool result {call}: expected {content}, got {actual}"
                        )
                        .into());
                    }
                }
            }
        }
    }
    if let Some(expected) = case["expect"]["task_files"].as_object() {
        let audits = recording
            .audits
            .get("parent")
            .ok_or("missing parent HTTP audit")?;
        if audits.len() != 1 {
            return Err("child files were not inspected through the shipped fs tool".into());
        }
        let outputs = wire::unique_outputs(
            wire::outputs(&audits[0], protocol)?
                .into_iter()
                .filter(|(id, _)| id.starts_with("binary_audit_") || id == "binary_active_list"),
        )?;
        for (index, (path, expected)) in expected.iter().enumerate() {
            let id = format!("binary_audit_{index}");
            let result = outputs
                .get(&id)
                .ok_or_else(|| format!("missing child file result {path}"))?;
            let actual: Value = serde_json::from_str(
                result["stdout"]
                    .as_str()
                    .ok_or("child fs read returned no stdout")?,
            )?;
            if result["success"] != true || !json_matchers::subset(expected, &actual) {
                return Err(format!("child {path}: expected {expected}, got {actual}").into());
            }
        }
        if let Some(expected_active) = case["expect"].get("active") {
            let mut expected_active: Vec<String> = serde_json::from_value(expected_active.clone())?;
            expected_active.sort();
            let active = active_children(expected, &outputs)?;
            if active != expected_active {
                return Err(format!(
                    "active children: expected {expected_active:?}, got {active:?}"
                )
                .into());
            }
        }
    }
    for (name, attempts) in &recording.attempts {
        let turns = case["models"].get(name).or_else(|| case.get("turns"));
        let prepares = usize::from(
            turns
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_array)
                .flatten()
                .any(|event| {
                    event["arguments"]["file"].is_string()
                        && event["name"].as_str().is_some_and(|name| {
                            matches!(name.split('@').next(), Some("read" | "edit"))
                        })
                }),
        );
        let expected = recording.requests[name].len() + prepares + recording.audits[name].len();
        if *attempts != expected {
            return Err(
                format!("{name}: expected {expected} total HTTP attempts, got {attempts}").into(),
            );
        }
    }
    Ok(())
}

fn active_children(
    expected_files: &serde_json::Map<String, Value>,
    outputs: &BTreeMap<String, Value>,
) -> TestResult<Vec<String>> {
    use acyclic_harness::task_volume::{ChildFile, ChildState, ChildStatus};
    use std::collections::BTreeSet;
    let listed = outputs
        .get("binary_active_list")
        .ok_or("missing shipped child listing")?;
    if listed["success"] != true {
        return Err("shipped child listing failed".into());
    }
    let mut statuses = BTreeSet::new();
    let mut paths = BTreeSet::new();
    for line in listed["stdout"]
        .as_str()
        .ok_or("child listing omitted stdout")?
        .lines()
    {
        let path = line
            .strip_prefix(".volumes/")
            .ok_or("child listing escaped its volume")?;
        let key = FileKey::parse(path)?;
        if !paths.insert(path.to_owned()) {
            return Err("child listing repeated a file".into());
        }
        match key {
            FileKey::Child {
                file: ChildFile::Status,
                ..
            } => {
                statuses.insert(path.to_owned());
            }
            FileKey::Child { .. } => {}
            _ => return Err("child listing contained another namespace".into()),
        }
    }
    let known: BTreeSet<_> = expected_files
        .keys()
        .filter(|path| path.ends_with("/status.json"))
        .cloned()
        .collect();
    if known != statuses {
        return Err(format!(
            "child status membership differs: expected {known:?}, got {statuses:?}"
        )
        .into());
    }
    let mut active = Vec::new();
    for (index, path) in expected_files.keys().enumerate() {
        if !statuses.contains(path) {
            continue;
        }
        let result = outputs
            .get(&format!("binary_audit_{index}"))
            .ok_or("missing child status read")?;
        let status = ChildStatus::parse(
            result["stdout"]
                .as_str()
                .ok_or("child status omitted stdout")?
                .as_bytes(),
        )?;
        if !status.settled || !matches!(status.state, ChildState::Cancelled | ChildState::Archived)
        {
            let FileKey::Child { reference, .. } = FileKey::parse(path)? else {
                return Err("invalid child status path".into());
            };
            active.push(reference.as_str().to_owned());
        }
    }
    active.sort();
    Ok(active)
}

#[cfg(test)]
mod active_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn active_child_oracle_checks_lifecycle_and_complete_membership(name in "[a-z]{1,12}", state in 0usize..5) {
            let state_name = ["Running", "Paused", "Interrupted", "Cancelled", "Archived"][state];
            let path = format!("agents/{name}/status.json");
            let expected = serde_json::Map::from_iter([(path.clone(), json!({"state":state_name}))]);
            let status = json!({"task":acyclic_harness::TaskId::new(),"state":state_name,"settled":true,"inputs":1});
            let mut outputs = BTreeMap::from([
                ("binary_active_list".into(), json!({"success":true,"stdout":format!(".volumes/{path}\n")})),
                ("binary_audit_0".into(), json!({"success":true,"stdout":serde_json::to_string(&status)?})),
            ]);
            let actual = active_children(&expected, &outputs).map_err(|error| TestCaseError::fail(error.to_string()))?;
            prop_assert_eq!(actual, if state < 3 { vec![name.clone()] } else { Vec::new() });
            outputs.get_mut("binary_active_list").unwrap()["stdout"] = "".into();
            prop_assert!(active_children(&expected, &outputs).is_err(), "lost child membership passed");
            outputs.get_mut("binary_active_list").unwrap()["stdout"] = format!(".volumes/{path}\n.volumes/agents/extra/status.json\n").into();
            prop_assert!(active_children(&expected, &outputs).is_err(), "extra child membership passed");
        }
    }
}

pub fn files(case: &Value, workspace: &Path) -> TestResult {
    let mut observed = BTreeMap::new();
    for (path, expected) in case["expect"]["files"].as_object().into_iter().flatten() {
        let actual = match std::fs::read(workspace.join(path)) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        if actual.as_deref() != expected.as_str().map(str::as_bytes) {
            return Err(format!("workspace {path}: expected {expected}, got {actual:?}").into());
        }
        observed.insert(path.clone(), actual);
    }
    if let Some(expected) = case["expect"]["diffs"].as_object() {
        let mut paths: std::collections::BTreeSet<_> = observed.keys().cloned().collect();
        paths.extend(
            case["seed"]
                .as_object()
                .into_iter()
                .flatten()
                .map(|(path, _)| path.clone()),
        );
        let mut actual = serde_json::Map::new();
        for path in paths {
            let base = case["seed"][&path].as_str().map(str::as_bytes);
            let current = match observed.get(&path) {
                Some(bytes) => bytes.clone(),
                None => match std::fs::read(workspace.join(&path)) {
                    Ok(bytes) => Some(bytes),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                    Err(error) => return Err(error.into()),
                },
            };
            if let Some(diff) = acyclic_harness::client::FileDiff::between(
                base,
                current.as_deref(),
                65_536,
                65_536,
            )? {
                actual.insert(path, serde_json::to_value(diff)?);
            }
        }
        if &actual != expected {
            return Err(
                format!("checkout diffs differ: expected {expected:?}, got {actual:?}").into(),
            );
        }
    }
    Ok(())
}
