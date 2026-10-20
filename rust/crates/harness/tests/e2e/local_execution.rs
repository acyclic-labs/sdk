use crate::{
    TestResult,
    frame_oracle::check_frames,
    local_case::{ExpectedOutcome, LocalCase},
    local_workspace, matchers,
};
use acyclic_fs::performance::BOUNDARY_BUDGET;
use acyclic_harness::{
    LocalHarness,
    client::{DiffLimits, DiffPath, DiffPublication, DiffView},
    context::{CompactionPolicy, ContextRoot, PinnedContextPath},
    conversation::{FileDescriptor, FileRef},
    model::{Model, ModelContent, ModelContentPart, ModelEvent, ModelRequest},
    native_settings::SettingsConfig,
    task_volume::{FileQuerySource, Query},
    test_support::{
        ScriptedEvent, ScriptedModelProvider, ScriptedTurn, local_harness_config, local_task_stream,
    },
};
use acyclic_stream::{StreamPath, StreamProvider as _};
use futures::StreamExt as _;
use prost::Message as _;
use serde_json::Value;
use std::sync::Arc;

pub(super) async fn run(case: LocalCase) -> TestResult {
    case.validate()?;
    let steps = case.turns.len();
    let temporary = local_workspace::temporary()?;
    let directory = temporary.path().join("filesystem");
    std::fs::create_dir(&directory)?;
    let state = temporary.path().join("state");
    let (host, volume, workspace, reader) = local_workspace::seed(&directory, &case.seed).await?;
    let model = Model::new("scripted".into(), "local".into(), "1".into(), Value::Null)?;
    let provider = Arc::new(ScriptedModelProvider::new([]));
    let home = temporary.path().join("home");
    for (path, text) in &case.home {
        let path = home.join(path);
        std::fs::create_dir_all(path.parent().ok_or("home file has no parent")?)?;
        match text {
            Value::String(text) => std::fs::write(path, text)?,
            value => std::fs::write(path, serde_json::to_vec(value)?)?,
        }
    }
    let config = local_harness_config(&directory, (model, provider.clone()))?
        .settings(SettingsConfig::new(&directory, &home).codex_home(home.join(".codex")))
        .state_directory(&state)
        .volume(volume.clone())
        .compaction(CompactionPolicy::Disabled)
        .goal_continuations(None);
    let harness = LocalHarness::open(config).await?;
    if !state.join("owner.json").is_file() {
        return Err("harness did not use the fixture-owned state directory".into());
    }
    // Initialization binds configured auxiliary directories. Select the command's
    // diff cut and pinned files from that initialized workspace, before dispatch.
    let source = reader.file_query(&volume, BOUNDARY_BUDGET)?;
    let mut observation = source.observe_files(Query::Plans, None, 16, 65_536).await?;
    let initial = observation.snapshot.take().ok_or("missing diff snapshot")?;
    let base = host.resolve(&workspace, None).await?.generation;
    let mut references = std::collections::BTreeMap::new();
    let paths = case
        .turns
        .iter()
        .filter_map(|turn| match turn {
            ScriptedTurn::Events(events) => Some(events),
            _ => None,
        })
        .flatten()
        .filter_map(|event| match event {
            ScriptedEvent::Event(ModelEvent::ToolCall(call))
                if matches!(call.name.split('@').next(), Some("read" | "edit")) =>
            {
                call.arguments.get("file").and_then(Value::as_str)
            }
            _ => None,
        });
    for path in paths {
        if let std::collections::btree_map::Entry::Vacant(entry) = references.entry(path.to_owned())
        {
            let text = case
                .seed
                .get(path)
                .ok_or("file recipe does not name a seeded file")?;
            let file = FileRef::new(
                volume.clone(),
                path.into(),
                hex::encode(base.as_resource().key()),
                FileDescriptor::from_bytes(text.as_bytes(), "text/plain", &mut None)?,
                path.rsplit('/').next().ok_or("seed filename")?.into(),
                &mut None,
            )?;
            entry.insert(serde_json::to_value(file)?);
        }
    }
    let turns = crate::local_case::bind_seed_files(case.turns, &references)?;
    let mut scripted_calls = crate::local_tool_files::calls(&turns);
    for (call_id, arguments) in &case.expect.arguments {
        let (_, call) = scripted_calls
            .iter_mut()
            .find(|(_, call)| &call.call_id == call_id)
            .ok_or("rewritten arguments name no scripted bash call")?;
        call.arguments = arguments.clone();
    }
    // A refused call never ran, so it publishes no tool request or result files.
    scripted_calls.retain(|(_, call)| {
        case.expect.results.as_ref().is_none_or(|results| {
            results
                .get(&call.call_id)
                .is_none_or(|result| result["content"]["kind"] != "error")
        })
    });
    provider.extend(turns);
    let stream = local_task_stream(&harness);
    let (operation, task_id, status) =
        crate::local_admission::submit(&harness, &case.prompt, reader.as_ref()).await?;
    let tool_files = crate::local_tool_files::observe(&harness, task_id, &scripted_calls).await?;
    let mut frames = stream
        .follow(StreamPath::new("harness/v3/coordinator/events")?, 0)
        .await?;
    let output = harness.resume(task_id).await;
    crate::local_admission::settled(&harness, task_id, status, &case.expect.statuses).await?;
    match (case.expect.outcome, output) {
        (ExpectedOutcome::Succeeded, Ok(output))
            if Some(&output.text) == case.expect.text.as_ref() => {}
        (ExpectedOutcome::Failed, Err(acyclic_harness::Error::TaskFailed { task, .. }))
            if task == task_id && case.expect.error.is_none() => {}
        // A local turn is uncertain only through a command, so the caller sees its cause.
        (
            ExpectedOutcome::Indeterminate,
            Err(acyclic_harness::Error::CommandIndeterminate {
                operation_id: id, ..
            }),
        ) if id == operation => {}
        (ExpectedOutcome::Failed, Err(error))
            if case
                .expect
                .error
                .as_deref()
                .is_some_and(|expected| error.to_string().contains(expected)) => {}
        (_, actual) => return Err(format!("local turn outcome differs: {actual:?}").into()),
    }
    if provider.remaining() != 0 {
        return Err("local model script was not consumed".into());
    }
    let requests = provider.requests();
    if requests.len() != steps {
        return Err("local scenario did not consume exactly its scripted model turns".into());
    }
    check_requests(&case.expect, &requests)?;
    if let Some(expected) = &case.expect.results {
        let actual = model_results(requests.last().ok_or("missing model request")?)?;
        check_results(expected, &actual)?;
        // Corrupt the consumer's real captured model history, not expectations.
        for call in expected.keys() {
            let mut missing = actual.clone();
            missing.remove(call);
            if check_results(expected, &missing).is_ok() {
                return Err("model oracle accepted a lost tool result".into());
            }
            for field in ["name", "content"] {
                let mut broken = actual.clone();
                broken.get_mut(call).ok_or("missing result")?[field] = "CORRUPTED".into();
                if check_results(expected, &broken).is_ok() {
                    return Err("model oracle accepted a corrupted tool result".into());
                }
            }
        }
    }
    if let Some(observation) = tool_files {
        let dispatches = provider.dispatches();
        let execution = dispatches
            .first()
            .ok_or("missing model dispatch")?
            .operation_id;
        if dispatches.len() != steps
            || dispatches.iter().enumerate().any(|(step, dispatch)| {
                dispatch.operation_id != execution || dispatch.step as usize != step
            })
        {
            return Err("local model execution or step binding differs".into());
        }
        crate::local_tool_files::verify(
            &harness,
            task_id,
            execution,
            observation,
            &scripted_calls,
            case.expect
                .results
                .as_ref()
                .ok_or("bash results are required")?,
        )
        .await?;
    }
    let path = StreamPath::new("harness/v3/coordinator/events")?;
    let cut = stream.tail(path).await?;
    let mut wire_frames = Vec::new();
    for sequence in 0..cut {
        let record = crate::stall::waiting("scheduler wire replay", frames.next())
            .await
            .ok_or("scheduler wire ended")??;
        if record.sequence != sequence {
            return Err("scheduler wire has a sequence gap".into());
        }
        let frame = acyclic_harness::wire::SchedulerEventEnvelope::decode(record.value.as_ref())?;
        if frame.revision != sequence + 1 {
            return Err("scheduler wire has a revision gap".into());
        }
        wire_frames.push(serde_json::from_slice(&frame.canonical_event_json)?);
    }
    check_frames(
        &case.expect.frames,
        &wire_frames,
        operation,
        case.expect.outcome,
    )?;
    let root = serde_json::to_value(operation)?;
    let terminal = wire_frames
        .iter()
        .position(|frame| frame["kind"] == "completed" && frame["operation_id"] == root)
        .ok_or("missing terminal")?;
    wire_frames.remove(terminal);
    if check_frames(
        &case.expect.frames,
        &wire_frames,
        operation,
        case.expect.outcome,
    )
    .is_ok()
    {
        return Err("wire oracle accepted a lost root terminal frame".into());
    }
    let target = source
        .observe_files(Query::Plans, None, 16, 65_536)
        .await?
        .snapshot
        .ok_or("missing final diff snapshot")?;
    let mut wire = Vec::new();
    let mut seen = initial.cursor.position;
    let mut paths = std::collections::BTreeSet::new();
    let mut removed = std::collections::BTreeSet::new();
    while seen != target.cursor.position {
        let change = crate::stall::waiting(
            "workspace follow before the settled cut",
            observation.changes.next(),
        )
        .await
        .ok_or("diff follow ended")??;
        if change.cursor.position <= seen || change.cursor.position > target.cursor.position {
            return Err("diff publication cursor did not advance to the accepted cut".into());
        }
        seen = change.cursor.position;
        for file in &change.files {
            if case
                .expect
                .files
                .get(&file.key.path())
                .and_then(Option::as_deref)
                .map(str::as_bytes)
                != Some(file.bytes.as_ref())
            {
                return Err("publication file bytes differ".into());
            }
        }
        for key in &change.removed {
            let path = key.path();
            if case.expect.files.get(&path) != Some(&None) {
                return Err(format!("unexpected removal publication: {path}").into());
            }
            removed.insert(path);
        }
        if case.expect.diffs.is_some() {
            paths.extend(change.files.iter().map(|file| file.key.path()));
            paths.extend(change.removed.iter().map(|key| key.path()));
        }
        wire.push(serde_json::to_value(&change)?);
    }
    matchers::ordered(&case.expect.wire, &wire, "file publication wire")?;
    for (path, expected) in &case.expect.files {
        if expected.is_none()
            && ((case.seed.contains_key(path) && !removed.contains(path))
                || target.files.iter().any(|file| file.key.path() == *path))
        {
            return Err(
                format!("missing removal publication or deleted file remains: {path}").into(),
            );
        }
    }
    if let Some(expected) = &case.expect.diffs {
        let current = host.resolve(&workspace, None).await?.generation;
        let root = ContextRoot {
            volume: volume.clone(),
            directory: String::new(),
        };
        let mut view = DiffView::new(volume.clone(), base.clone())?;
        let dropped = view.clone();
        let changes = paths
            .into_iter()
            .map(|path| DiffPath {
                base: initial
                    .files
                    .iter()
                    .any(|file| file.key.path() == path)
                    .then(|| PinnedContextPath {
                        root: root.clone(),
                        generation: base.clone(),
                        path: path.clone(),
                    }),
                current: target
                    .files
                    .iter()
                    .any(|file| file.key.path() == path)
                    .then(|| PinnedContextPath {
                        root: root.clone(),
                        generation: current.clone(),
                        path: path.clone(),
                    }),
                path,
            })
            .collect();
        view.apply(
            &DiffPublication {
                previous: base,
                generation: current,
                paths: changes,
            },
            reader.as_ref(),
            DiffLimits {
                files: 16,
                changes: 16,
                file_bytes: 65_536,
                render_bytes: 65_536,
                work: 65_536,
            },
        )
        .await?;
        assert_rendered(&view, expected)?;
        if !expected.is_empty() {
            // Drop the publication only for this consumer. Exercise the same oracle
            // immediately after the real follow and fold, without another execution
            // or an await that could delay reaching the negative-control assertion.
            let error = assert_rendered(&dropped, expected)
                .expect_err("consumer that dropped the publication passed unchanged expectations");
            assert!(
                error.to_string().contains("rendered diffs differ"),
                "{error}"
            );
        }
    }
    // Fs::local reuses live roots. Release every holder before a cold reopen.
    drop(frames);
    drop(stream);
    drop(harness);
    drop(provider);
    drop(observation);
    drop(source);
    drop(reader);
    drop(host);
    drop(initial);
    drop(target);
    let cold = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(&directory)).await?;
    let cold = acyclic_harness::filesystem::FilesystemHost::new(cold, volume.provider(), None)?;
    for (path, expected) in &case.expect.files {
        let read = cold
            .read(&workspace, None, &format!("/{path}"), 65_536, None)
            .await;
        match (expected, read) {
            (Some(expected), Ok(bytes)) if bytes.as_ref() == expected.as_bytes() => {}
            (None, Err(acyclic_harness::Error::NotFound(_))) => {}
            (_, actual) => {
                return Err(format!("persisted file differs: {path}: {actual:?}").into());
            }
        }
    }
    if requests.is_empty() {
        // Only a failed case may stop before dispatch; validate() requires it.
        return Ok(());
    }
    if ["bash", "fs", "read", "write", "edit"]
        .iter()
        .any(|name| !requests[0].tools.iter().any(|tool| tool.name == *name))
    {
        return Err("local scenario did not use the production default toolset".into());
    }
    let prompt = ModelContent::Text(case.prompt);
    if !requests[0]
        .messages
        .iter()
        .any(|message| message.content == prompt)
    {
        return Err("local scenario prompt was lost before model dispatch".into());
    }
    Ok(())
}

fn check_requests(
    expected: &crate::local_case::Expectations,
    requests: &[ModelRequest],
) -> TestResult {
    let requests = requests
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(expected) = &expected.requests {
        matchers::ordered(expected, &requests, "requests")?;
    }
    for forbidden in &expected.not_requests {
        if requests
            .iter()
            .any(|request| matchers::subset(forbidden, request))
        {
            return Err(format!("forbidden request: {forbidden}").into());
        }
    }
    Ok(())
}

fn assert_rendered(
    view: &DiffView,
    expected: &std::collections::BTreeMap<String, Value>,
) -> TestResult {
    let rendered = rendered_files(view)?;
    if &rendered != expected {
        return Err(
            format!("rendered diffs differ: expected {expected:?}, got {rendered:?}").into(),
        );
    }
    Ok(())
}

fn rendered_files(view: &DiffView) -> TestResult<std::collections::BTreeMap<String, Value>> {
    view.files()
        .iter()
        .map(|(path, diff)| Ok((path.clone(), serde_json::to_value(diff)?)))
        .collect()
}

pub(crate) fn model_results(
    request: &ModelRequest,
) -> TestResult<std::collections::BTreeMap<String, Value>> {
    let mut results = std::collections::BTreeMap::new();
    for part in request
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
    {
        if let ModelContentPart::ToolResult { call_id, .. } = part
            && results
                .insert(call_id.clone(), serde_json::to_value(part)?)
                .is_some()
        {
            return Err("model received duplicate tool results".into());
        }
    }
    Ok(results)
}

pub(crate) fn check_results(
    expected: &std::collections::BTreeMap<String, Value>,
    actual: &std::collections::BTreeMap<String, Value>,
) -> TestResult {
    if expected.keys().ne(actual.keys()) {
        return Err(format!(
            "tool result identities differ: expected {expected:?}, got {actual:?}"
        )
        .into());
    }
    for (call, expected) in expected {
        if !matchers::subset(expected, &actual[call]) {
            return Err(format!(
                "model tool result {call}: expected {expected}, got {}",
                actual[call]
            )
            .into());
        }
    }
    Ok(())
}
