use super::{TestResult, diff_case::DiffCase, diff_workspace, matchers};
use acyclic_fs::performance::BOUNDARY_BUDGET;
use acyclic_harness::{
    LocalHarness, OperationId,
    client::{DiffLimits, DiffPath, DiffPublication, DiffView},
    context::{CompactionPolicy, ContextRoot, PinnedContextPath},
    model::{Model, ModelContent},
    task_volume::{FileQuerySource, Query},
    test_support::{ScriptedModelProvider, local_harness_config, local_task_stream},
};
use acyclic_stream::{StreamPath, StreamProvider as _};
use futures::StreamExt as _;
use prost::Message as _;
use serde_json::Value;
use std::sync::Arc;

pub(super) async fn run(case: DiffCase) -> TestResult {
    let temporary = diff_workspace::temporary()?;
    let directory = temporary.path().join("filesystem");
    std::fs::create_dir(&directory)?;
    let state = temporary.path().join("state");
    let (host, volume, workspace, reader) = diff_workspace::seed(&directory, &case).await?;
    let source = reader.file_query(&volume, BOUNDARY_BUDGET)?;
    let mut observation = source.observe_files(Query::Plans, None, 16, 65_536).await?;
    let initial = observation.snapshot.take().ok_or("missing diff snapshot")?;
    let base = host.resolve(&workspace, None).await?.generation;
    let mut view = DiffView::new(volume.clone(), base.clone())?;
    let model = Model::new("scripted".into(), "diff".into(), "1".into(), Value::Null)?;
    let provider = Arc::new(ScriptedModelProvider::new(case.turns));
    let mut config = local_harness_config(&directory, (model, provider.clone()))?
        .state_directory(&state)
        .volume(volume.clone())
        .compaction(CompactionPolicy::Disabled);
    config.goal_continuations = None;
    let harness = LocalHarness::open(config).await?;
    if !state.join("owner.json").is_file() {
        return Err("harness did not use the fixture-owned state directory".into());
    }
    let stream = local_task_stream(&harness);
    let operation = OperationId::new();
    let acyclic_harness::Admission::Accepted(acyclic_harness::runtime::RuntimeTask::Durable {
        task_id,
        ..
    }) = harness.submit(operation, &case.prompt).await?
    else {
        return Err("diff admission rejected".into());
    };
    let mut frames = stream
        .follow(StreamPath::new("harness/v3/coordinator/events")?, 0)
        .await?;
    let output = harness.resume(task_id).await?;
    if output.text != case.expect.text || provider.remaining() != 0 {
        return Err("diff turn output or model script differs".into());
    }
    let mut wire_frames = Vec::new();
    loop {
        let record = frames.next().await.ok_or("scheduler wire ended")??;
        let frame = acyclic_harness::wire::SchedulerEventEnvelope::decode(record.value.as_ref())?;
        let event: Value = serde_json::from_slice(&frame.canonical_event_json)?;
        let decoded: acyclic_harness::scheduler::SchedulerEvent =
            serde_json::from_slice(&frame.canonical_event_json)?;
        let terminal = match decoded {
            acyclic_harness::scheduler::SchedulerEvent::Completed {
                operation_id,
                outcome,
                ..
            } if operation_id == operation => {
                if !matches!(outcome, acyclic_harness::Outcome::Succeeded(_)) {
                    return Err("wire task did not succeed".into());
                }
                true
            }
            _ => false,
        };
        wire_frames.push(event);
        if terminal {
            break;
        }
    }
    matchers::ordered(&case.expect.frames, &wire_frames, "scheduler wire frames")?;
    let target = source
        .observe_files(Query::Plans, None, 16, 65_536)
        .await?
        .snapshot
        .ok_or("missing final diff snapshot")?;
    let current = host.resolve(&workspace, None).await?.generation;
    let root = ContextRoot {
        volume: volume.clone(),
        directory: String::new(),
    };
    let mut wire = Vec::new();
    let mut seen = initial.cursor.position;
    let mut paths = std::collections::BTreeSet::new();
    let mut removed = std::collections::BTreeSet::new();
    while seen != target.cursor.position {
        let change = observation
            .changes
            .next()
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
        paths.extend(change.files.iter().map(|file| file.key.path()));
        paths.extend(change.removed.iter().map(|key| key.path()));
        wire.push(serde_json::to_value(&change)?);
    }
    matchers::ordered(&case.expect.wire, &wire, "file publication wire")?;
    for (path, expected) in &case.expect.files {
        if expected.is_none()
            && (!removed.contains(path) || target.files.iter().any(|file| file.key.path() == *path))
        {
            return Err(
                format!("missing removal publication or deleted file remains: {path}").into(),
            );
        }
    }
    let dropped = view.clone();
    {
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
    }
    assert_rendered(&view, &case.expect.diffs)?;
    if !case.expect.diffs.is_empty() {
        // Drop the publication only for this consumer. Exercise the same oracle
        // immediately after the real follow and fold, without another execution
        // or an await that could delay reaching the negative-control assertion.
        let error = assert_rendered(&dropped, &case.expect.diffs)
            .expect_err("consumer that dropped the publication passed unchanged expectations");
        assert!(
            error.to_string().contains("rendered diffs differ"),
            "{error}"
        );
    }
    // Fs::local reuses live roots. Release every holder before a cold reopen.
    drop(frames);
    drop(stream);
    drop(harness);
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
    let requests = provider.requests();
    if requests.len() != 2
        || ["write", "bash"]
            .iter()
            .any(|name| !requests[0].tools.iter().any(|tool| tool.name == *name))
    {
        return Err("diff scenario did not use the production default toolset".into());
    }
    let prompt = ModelContent::Text(case.prompt);
    if !requests[0]
        .messages
        .iter()
        .any(|message| message.content == prompt)
    {
        return Err("diff scenario prompt was lost before model dispatch".into());
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
