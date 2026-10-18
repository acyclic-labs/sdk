use super::{TestResult, goals::GoalCase, matchers};
use acyclic_harness::{
    IdempotencyKey, LocalHarness,
    context::CompactionPolicy,
    conversation::{FileDescriptor, FileRef, Limits, VolumeRef},
    filesystem::{WorkspaceMutation, workspace_ref},
    goal::GoalView,
    model::{Model, ModelContent, ModelRole},
    test_support::{ScriptedModelProvider, local_harness_config, local_task_stream},
};
use acyclic_stream::{Record, StreamPath, StreamProvider};
use futures::TryStreamExt as _;
use prost::Message as _;
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc};

fn frame(record: Record, expected: usize) -> TestResult<Value> {
    let frame = acyclic_harness::wire::SchedulerEventEnvelope::decode(record.value.as_ref())?;
    if record.sequence != expected as u64 || frame.revision != record.sequence + 1 {
        return Err("goal frame sequence or revision gap".into());
    }
    let event: Value = serde_json::from_slice(&frame.canonical_event_json)?;
    Ok(Value::Object(
        [
            ("revision".into(), Value::from(frame.revision)),
            ("operation_id".into(), Value::String(frame.operation_id)),
            ("canonical_event_json".into(), event),
        ]
        .into_iter()
        .collect(),
    ))
}

async fn goal(harness: &LocalHarness) -> acyclic_harness::Result<GoalView> {
    let workspace = workspace_ref(
        harness.scratch().provider().clone(),
        &harness.scratch().storage_name(&mut None)?,
    )?;
    let generation = harness
        .filesystem()
        .resolve(&workspace, None)
        .await?
        .generation;
    let bytes = harness
        .filesystem()
        .read(&workspace, Some(&generation), "/GOAL.md", 4096, None)
        .await?;
    let source = FileRef::new(
        harness.scratch().clone(),
        "GOAL.md".into(),
        hex::encode(generation.as_resource().key()),
        FileDescriptor::from_bytes(&bytes, "text/markdown", &mut None)?,
        "GOAL.md".into(),
        &mut None,
    )?;
    GoalView::from_file(source, &bytes, 4096)
}

pub(super) async fn run(mut case: GoalCase) -> TestResult {
    let temporary = super::local_workspace::temporary()?;
    let directory = temporary.path().join("filesystem");
    std::fs::create_dir(&directory)?;
    let state = temporary.path().join("state");
    let (_, project, _, reader) =
        super::local_workspace::seed(&directory, &case.project_seed).await?;
    let provider = Arc::new(ScriptedModelProvider::new(std::mem::take(&mut case.turns)));
    let config = local_harness_config(
        &directory,
        (
            Model::new("scripted".into(), "goal".into(), "1".into(), Value::Null)?,
            provider.clone(),
        ),
    )?
    .state_directory(&state)
    .volume(project.clone())
    .compaction(CompactionPolicy::Disabled)
    .limits(Limits {
        model_steps: case.max_steps as usize,
        ..Limits::default()
    })
    .goal_continuations(case.policy);
    let harness = LocalHarness::open(config).await?;
    let workspace = workspace_ref(
        harness.scratch().provider().clone(),
        &harness.scratch().storage_name(&mut None)?,
    )?;
    let mutations: Vec<_> = std::mem::take(&mut case.seed)
        .into_iter()
        .map(|(path, text)| WorkspaceMutation::PutFile {
            path: format!("/{path}"),
            bytes: text.into_bytes().into(),
        })
        .collect();
    let seeded = if mutations.is_empty() {
        None
    } else {
        Some(
            harness
                .filesystem()
                .apply(
                    &workspace,
                    None,
                    &mutations,
                    &IdempotencyKey::new("goal-seed")?,
                )
                .await?,
        )
    };
    let stream = local_task_stream(&harness);
    let path = StreamPath::new("harness/v3/coordinator/events")?;
    let (operation, task_id, status) =
        super::local_admission::submit(&harness, &case.prompt, reader.as_ref()).await?;
    // The seed reader owns a filesystem host. Release it before cold reopen.
    drop(reader);
    let mut followed = stream.follow(path.clone(), 0).await?;
    let mut frames = Vec::new();
    let result = {
        let execution = harness.resume(task_id);
        tokio::pin!(execution);
        loop {
            tokio::select! {
                output = &mut execution => break output,
                record = followed.try_next() => {
                    let event = frame(record?.ok_or("goal follow ended")?, frames.len())?;
                    frames.push(event);
                },
            }
        }
    };
    // Settle supplies a finite accepted cut. Drain that exact follow prefix;
    // never wait for an expected task count (disabled admission must fail fast).
    let accepted = stream.watermarks(path).await?.accepted;
    while (frames.len() as u64) < accepted {
        frames.push(frame(
            followed
                .try_next()
                .await?
                .ok_or("goal follow ended before its accepted cut")?,
            frames.len(),
        )?);
    }
    {
        let mut next = std::pin::pin!(followed.try_next());
        if super::quiescent::until_idle(next.as_mut()).await.is_some() {
            return Err("goal wire delivered beyond its settled cut".into());
        }
    }
    match (&case.expect.runtime_error, &result) {
        (None, Ok(output)) if output.text == case.expect.text => {}
        (Some(expected), Err(error)) if error.to_string().contains(expected) => {}
        _ => return Err(format!("unexpected goal runtime result: {result:?}").into()),
    }
    super::local_admission::settled(&harness, task_id, status, &case.expect.statuses).await?;
    matchers::ordered(&case.expect.frames, &frames, "goal wire frames")?;
    let mut declared = Vec::new();
    let mut completed = Vec::new();
    for frame in &frames {
        let event = &frame["canonical_event_json"];
        match event["kind"].as_str() {
            Some("declared") => declared.push(
                frame["operation_id"]
                    .as_str()
                    .ok_or("missing goal operation")?,
            ),
            Some("completed") => {
                if event["outcome"].get("Succeeded").is_none() {
                    return Err("goal task did not succeed".into());
                }
                completed.push(
                    frame["operation_id"]
                        .as_str()
                        .ok_or("missing completion operation")?,
                );
            }
            _ => {}
        }
    }
    if declared != completed
        || declared.first().copied() != Some(operation.to_string().as_str())
        || declared.iter().copied().collect::<BTreeSet<_>>().len() != declared.len()
    {
        return Err("goal frames lost, duplicated or reordered an admission/completion".into());
    }
    let requests = provider.requests();
    if !requests.first().is_some_and(|request| {
        request.messages.iter().any(|message| {
            message.role == ModelRole::User
                && matches!(&message.content, ModelContent::Text(text) if text == &case.expect.request_prompt)
        })
    }) {
        return Err("model saw different goal prompt bytes".into());
    }
    if provider.remaining() != 0 || !provider.reconciliations().is_empty() {
        return Err("unused goal script or unexpected reconciliation".into());
    }
    let actual = json!({"continuations":declared.len() - 1, "tasks":declared.len(), "requests":requests.len()});
    if !matchers::subset(&case.expect.result, &actual) {
        return Err(format!("goal result: expected {}, got {actual}", case.expect.result).into());
    }
    match (
        &case.expect.view,
        &case.expect.view_error,
        goal(&harness).await,
    ) {
        (Some(expected), None, Ok(actual))
            if matchers::subset(expected, &serde_json::to_value(&actual)?) => {}
        (None, Some(expected), Err(error)) if error.to_string().contains(expected) => {}
        actual => return Err(format!("unexpected goal view: {actual:?}").into()),
    }
    if let Some(expected) = &case.expect.requests {
        let requests = requests
            .into_iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?;
        matchers::ordered(expected, &requests, "goal model requests")?;
    }
    check_files(&harness, harness.scratch(), &case.expect.files).await?;
    check_files(&harness, harness.project(), &case.expect.project_files).await?;
    if let Some(generation) = seeded {
        for mutation in mutations {
            if let WorkspaceMutation::PutFile { path, bytes } = mutation {
                let retained = harness
                    .filesystem()
                    .read(&workspace, Some(&generation), &path, 4096, None)
                    .await?;
                if retained.as_ref() != bytes.as_ref() {
                    return Err("goal writes changed the immutable seed generation".into());
                }
            }
        }
    }
    // Reopening with future admission disabled still replays the ordinary task.
    let count = provider.requests().len();
    drop(followed);
    drop(stream);
    drop(harness);
    let config = local_harness_config(
        &directory,
        (
            Model::new("scripted".into(), "goal".into(), "1".into(), Value::Null)?,
            provider.clone(),
        ),
    )?
    .state_directory(&state)
    .volume(project.clone())
    .compaction(CompactionPolicy::Disabled)
    .limits(Limits {
        model_steps: case.max_steps as usize,
        ..Limits::default()
    })
    .goal_continuations(None);
    let reopened = LocalHarness::open(config).await?;
    if reopened.resume(task_id).await?.text != case.expect.text {
        return Err("reopened goal task changed its settled result".into());
    }
    if provider.requests().len() != count {
        return Err("reopening a goal task redispatched the model".into());
    }
    check_files(&reopened, reopened.scratch(), &case.expect.files).await?;
    check_files(&reopened, reopened.project(), &case.expect.project_files).await?;
    Ok(())
}

async fn check_files(
    harness: &LocalHarness,
    volume: &VolumeRef,
    expected: &std::collections::BTreeMap<String, Option<String>>,
) -> TestResult {
    if expected.is_empty() {
        return Ok(());
    }
    let workspace = workspace_ref(volume.provider().clone(), &volume.storage_name(&mut None)?)?;
    for (path, expected) in expected {
        let actual = harness
            .filesystem()
            .read(&workspace, None, &format!("/{path}"), 4096, None)
            .await;
        match (expected, actual) {
            (Some(expected), Ok(bytes)) if bytes.as_ref() == expected.as_bytes() => {}
            (None, Err(acyclic_harness::Error::NotFound(_))) => {}
            (expected, actual) => {
                return Err(
                    format!("goal file {path}: expected {expected:?}, got {actual:?}").into(),
                );
            }
        }
    }
    Ok(())
}
