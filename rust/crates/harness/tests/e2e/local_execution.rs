use crate::{
    TestResult,
    local_case::{ExpectedOutcome, LocalCase},
    local_workspace, matchers,
};
use acyclic_harness::{
    LocalHarness,
    agent::{ConversationEntry, TurnInput},
    client::{DiffLimits, DiffPath, DiffPublication, DiffView},
    context::{CompactionPolicy, ContextRoot, PinnedContextPath},
    conversation::{FileDescriptor, FileRef},
    model::{Model, ModelContentPart, ModelEvent, ModelRequest},
    native_settings::SettingsConfig,
    test_support::{
        PromptOutcome, ScriptedEvent, ScriptedModelProvider, ScriptedTurn, local_harness_config,
        prompt_settled,
    },
};
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
    let provider = Arc::new(ScriptedModelProvider::new([]));
    let home = temporary.path().join("home");
    seed_home(&home, &case.home)?;
    let harness = Arc::new(open_harness(&directory, &home, &state, &volume, &provider).await?);
    if !state.join("owner.json").is_file() {
        return Err("harness did not use the fixture-owned state directory".into());
    }
    let base = host.resolve(&workspace, None).await?.generation;
    let references = seed_references(&case, &volume, &base)?;
    let turns = crate::local_case::bind_seed_files(case.turns, &references)?;
    provider.extend(turns);
    let outcome = prompt_settled(&harness, "scenario", &case.prompt).await?;
    let entry = verify_outcome(&case.expect, outcome)?;
    if provider.remaining() != 0 {
        return Err("local model script was not consumed".into());
    }
    let requests = provider.requests();
    verify_model_requests(&case.expect, &requests, steps)?;
    if let Some(expected) = &case.expect.diffs {
        let current = host.resolve(&workspace, None).await?.generation;
        verify_diffs(
            &volume,
            base,
            current,
            &case.seed,
            &case.expect.files,
            &reader,
            expected,
        )
        .await?;
    }
    verify_prompt(&harness, &requests, entry, &case.prompt).await?;
    // Fs::local reuses live roots. Release every holder before a cold reopen.
    drop(harness);
    drop(provider);
    drop(reader);
    drop(host);
    verify_cold_files(&directory, &volume, &workspace, &case.expect.files).await
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

fn seed_home(
    home: &std::path::Path,
    files: &std::collections::BTreeMap<String, Value>,
) -> TestResult {
    for (path, text) in files {
        let path = home.join(path);
        std::fs::create_dir_all(path.parent().ok_or("home file has no parent")?)?;
        match text {
            Value::String(text) => std::fs::write(path, text)?,
            value => std::fs::write(path, serde_json::to_vec(value)?)?,
        }
    }
    Ok(())
}

fn seed_references(
    case: &LocalCase,
    volume: &acyclic_harness::conversation::VolumeRef,
    base: &acyclic_harness::resources::GenerationRef,
) -> TestResult<std::collections::BTreeMap<String, Value>> {
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
                call.arguments.get("version").and_then(Value::as_str)
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
    Ok(references)
}

fn verify_model_requests(
    expect: &crate::local_case::Expectations,
    requests: &[ModelRequest],
    steps: usize,
) -> TestResult {
    if requests.len() != steps {
        return Err("local scenario did not consume exactly its scripted model turns".into());
    }
    check_requests(expect, requests)?;
    if let Some(expected) = &expect.results {
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
    Ok(())
}

async fn verify_diffs(
    volume: &acyclic_harness::conversation::VolumeRef,
    base: acyclic_harness::resources::GenerationRef,
    current: acyclic_harness::resources::GenerationRef,
    seed: &std::collections::BTreeMap<String, String>,
    files: &std::collections::BTreeMap<String, Option<String>>,
    reader: &local_workspace::Reader,
    expected: &std::collections::BTreeMap<String, Value>,
) -> TestResult {
    let root = ContextRoot {
        volume: volume.clone(),
        directory: String::new(),
    };
    let mut view = DiffView::new(volume.clone(), base.clone())?;
    let dropped = view.clone();
    // Every expected file changed from its seed: present at the base when
    // seeded, and at the current generation unless expected removed.
    let changes = files
        .iter()
        .filter(|(path, after)| seed.get(*path) != after.as_ref())
        .map(|(path, after)| (path.clone(), seed.contains_key(path), after.is_some()))
        .map(|(path, in_base, in_current)| DiffPath {
            largest_file_bytes: None,
            matches_base: false,
            base: in_base.then(|| PinnedContextPath {
                root: root.clone(),
                generation: base.clone(),
                path: path.clone(),
            }),
            current: in_current.then(|| PinnedContextPath {
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
        reader,
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
    Ok(())
}

async fn verify_cold_files(
    directory: &std::path::Path,
    volume: &acyclic_harness::conversation::VolumeRef,
    workspace: &acyclic_harness::resources::WorkspaceRef,
    files: &std::collections::BTreeMap<String, Option<String>>,
) -> TestResult {
    let cold = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(directory)).await?;
    let cold = acyclic_harness::filesystem::FilesystemHost::new(cold, volume.provider(), None)?;
    for (path, expected) in files {
        let read = cold
            .read(workspace, None, &format!("/{path}"), 65_536, None)
            .await;
        match (expected, read) {
            (Some(expected), Ok(bytes)) if bytes.as_ref() == expected.as_bytes() => {}
            (None, Err(acyclic_harness::Error::NotFound(_))) => {}
            (_, actual) => {
                return Err(format!("persisted file differs: {path}: {actual:?}").into());
            }
        }
    }
    Ok(())
}

fn verify_outcome(
    expect: &crate::local_case::Expectations,
    outcome: PromptOutcome,
) -> TestResult<Option<ConversationEntry>> {
    match (expect.outcome, outcome) {
        (ExpectedOutcome::Succeeded, PromptOutcome::Settled(entry, output))
            if Some(&output.text) == expect.text.as_ref() =>
        {
            Ok(Some(entry))
        }
        // A failed or uncertain turn ends the agent with its cause.
        (
            ExpectedOutcome::Failed | ExpectedOutcome::Indeterminate,
            PromptOutcome::Failed(message),
        ) if expect
            .error
            .as_deref()
            .is_none_or(|expected| message.contains(expected)) =>
        {
            Ok(None)
        }
        (_, actual) => Err(format!("local turn outcome differs: {actual:?}").into()),
    }
}

/// The first request ran the production default toolset on the staged prompt.
async fn verify_prompt(
    harness: &LocalHarness,
    requests: &[ModelRequest],
    entry: Option<ConversationEntry>,
    prompt: &str,
) -> TestResult {
    let Some(entry) = entry else {
        // Only a failed case may stop before dispatch; validate() requires it.
        return Ok(());
    };
    let first = requests.first().ok_or("missing model request")?;
    if ["shell", "fs", "read", "write", "edit"]
        .iter()
        .any(|name| !first.tools.iter().any(|tool| tool.name == *name))
    {
        return Err("local scenario did not use the production default toolset".into());
    }
    let TurnInput::Message { body, .. } = entry.input else {
        return Err("local scenario turn did not start from its prompt".into());
    };
    let staged = harness
        .client_reader()?
        .read(&body, 1 << 20, None, None)?
        .await?;
    let dispatched = first
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .any(|part| matches!(part, ModelContentPart::File { file, .. } if *file == *body));
    if staged.as_ref() != prompt.as_bytes() || !dispatched {
        return Err("local scenario prompt was lost before model dispatch".into());
    }
    Ok(())
}

async fn open_harness(
    directory: &std::path::Path,
    home: &std::path::Path,
    state: &std::path::Path,
    volume: &acyclic_harness::conversation::VolumeRef,
    provider: &Arc<ScriptedModelProvider>,
) -> TestResult<LocalHarness> {
    let model = Model::new("scripted".into(), "local".into(), "1".into(), Value::Null)?;
    let config = local_harness_config(directory, (model, provider.clone()))?
        .settings(SettingsConfig::new(directory, home).codex_home(home.join(".codex")))
        .state_directory(state)
        .volume(volume.clone())
        .compaction(CompactionPolicy::Disabled)
        .goal_continuations(None);
    Ok(LocalHarness::open(config).await?)
}
