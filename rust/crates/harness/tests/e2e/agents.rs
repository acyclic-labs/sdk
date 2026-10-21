use super::{TestResult, matchers};
use acyclic_harness::{
    Error, LocalHarness, Result,
    context::{ModelContextCapacity, ModelTokenCount},
    conversation::ContentResidencyVerifier,
    model::{
        Model, ModelContent, ModelContentPart, ModelDispatch, ModelEvent, ModelProvider, ModelRole,
        PreparedModelRequest,
    },
    task_volume::{
        ChildFile, ChildState, ChildStatus, Component, Control, FileKey, FileValue, Query,
    },
    test_support::{ScriptedModelProvider, ScriptedTurn, local_harness_config, read_request_files},
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use futures::{StreamExt as _, TryStreamExt as _};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    prompt: String,
    #[serde(default)]
    seed: BTreeMap<String, String>,
    /// Commit the seed into a host Git repository before the harness opens.
    #[serde(default)]
    repository: bool,
    models: BTreeMap<String, Vec<ScriptedTurn>>,
    #[serde(default)]
    routes: BTreeMap<String, String>,
    #[serde(default)]
    actions: Vec<Action>,
    #[serde(default)]
    reader: Reader,
    expect: Expected,
}

/// Whose authority a model request's file parts are read under.
#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Reader {
    /// The executing task's own reader, as the harness binds it.
    #[default]
    Task,
    /// The first reader bound, reused for every task: the former
    /// composition-wide binding, kept only as a negative control.
    Composition,
}

type BoundReader = (Arc<dyn ContentResidencyVerifier>, u64);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    text: Option<String>,
    #[serde(default)]
    indeterminate: bool,
    requests: BTreeMap<String, usize>,
    task_files: BTreeMap<String, Value>,
    #[serde(default)]
    model_requests: BTreeMap<String, Vec<Value>>,
    #[serde(default)]
    files: BTreeMap<String, Option<String>>,
    #[serde(default)]
    active: Option<Vec<String>>,
    #[serde(default)]
    toolsets: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Action {
    model: String,
    request: usize,
    agent: String,
    control: Option<Control>,
    message: Option<String>,
}

#[derive(Clone)]
struct Models {
    scripts: Arc<BTreeMap<String, Arc<ScriptedModelProvider>>>,
    routes: Arc<BTreeMap<[u8; 32], String>>,
    actions: Arc<BTreeMap<(String, usize), Action>>,
    events: tokio::sync::mpsc::UnboundedSender<(Action, tokio::sync::oneshot::Sender<()>)>,
    denied_reads: std::sync::mpsc::Sender<String>,
    /// The executing task's reader; every request reads its file parts through
    /// it, as a provider projecting file bytes does.
    content: Option<BoundReader>,
    /// Set under [`Reader::Composition`]: the one reader every task reuses.
    pinned: Option<Arc<std::sync::OnceLock<BoundReader>>>,
}

impl Models {
    fn get(&self, name: &str) -> Result<&ScriptedModelProvider> {
        self.scripts
            .get(name)
            .map(Arc::as_ref)
            .ok_or_else(|| Error::NotFound(format!("scripted model {name}")))
    }
    fn route(&self, request: &PreparedModelRequest) -> Result<(&str, &ScriptedModelProvider)> {
        let text = |text: &str| {
            let hash: [u8; 32] = Sha256::digest(text.as_bytes()).into();
            self.routes.get(&hash)
        };
        let part = |part: &ModelContentPart| match part {
            ModelContentPart::File { file, .. } => self.routes.get(file.descriptor().sha256()),
            ModelContentPart::Text { text: value } => text(value),
            _ => None,
        };
        let name = request
            .request()
            .messages
            .iter()
            .filter(|message| message.role == ModelRole::User)
            .find_map(|message| match &message.content {
                ModelContent::Text(value) => text(value),
                ModelContent::Part(value) => part(value),
                ModelContent::Parts(parts) => parts.iter().find_map(|value| part(value)),
            })
            .map(String::as_str)
            .unwrap_or(&request.request().model.name);
        self.scripts
            .get_key_value(name)
            .map(|(name, provider)| (name.as_str(), provider.as_ref()))
            .ok_or_else(|| Error::NotFound(format!("scripted model {name}")))
    }
}

impl ModelProvider for Models {
    fn bind_content(
        &self,
        reader: Arc<dyn ContentResidencyVerifier>,
        maximum_bytes: u64,
    ) -> Option<Arc<dyn ModelProvider>> {
        let content = match &self.pinned {
            Some(pinned) => pinned.get_or_init(|| (reader, maximum_bytes)).clone(),
            None => (reader, maximum_bytes),
        };
        Some(Arc::new(Self {
            content: Some(content),
            ..self.clone()
        }))
    }
    fn context_capacity(&self, model: &Model) -> Result<ModelContextCapacity> {
        self.get(&model.name)?.context_capacity(model)
    }
    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
    ) -> BoxProviderFuture<'a, Result<ModelTokenCount>> {
        Box::pin(async move { self.route(request)?.1.count_tokens(request).await })
    }
    fn generate<'a>(
        &'a self,
        request: PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> BoxProviderStream<'a, Result<ModelEvent>> {
        let (name, provider) = match self.route(&request) {
            Ok(value) => value,
            Err(error) => return Box::pin(futures::stream::iter([Err(error)])),
        };
        let turn = provider.requests().len() + 1;
        let action = self.actions.get(&(name.to_owned(), turn)).cloned();
        eprintln!(
            "agent scenario: model {name} request {turn} ({} bytes)",
            request.bytes().len()
        );
        Box::pin(
            futures::stream::once(async move {
                let reader = self
                    .content
                    .as_ref()
                    .map(|(reader, maximum)| (reader.as_ref(), *maximum));
                read_request_files(reader, &request)
                    .await
                    .inspect_err(|error| {
                        if matches!(
                            error,
                            Error::Unauthorized(_)
                                | Error::NativeBoundary {
                                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                                    ..
                                }
                        ) {
                            let _ = self.denied_reads.send(name.to_owned());
                        }
                    })?;
                if let Some(action) = action {
                    let (accepted, reply) = tokio::sync::oneshot::channel();
                    self.events
                        .send((action, accepted))
                        .map_err(|_| Error::Conflict("scenario client stopped".into()))?;
                    reply
                        .await
                        .map_err(|_| Error::Conflict("scenario control was not accepted".into()))?;
                }
                Ok::<_, Error>(provider.generate(request, dispatch))
            })
            .try_flatten(),
        )
    }
}

async fn control(harness: &LocalHarness, action: &Action) -> Result<()> {
    let reference =
        Component::new(action.agent.clone()).map_err(|e| Error::Invalid(e.to_string()))?;
    let (file, bytes) = match (action.control, action.message.as_ref()) {
        (Some(control), None) => (ChildFile::Control, control.bytes().to_vec()),
        (None, Some(message)) => (
            ChildFile::Inbox(
                Component::new(format!("scenario-{}-{}", action.model, action.request))
                    .map_err(|e| Error::Invalid(e.to_string()))?,
            ),
            message.as_bytes().to_vec(),
        ),
        _ => {
            return Err(Error::Invalid(
                "scenario action needs exactly one control or message".into(),
            ));
        }
    };
    let file = FileValue::new(
        FileKey::Child {
            reference: reference.clone(),
            file,
        },
        bytes,
    );
    let mut observation = harness
        .task_files()
        .observe(Query::Child(reference), None, 64)
        .await?;
    let mut generation = observation
        .snapshot
        .take()
        .ok_or_else(|| Error::Conflict("scenario control snapshot is absent".into()))?
        .generation;
    loop {
        match harness.task_files().write(generation, &file).await {
            Ok(_) => return Ok(()),
            Err(acyclic_harness::task_volume::Error::Conflict(_)) => {
                let change = observation
                    .changes
                    .next()
                    .await
                    .ok_or_else(|| Error::Conflict("scenario control follow ended".into()))??;
                generation = acyclic_harness::task_volume::Generation {
                    volume: change.cursor.volume,
                    epoch: change.cursor.epoch,
                    position: change.cursor.position,
                    commit: change.cursor.commit,
                };
            }
            Err(error) => return Err(error.into()),
        }
    }
}

pub(super) async fn run(value: Value) -> TestResult {
    let case: Case = serde_json::from_value(value)?;
    if case.expect.indeterminate != case.expect.text.is_none() {
        return Err("scenario must select either an answer or an indeterminate outcome".into());
    }
    if case.models.keys().ne(case.expect.requests.keys()) {
        return Err("every scripted model needs an exact request count".into());
    }
    for (name, turns) in &case.models {
        if turns.len() != case.expect.requests[name] {
            return Err(
                format!("model {name}: script must contain exactly the expected work").into(),
            );
        }
    }
    let (events, mut actions) = tokio::sync::mpsc::unbounded_channel();
    let (denied_reads, denied_readers) = std::sync::mpsc::channel();
    let expected_actions = case.actions.len();
    let provider = Arc::new(Models {
        scripts: Arc::new(
            case.models
                .into_iter()
                .map(|(name, turns)| {
                    (
                        name,
                        Arc::new(ScriptedModelProvider::new(turns).with_context_capacity(
                            ModelContextCapacity {
                                context_tokens: 32_768,
                                output_tokens: Some(4096),
                            },
                        )),
                    )
                })
                .collect(),
        ),
        routes: Arc::new(
            case.routes
                .into_iter()
                .map(|(body, name)| {
                    let hash: [u8; 32] = Sha256::digest(body.as_bytes()).into();
                    (hash, name)
                })
                .collect(),
        ),
        actions: Arc::new(
            case.actions
                .into_iter()
                .map(|action| ((action.model.clone(), action.request), action))
                .collect(),
        ),
        events,
        denied_reads,
        content: None,
        pinned: matches!(case.reader, Reader::Composition).then(Default::default),
    });
    if provider.actions.len() != expected_actions {
        return Err("scenario actions must use distinct model request events".into());
    }
    let directory = tempfile::tempdir()?;
    for (path, text) in case.seed {
        let path = file_path(directory.path(), &path)?;
        std::fs::create_dir_all(path.parent().ok_or("missing seed parent")?)?;
        std::fs::write(path, text)?;
    }
    if case.repository {
        for args in [
            &["init", "-q"][..],
            &["add", "-A"],
            &[
                "-c",
                "user.name=Scenario",
                "-c",
                "user.email=scenario@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-qm",
                "seed",
            ],
        ] {
            let status = std::process::Command::new("git")
                .args(args)
                .current_dir(directory.path())
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .status()?;
            if !status.success() {
                return Err(format!("seed repository git {args:?} failed: {status}").into());
            }
        }
    }
    let model = Model::new("scripted".into(), "parent".into(), "1".into(), Value::Null)?;
    // The test composition owns its state and settings; nothing reaches the per-user store.
    let harness = LocalHarness::open(local_harness_config(
        directory.path(),
        (model, provider.clone()),
    )?)
    .await?;
    let mut applied = 0;
    let outcome = tokio::select! {
        result = harness.run(&case.prompt) => result,
        result = async {
            while let Some((action, accepted)) = actions.recv().await {
                control(&harness, &action).await?;
                applied += 1;
                let _ = accepted.send(());
            }
            Err(Error::Conflict("scenario action stream ended".into()))
        } => result,
    };
    let output = match outcome {
        Ok(output) => Some(output),
        Err(Error::Indeterminate(_) | Error::CommandIndeterminate { .. })
            if case.expect.indeterminate =>
        {
            None
        }
        Err(error) => {
            let requests: BTreeMap<_, _> = provider
                .scripts
                .iter()
                .map(|(name, model)| (name.as_str(), model.requests().len()))
                .collect();
            let children = harness
                .task_files()
                .observe(Query::Children, None, 64)
                .await?
                .snapshot
                .ok_or("missing child failure snapshot")?;
            let statuses: BTreeMap<_, _> = children
                .files
                .iter()
                .filter(|file| file.key.path().ends_with("/status.json"))
                .map(|file| {
                    (
                        file.key.path(),
                        String::from_utf8_lossy(&file.bytes).into_owned(),
                    )
                })
                .collect();
            return Err(format!(
                "{error}; model requests: {requests:?}; child statuses: {statuses:?}; unauthorized content readers: {:?}",
                denied_readers.try_iter().collect::<Vec<_>>()
            )
            .into());
        }
    };
    if applied != expected_actions {
        return Err(
            format!("expected {expected_actions} accepted client actions, got {applied}").into(),
        );
    }
    for (name, expected) in &case.expect.toolsets {
        let mut expected = expected.clone();
        expected.sort();
        for request in provider.get(name)?.requests() {
            let mut actual: Vec<_> = request.tools.iter().map(|tool| tool.name.clone()).collect();
            actual.sort();
            if actual != expected {
                return Err(
                    format!("model {name}: expected tools {expected:?}, got {actual:?}").into(),
                );
            }
        }
    }
    if let Some(mut expected) = case.expect.active {
        let snapshot = harness
            .task_files()
            .observe(Query::Children, None, 64)
            .await?
            .snapshot
            .ok_or("missing active child snapshot")?;
        let mut actual = Vec::new();
        for file in snapshot.files {
            if let FileKey::Child {
                reference,
                file: ChildFile::Status,
            } = file.key
            {
                let status = ChildStatus::parse(&file.bytes)?;
                if !status.settled
                    || !matches!(status.state, ChildState::Cancelled | ChildState::Archived)
                {
                    actual.push(reference.as_str().to_owned());
                }
            }
        }
        actual.sort();
        expected.sort();
        if actual != expected {
            return Err(format!("active children: expected {expected:?}, got {actual:?}").into());
        }
    }
    let text = output.map(|output| output.text);
    if text != case.expect.text {
        return Err(format!("unexpected parent output: {text:?}").into());
    }
    for (name, expected) in case.expect.requests {
        let model = provider.get(&name)?;
        if model.requests().len() != expected || model.remaining() != 0 {
            // A parent may finish normally after a child refuses its input.
            // Preserve the same reader evidence as the failed-parent path.
            let requests: BTreeMap<_, _> = provider
                .scripts
                .iter()
                .map(|(name, model)| (name.as_str(), model.requests().len()))
                .collect();
            return Err(format!("model {name}: expected {expected} requests and no unused turns, got {} requests and {} unused turns; model requests: {requests:?}; unauthorized content readers: {:?}", model.requests().len(), model.remaining(), denied_readers.try_iter().collect::<Vec<_>>()).into());
        }
    }
    for (name, expected) in case.expect.model_requests {
        let actual = provider
            .get(&name)?
            .requests()
            .into_iter()
            .map(serde_json::to_value)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        matchers::ordered(&expected, &actual, &name)?;
    }
    for (path, expected) in case.expect.files {
        let actual = if let Some(relative) = path.strip_prefix(".volumes/scratch/") {
            file_path(directory.path(), &path)?;
            // Command projections retire between invocations. Observe the
            // owner's durable private bytes through its authenticated reader.
            match harness
                .client_reader()?
                .read_private_path(harness.scratch(), "", relative, None, 65_536, None)?
                .await
            {
                Ok((_, bytes, _)) => Some(String::from_utf8(bytes.to_vec())?),
                Err(Error::NotFound(_)) => None,
                Err(error) => return Err(error.into()),
            }
        } else {
            match std::fs::read_to_string(file_path(directory.path(), &path)?) {
                Ok(text) => Some(text),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            }
        };
        if actual != expected {
            return Err(format!("workspace {path}: expected {expected:?}, got {actual:?}").into());
        }
    }
    for (path, expected) in case.expect.task_files {
        let key = FileKey::parse(&path)?;
        let snapshot = harness
            .task_files()
            .observe(Query::File(key), None, 1)
            .await?
            .snapshot
            .ok_or("missing file snapshot")?;
        if expected.is_null() {
            if !snapshot.files.is_empty() {
                return Err(format!("unexpected file {path}").into());
            }
            continue;
        }
        let file = snapshot
            .files
            .first()
            .ok_or_else(|| format!("missing {path}"))?;
        let actual = serde_json::from_slice(&file.bytes)?;
        if !matchers::subset(&expected, &actual) {
            return Err(format!("{path}: expected {expected}, got {actual}").into());
        }
    }
    Ok(())
}

fn file_path(root: &std::path::Path, path: &str) -> TestResult<std::path::PathBuf> {
    let path = std::path::Path::new(path);
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
        return Err("scenario file must stay inside its workspace".into());
    }
    Ok(root.join(path))
}

/// Negative control: one reader bound for the whole composition, as the model
/// provider was bound at open, cannot read the task a child received, so the
/// child never reaches its model.
#[test]
fn a_composition_wide_reader_cannot_read_a_childs_task() -> TestResult {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/local-scenarios/agents/spawn-wait.json");
    let mut case: Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let object = case.as_object_mut().ok_or("scenario must be an object")?;
    object.remove("driver");
    let runtime = super::scenario_runtime()?;
    // Prove this head can execute the scenario before changing its reader.
    runtime.block_on(run(case.clone()))?;
    case["reader"] = "composition".into();
    let error = runtime
        .block_on(run(case))
        .err()
        .ok_or("a composition-wide reader read the child's task")?
        .to_string();
    require_child_reader_refusal(&error)
}

fn require_child_reader_refusal(error: &str) -> TestResult {
    if error.contains(r#""worker": 0"#)
        && error.contains(r#"unauthorized content readers: ["worker"]"#)
    {
        Ok(())
    } else {
        Err(format!("the child's content read was not refused: {error}").into())
    }
}

#[test]
fn reader_control_rejects_unrelated_failures() {
    let no_dispatch = r#"workspace generation changed; model requests: {"parent": 1, "worker": 0}"#;
    // Negative control: the former oracle accepts this unrelated failure.
    assert!(no_dispatch.contains(r#""worker": 0"#));
    assert!(require_child_reader_refusal(no_dispatch).is_err());
    assert!(
        require_child_reader_refusal(&format!(
            "{no_dispatch}; unauthorized content readers: [\"parent\"]"
        ))
        .is_err()
    );
    assert!(
        require_child_reader_refusal(&format!(
            "{no_dispatch}; unauthorized content readers: [\"worker\"]"
        ))
        .is_ok()
    );
    assert!(
        require_child_reader_refusal(
            r#"model requests: {"worker": 1}; unauthorized content readers: ["worker"]"#
        )
        .is_err()
    );
}
