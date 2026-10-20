//! Composer input through the native client entry points GraphCoder composes:
//! the TUI command session bound by `local_ui`, and headless `-p` / JSONL
//! submission through `connect_local`. The scripted provider records each
//! dispatched model request, so the oracle observes what the model received.
use super::TestResult;
use acyclic_harness::{
    LocalHarness, OperationId,
    client::{self, HeadlessOutcome},
    conversation::ContentResidencyVerifier,
    model::{Model, ModelContent, ModelContentPart, ModelRequest},
    native_settings::SettingsConfig,
    test_support::{ScriptedModelProvider, ScriptedTurn, local_harness_config},
};
use serde::Deserialize;
use serde_json::Value;
use std::{collections::BTreeMap, path::Path, sync::Arc};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ComposerEntry {
    /// Plain checkout files, including project `.claude/commands`.
    #[serde(default)]
    seed: BTreeMap<String, String>,
    /// Plain files in the fixture-owned user home, such as `.claude/commands`.
    #[serde(default)]
    home: BTreeMap<String, String>,
    prompt: String,
    #[serde(default)]
    turns: Vec<ScriptedTurn>,
    expect: Expectations,
}

/// Every case runs through each entry point against a fresh harness.
#[derive(Clone, Copy, Debug)]
enum Entry {
    #[cfg(feature = "tui")]
    /// `local_ui` session and TUI submit effect, consumed by the stock runner.
    Tui,
    /// `graphcoder -p`.
    Prompt,
    /// `graphcoder --input-format jsonl`.
    Jsonl,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expectations {
    /// Text parts of the user message in the first model request, joined.
    user_text: Option<String>,
    /// Exact bytes of each file part in that user message, in order.
    #[serde(default)]
    attachments: Vec<String>,
    /// Substrings no dispatched model request may contain.
    #[serde(default)]
    absent: Vec<String>,
    /// Required refusal substring; refusal must precede every model request.
    error: Option<String>,
}

impl ComposerEntry {
    pub(super) fn parse(value: Value) -> Result<Self, String> {
        let case: Self = serde_json::from_value(value).map_err(|error| error.to_string())?;
        let refused = case.expect.error.is_some();
        if case.prompt.is_empty()
            || refused == case.expect.user_text.is_some()
            || refused != case.turns.is_empty()
            || (refused && !case.expect.attachments.is_empty())
            || case.expect.error.as_ref().is_some_and(String::is_empty)
        {
            return Err(
                "composer entry cases expect either a model turn with user text or a refusal"
                    .into(),
            );
        }
        Ok(case)
    }

    pub(super) async fn run(self) -> TestResult {
        // The TUI composer binding exists only in `tui` builds; GraphCoder
        // always enables it, and the headless entries run in every build.
        let entries = [
            #[cfg(feature = "tui")]
            Entry::Tui,
            Entry::Prompt,
            Entry::Jsonl,
        ];
        for entry in entries {
            Box::pin(self.run_entry(entry))
                .await
                .map_err(|error| format!("{entry:?} entry: {error}"))?;
        }
        Ok(())
    }

    async fn run_entry(&self, entry: Entry) -> TestResult {
        let temporary = super::local_workspace::temporary()?;
        let workspace = temporary.path().join("workspace");
        let home = temporary.path().join("home");
        write_tree(&workspace, &self.seed)?;
        write_tree(&home, &self.home)?;
        let steps = self.turns.len();
        let provider = Arc::new(ScriptedModelProvider::new(self.turns.clone()));
        let model = Model::new("scripted".into(), "local".into(), "1".into(), Value::Null)?;
        let config = local_harness_config(&workspace, (model, provider.clone()))?
            .settings(SettingsConfig::new(&workspace, &home).codex_home(home.join(".codex")))
            .state_directory(temporary.path().join("state"))
            .compaction(acyclic_harness::context::CompactionPolicy::Disabled)
            .goal_continuations(None);
        let harness = Arc::new(Box::pin(LocalHarness::open(config)).await?);
        let reader = harness.client_reader()?;
        let outcome = match entry {
            #[cfg(feature = "tui")]
            Entry::Tui => Box::pin(tui(&harness, reader.as_ref(), &self.prompt)).await,
            Entry::Prompt | Entry::Jsonl => {
                Box::pin(headless(harness.clone(), entry, &self.prompt)).await
            }
        };
        let requests = provider.requests();
        let checked = Box::pin(check(
            &self.expect,
            outcome,
            &requests,
            steps,
            reader.as_ref(),
        ))
        .await;
        Box::pin(client::disconnect_local(harness)).await?;
        checked
    }
}

fn write_tree(root: &Path, files: &BTreeMap<String, String>) -> TestResult {
    std::fs::create_dir_all(root)?;
    for (path, text) in files {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().ok_or("seed file has no parent")?)?;
        std::fs::write(path, text)?;
    }
    Ok(())
}

#[cfg(feature = "tui")]
async fn tui(
    harness: &LocalHarness,
    reader: &dyn ContentResidencyVerifier,
    prompt: &str,
) -> TestResult {
    let session = OperationId::new();
    let mut ui = Box::pin(client::local_ui(
        harness,
        &session.to_string(),
        &harness.client_owner(),
    ))
    .await?;
    Box::pin(client::apply_tui_input(
        &mut ui,
        reader,
        acyclic_harness::tui::InputEffect::Submit {
            text: prompt.into(),
            queued: false,
            edit: None,
        },
    ))
    .await?;
    // GraphCoder's command owner consumes the same session's request files.
    let mut owner = client::CommandSession::open_local(
        harness,
        uuid::Uuid::from_bytes(session.into_bytes()),
        65_536,
        4096,
    )
    .await?;
    let request = owner.next_request().await?;
    Box::pin(client::run_local_command(&mut owner, harness, &request))
        .await?
        .ok_or("TUI request was not consumed")?;
    Ok(())
}

async fn headless(harness: Arc<LocalHarness>, entry: Entry, prompt: &str) -> TestResult {
    let operation = OperationId::new();
    let command = client::prepare_local_prompt(operation, prompt)?;
    let (mut connection, target) = Box::pin(client::connect_local(harness, operation)).await?;
    let mut output = Vec::new();
    let outcome = match entry {
        Entry::Jsonl => {
            let line = client::jsonl::request(&command)?;
            Box::pin(client::run_jsonl(
                &mut connection,
                target,
                futures::stream::iter([Ok(line)]),
                &mut output,
            ))
            .await
        }
        _ => {
            Box::pin(client::run_headless(
                &mut connection,
                command,
                target,
                &mut output,
            ))
            .await
        }
    };
    match Box::pin(connection.finish(outcome)).await? {
        HeadlessOutcome::Completed => Ok(()),
        outcome => Err(format!(
            "headless outcome {outcome:?}: {}",
            String::from_utf8_lossy(&output)
        )
        .into()),
    }
}

async fn check(
    expected: &Expectations,
    outcome: TestResult,
    requests: &[ModelRequest],
    steps: usize,
    reader: &dyn ContentResidencyVerifier,
) -> TestResult {
    let serialized = requests
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()?;
    for absent in &expected.absent {
        if let Some(request) = serialized.iter().find(|request| request.contains(absent)) {
            return Err(format!("model request contains {absent:?}: {request}").into());
        }
    }
    if let Some(error) = &expected.error {
        return match outcome {
            Err(actual) if actual.to_string().contains(error) && requests.is_empty() => Ok(()),
            actual => Err(format!(
                "expected refusal {error:?} before model work, got {actual:?} after {} requests",
                requests.len()
            )
            .into()),
        };
    }
    outcome?;
    if requests.len() != steps {
        return Err(format!("expected {steps} model requests, got {}", requests.len()).into());
    }
    let user = requests[0]
        .messages
        .iter()
        .rev()
        .find(|message| serde_json::to_value(message.role).is_ok_and(|role| role == "user"))
        .ok_or("first model request has no user message")?;
    let plain;
    let parts = match &user.content {
        ModelContent::Text(text) => {
            plain = [ModelContentPart::Text { text: text.clone() }];
            &plain[..]
        }
        ModelContent::Part(part) => std::slice::from_ref(part),
        ModelContent::Parts(parts) => parts.as_slice(),
    };
    let mut text = String::new();
    let mut files = Vec::new();
    for part in parts {
        match part {
            ModelContentPart::Text { text: part } => text.push_str(part),
            ModelContentPart::File { file, .. } => files.push(file),
            _ => return Err("user message has a non-input part".into()),
        }
    }
    if expected.user_text.as_ref() != Some(&text) {
        return Err(format!("user text: expected {:?}, got {text:?}", expected.user_text).into());
    }
    if files.len() != expected.attachments.len() {
        return Err(format!(
            "attachments: expected {}, got {} in {}",
            expected.attachments.len(),
            files.len(),
            serialized[0]
        )
        .into());
    }
    for (file, expected) in files.into_iter().zip(&expected.attachments) {
        let bytes = reader.read(file, 65_536, None)?.await?;
        if bytes.as_ref() != expected.as_bytes() {
            return Err(format!(
                "attachment {}: expected {expected:?}, got {:?}",
                file.path(),
                String::from_utf8_lossy(&bytes)
            )
            .into());
        }
    }
    Ok(())
}
