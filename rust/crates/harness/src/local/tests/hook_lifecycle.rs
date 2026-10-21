use super::*;
use crate::conversation::{ContentPublisher, ContentResidencyVerifier};
use crate::native_settings::SettingsConfig;
use futures::StreamExt;

struct WarningWriter(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for WarningWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        // Test-only sink, a leaf lock; writing never invokes a provider/caller.
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn settings(script: &Path, text: &str) -> Value {
    json!({"hooks":{"SessionStart":[{"matcher":"startup","hooks":[{
        "type":"command", "command":"sh", "args":[script, text], "onFailure":"block"
    }]}]}})
}

fn request_has(provider: &ScriptedModelProvider, index: usize, text: &str) -> Result<bool> {
    Ok(serde_json::to_string(&provider.requests()[index])
        .map_err(super::super::storage)?
        .contains(text))
}

#[tokio::test]
async fn start_hooks_run_from_pinned_settings_once_and_retain_their_workspace_edits() -> Result<()>
{
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    let workspace = root.path().join("workspace");
    let home = root.path().join("home");
    for path in [workspace.join(".claude"), home.join(".claude")] {
        std::fs::create_dir_all(path).map_err(super::super::storage)?;
    }
    let script = root.path().join("hook.sh");
    std::fs::write(
        &script,
        r#"set -eu
[ "$PWD" = "$CLAUDE_PROJECT_DIR" ]
cat >/dev/null
printf '%s\n' "$1" >> hook-runs
printf '{"hookSpecificOutput":{"additionalContext":"%s"}}' "$1"
"#,
    )
    .map_err(super::super::storage)?;
    let user = home.join(".claude/settings.json");
    std::fs::write(&user, settings(&script, "pinned-start-A").to_string())
        .map_err(super::super::storage)?;
    std::fs::write(
        workspace.join(".claude/settings.json"),
        settings(&script, "untrusted-start").to_string(),
    )
    .map_err(super::super::storage)?;
    let provider = scripted([
        ScriptedTurn::Text("answer A".into()),
        ScriptedTurn::Text("answer B".into()),
    ]);
    let config = || -> Result<LocalHarnessConfig> {
        Ok(checkout_config(&workspace, (model()?, provider.clone()))?
            .state_directory(root.path().join("state"))
            .settings(SettingsConfig::new(&workspace, &home).codex_home(home.join(".codex"))))
    };
    let harness = LocalHarness::open(config()?).await?;
    let operation = OperationId::new();
    let task = super::context::admitted(harness.submit(operation, "queued A").await?)?;
    assert!(provider.requests().is_empty());
    let manifest = harness.retained_context_manifest(operation).await?;
    let key = crate::task_volume::FileKey::Task {
        task: crate::task_volume::Component::new(task.to_string())?,
        file: crate::task_volume::TaskFile::Context(crate::task_volume::Component::new(
            "context-manifest",
        )?),
    };
    let protected = harness
        .task_files
        .inner
        .snapshot(&crate::task_volume::Query::File(key.clone()), 1)
        .await?;
    let pointer: crate::conversation::FileRef =
        serde_json::from_slice(&protected.files[0].bytes).map_err(super::super::storage)?;
    assert_eq!(pointer, manifest);
    assert!(matches!(
        harness
            .task_files
            .write(
                protected.generation,
                &crate::task_volume::FileValue::new(key, b"{}".to_vec())
            )
            .await,
        Err(crate::task_volume::Error::Unauthorized)
    ));
    drop(harness);
    std::fs::write(&user, settings(&script, "pinned-start-B").to_string())
        .map_err(super::super::storage)?;
    let harness = LocalHarness::open(config()?).await?;
    let output = harness.resume(task).await?;
    assert_eq!(output.text, "answer A");
    assert!(request_has(&provider, 0, "pinned-start-A")?);
    assert!(!request_has(&provider, 0, "pinned-start-B")?);
    assert!(!request_has(&provider, 0, "untrusted-start")?);
    assert_eq!(harness.resume(task).await?, output);
    let next = super::context::admitted(harness.submit(OperationId::new(), "new B").await?)?;
    assert_eq!(harness.resume(next).await?.text, "answer B");
    assert!(request_has(&provider, 1, "pinned-start-B")?);
    assert!(!request_has(&provider, 1, "pinned-start-A")?);
    assert_eq!(
        std::fs::read_to_string(workspace.join("hook-runs")).map_err(super::super::storage)?,
        "pinned-start-A\npinned-start-B\n"
    );
    let reader = harness.context_reader()?;
    let (_, bytes, _) = reader
        .read_private_path(harness.project(), "", "hook-runs", None, 4096, None)?
        .await?;
    assert_eq!(bytes.as_ref(), b"pinned-start-A\npinned-start-B\n");
    assert_eq!(
        provider.requests().len(),
        2,
        "replay resent a model request"
    );
    Ok(())
}

#[tokio::test]
async fn queued_admissions_reconstruct_hook_authority_from_their_original_model_command()
-> Result<()> {
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    let workspace = root.path().join("workspace");
    let home = root.path().join("home");
    std::fs::create_dir(&workspace).map_err(super::super::storage)?;
    std::fs::create_dir_all(home.join(".claude")).map_err(super::super::storage)?;
    let script = root.path().join("hook.sh");
    std::fs::write(
        &script,
        "cat >/dev/null\nprintf 'once\\n' >> hook-runs\nprintf '%s' \"$1\"\n",
    )
    .map_err(super::super::storage)?;
    let current = home.join(".claude/settings.json");
    std::fs::write(
        &current,
        settings(&script, "original-admission-context").to_string(),
    )
    .map_err(super::super::storage)?;
    let provider = text();
    let config = || -> Result<LocalHarnessConfig> {
        Ok(checkout_config(&workspace, (model()?, provider.clone()))?
            .state_directory(root.path().join("state"))
            .settings(SettingsConfig::new(&workspace, &home).codex_home(home.join(".codex"))))
    };
    let harness = LocalHarness::open(config()?).await?;
    let operation = OperationId::new();
    // Admit the durable contract directly, as other producers do. No derived
    // LocalHarness context pointer has been published by stage_bound_turn.
    let manifest = harness.stage_context_manifest(operation).await?;
    let command = crate::filesystem::ModelTaskCommand {
        input: crate::model::ModelContent::Text("queued before hook dispatch".into()),
        context_manifest: manifest.clone(),
        summary_source: None,
        selected_context: None,
        max_steps: 8,
        max_output_tokens: None,
        mailbox_operation: None,
        selected_output_volume: None,
    };
    let publisher = harness.input_publisher()?;
    let bytes = serde_json::to_vec(&command).map_err(super::super::storage)?;
    let payload = publisher
        .stage_once(
            operation,
            &format!("inputs/{operation}.json"),
            &bytes,
            "application/json",
            "queued model",
            None,
        )?
        .await?
        .0;
    let (receive, model) = harness
        .finish_turn_inputs(operation, payload, publisher.as_ref())
        .await?;
    let definition = harness
        .runtime
        .harness()
        .task::<Value, crate::executor::TurnOutput>("acyclic.stock_turn@1")?;
    let task = super::context::admitted(
        harness
            .runtime
            .harness()
            .admit(
                operation,
                &definition,
                json!({"receive":receive,"model":model}),
                None,
            )
            .await?,
    )?;
    let key = crate::task_volume::FileKey::parse(&format!(
        ".tasks/{task}/context/context-manifest/manifest.json"
    ))?;
    assert!(
        harness
            .task_files
            .inner
            .snapshot(&crate::task_volume::Query::File(key.clone()), 1)
            .await?
            .files
            .is_empty()
    );
    let backend = harness.workspace.as_ref().unwrap().store.clone();
    drop(publisher);
    drop(harness);
    std::fs::remove_file(&current).map_err(super::super::storage)?;
    let harness = LocalHarness::open(config()?).await?;
    drop(backend);
    assert_eq!(harness.resume(task).await?.text, "done");
    assert!(request_has(&provider, 0, "original-admission-context")?);
    let files = harness
        .task_files
        .inner
        .snapshot(&crate::task_volume::Query::File(key), 1)
        .await?;
    let retained: crate::conversation::FileRef =
        serde_json::from_slice(&files.files[0].bytes).map_err(super::super::storage)?;
    assert_eq!(retained, manifest);
    assert_eq!(
        std::fs::read_to_string(workspace.join("hook-runs")).map_err(super::super::storage)?,
        "once\n"
    );
    assert_eq!(harness.resume(task).await?.text, "done");
    assert_eq!(provider.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn reopening_after_hook_completion_reuses_its_outcome_without_relaunching() -> Result<()> {
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    let workspace = root.path().join("workspace");
    let home = root.path().join("home");
    std::fs::create_dir(&workspace).map_err(super::super::storage)?;
    std::fs::create_dir_all(home.join(".claude")).map_err(super::super::storage)?;
    let script = root.path().join("hook.sh");
    std::fs::write(&script,
        "set -eu\ncat >/dev/null\nprintf 'started\\n' >> hook-runs\nprintf 'retained-hook-context'\n")
        .map_err(super::super::storage)?;
    let user = home.join(".claude/settings.json");
    std::fs::write(&user, settings(&script, "unused").to_string())
        .map_err(super::super::storage)?;
    let (signal, mut received) = futures::channel::mpsc::unbounded();
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Events(vec![ScriptedEvent::Pending {}])])
            .with_context_capacity(capacity())
            .with_dispatch_observer(move |_| {
                signal.unbounded_send(()).unwrap();
            }),
    );
    let config = |provider: Arc<ScriptedModelProvider>| -> Result<LocalHarnessConfig> {
        Ok(checkout_config(&workspace, (model()?, provider))?
            .state_directory(root.path().join("state"))
            .settings(SettingsConfig::new(&workspace, &home).codex_home(home.join(".codex"))))
    };
    let harness = LocalHarness::open(config(provider.clone())?).await?;
    let task = super::context::admitted(harness.submit(OperationId::new(), "recover").await?)?;
    let mut pending = Box::pin(harness.resume(task));
    tokio::select! {
        signal = received.next() => assert_eq!(signal, Some(())),
        result = &mut pending => panic!("provider did not remain pending: {result:?}"),
    }
    drop(pending);
    assert!(request_has(&provider, 0, "retained-hook-context")?);
    // The fixture owns the project backend across client reopen. A cancelled
    // read may still be releasing its last native store handle; that teardown
    // is independent of reloading the task stream and its retained hook output.
    let project_backend = harness.workspace().unwrap().store.clone();
    drop(harness);
    // Recovery has neither the current settings nor the executable available.
    std::fs::remove_file(&user).map_err(super::super::storage)?;
    std::fs::remove_file(&script).map_err(super::super::storage)?;
    let recovered = Arc::new(
        ScriptedModelProvider::new([])
            .with_context_capacity(capacity())
            .with_reconciliations([ScriptedReconciliation {
                orphan: true,
                observed: vec![],
                continuation: Some(vec![
                    ModelEvent::Content {
                        delta: "recovered".into(),
                    },
                    ModelEvent::Completed {
                        metadata: Value::Null,
                    },
                ]),
            }]),
    );
    let harness = LocalHarness::open(config(recovered.clone())?).await?;
    drop(project_backend);
    assert_eq!(harness.resume(task).await?.text, "recovered");
    assert!(recovered.requests().is_empty());
    assert_eq!(
        std::fs::read_to_string(workspace.join("hook-runs")).map_err(super::super::storage)?,
        "started\n"
    );
    Ok(())
}

#[tokio::test]
async fn start_failures_are_retained_and_only_explicit_failure_policy_blocks() -> Result<()> {
    // The workflow runs on a worker, so logging uses the process subscriber.
    // Isolate that subscriber rather than changing another parallel test's log.
    if !std::env::args().any(|arg| arg == "hook-log-child") {
        let child = std::process::Command::new(std::env::current_exe().map_err(super::super::storage)?)
            .args(["--exact", "local::tests::hook_lifecycle::start_failures_are_retained_and_only_explicit_failure_policy_blocks",
                "--nocapture", "--skip", "hook-log-child"])
            .output().map_err(super::super::storage)?;
        assert!(
            child.status.success(),
            "{}{}",
            String::from_utf8_lossy(&child.stdout),
            String::from_utf8_lossy(&child.stderr)
        );
        return Ok(());
    }
    let warnings = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = Arc::clone(&warnings);
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .with_writer(move || WarningWriter(Arc::clone(&sink)))
        .finish();
    tracing::subscriber::set_global_default(subscriber).map_err(super::super::storage)?;
    use crate::task_volume::{FileKey, Query, TaskFile};
    for block in [false, true] {
        warnings.lock().unwrap().clear();
        let root = tempfile::tempdir().map_err(super::super::storage)?;
        let workspace = root.path().join("workspace");
        let home = root.path().join("home");
        std::fs::create_dir(&workspace).map_err(super::super::storage)?;
        std::fs::create_dir_all(home.join(".claude")).map_err(super::super::storage)?;
        let hooks = json!({"hooks":{"SessionStart":[{"hooks":[{
            "type":"command", "command":"missing-lifecycle-executable",
            "args":[], "onFailure":if block { "block" } else { "continue" }
        }]}]}});
        std::fs::write(home.join(".claude/settings.json"), hooks.to_string())
            .map_err(super::super::storage)?;
        let provider = text();
        let harness = LocalHarness::open(
            checkout_config(&workspace, (model()?, provider.clone()))?
                .state_directory(root.path().join("state"))
                .settings(SettingsConfig::new(&workspace, &home).codex_home(home.join(".codex"))),
        )
        .await?;
        let task = super::context::admitted(harness.submit(OperationId::new(), "guard").await?)?;
        let result = harness.resume(task).await;
        let warning_text = String::from_utf8(warnings.lock().unwrap().clone()).unwrap();
        assert!(
            warning_text.contains("configured hook failed"),
            "{warning_text}"
        );
        assert!(warning_text.contains("cause=NotFound"), "{warning_text}");
        assert_eq!(result.is_err(), block, "{result:?}");
        assert_eq!(provider.requests().len(), usize::from(!block));
        let files = harness.task_files.inner.snapshot(&Query::Root, 64).await?;
        let retained = files.files.iter().find(|file| matches!(&file.key,
            FileKey::Task { file:TaskFile::Context(id), .. } if id.as_str().starts_with("hook-") && id.as_str().ends_with("-outcome")))
            .expect("missing hook outcome");
        let output: crate::hooks::HookOutput =
            serde_json::from_slice(&retained.bytes).map_err(super::super::storage)?;
        assert_eq!(output.blocks(), block);
        assert_eq!(output.diagnostics.len(), 1);
        assert!(
            matches!(
                output.diagnostics[0].cause,
                crate::ProcessError::NotFound { .. }
            ),
            "{output:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn aggregate_capture_limits_retain_a_typed_outcome_instead_of_an_unresolved_claim()
-> Result<()> {
    use crate::task_volume::{FileKey, Query, TaskFile};
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    let workspace = root.path().join("workspace");
    let home = root.path().join("home");
    std::fs::create_dir(&workspace).map_err(super::super::storage)?;
    std::fs::create_dir_all(home.join(".claude")).map_err(super::super::storage)?;
    let script = root.path().join("flood.sh");
    std::fs::write(
        &script,
        "cat >/dev/null\nprintf 'started\\n' >> hook-runs\nhead -c 65536 /dev/zero\n",
    )
    .map_err(super::super::storage)?;
    let command = json!({"type":"command","command":"sh","args":[script],"onFailure":"block"});
    let settings = json!({"hooks":{"SessionStart":[{"hooks":[command.clone(),command]}]}});
    std::fs::write(home.join(".claude/settings.json"), settings.to_string())
        .map_err(super::super::storage)?;
    let provider = text();
    let harness = LocalHarness::open(
        checkout_config(&workspace, (model()?, provider.clone()))?
            .without_default_tools()
            .state_directory(root.path().join("state"))
            .limits(crate::conversation::Limits {
                file_bytes: 4096,
                ..Default::default()
            })
            .settings(SettingsConfig::new(&workspace, &home).codex_home(home.join(".codex"))),
    )
    .await?;
    let task =
        super::context::admitted(harness.submit(OperationId::new(), "bounded guards").await?)?;
    assert!(harness.resume(task).await.is_err());
    let files = harness.task_files.inner.snapshot(&Query::Root, 64).await?;
    let outcome = files
        .files
        .iter()
        .find(|file| {
            matches!(&file.key,
        FileKey::Task { file:TaskFile::Context(id), .. } if id.as_str().ends_with("-outcome"))
        })
        .expect("capture limit left an unresolved hook claim");
    assert!(outcome.bytes.len() <= 4096);
    let output: crate::hooks::HookOutput =
        serde_json::from_slice(&outcome.bytes).map_err(super::super::storage)?;
    assert!(output.blocks());
    assert!(
        output
            .diagnostics
            .iter()
            .all(|failure| matches!(failure.cause, crate::ProcessError::OutputLimit { .. }))
    );
    let before = std::fs::read(workspace.join("hook-runs")).map_err(super::super::storage)?;
    assert_eq!(before, b"started\nstarted\n");
    assert!(harness.resume(task).await.is_err());
    assert_eq!(
        std::fs::read(workspace.join("hook-runs")).map_err(super::super::storage)?,
        before
    );
    assert!(provider.requests().is_empty());
    Ok(())
}

/// Names below the checkout's `.volumes/`; an absent or empty directory is not
/// visible to `git status`, while a mount registry or projection is.
/// Volume entries left in the checkout, other than the checkout's own
/// incarnation record, which persists by design and must hold only its token.
fn volume_residue(workspace: &Path) -> Result<Vec<String>> {
    let incarnation = Path::new(crate::local::workspace::volumes::INCARNATION_PATH);
    let owned = incarnation
        .file_name()
        .expect("incarnation leaf")
        .to_string_lossy();
    let mut residue = Vec::new();
    let entries = match std::fs::read_dir(workspace.join(".volumes")) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(residue),
        Err(error) => return Err(super::super::storage(error)),
    };
    for entry in entries {
        let name = entry
            .map_err(super::super::storage)?
            .file_name()
            .to_string_lossy()
            .into_owned();
        if name != owned {
            residue.push(name);
            continue;
        }
        let record = std::fs::read_dir(workspace.join(incarnation))
            .map_err(super::super::storage)?
            .map(|entry| {
                Ok(entry
                    .map_err(super::super::storage)?
                    .file_name()
                    .to_string_lossy()
                    .into_owned())
            })
            .collect::<Result<Vec<_>>>()?;
        if record != ["token"] {
            residue.push(format!("{name}/{}", record.join(",")));
        }
    }
    Ok(residue)
}

async fn open_checkout(
    root: &Path,
    name: &str,
    home: &Path,
    provider: &Arc<ScriptedModelProvider>,
) -> Result<LocalHarness> {
    LocalHarness::open(
        checkout_config(&root.join(name), (model()?, provider.clone()))?
            .state_directory(root.join(format!("state-{name}")))
            .settings(SettingsConfig::new(root.join(name), home).codex_home(home.join(".codex"))),
    )
    .await
}

// Hooks and bash share one native execution scope that projects bound volumes
// on entry and retires them on exit. Both real turns leave a clean checkout,
// before and after the host disconnects.
#[tokio::test]
async fn hook_and_bash_turns_leave_a_clean_checkout() -> Result<()> {
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    let home = root.path().join("home");
    std::fs::create_dir_all(home.join(".claude")).map_err(super::super::storage)?;
    let script = root.path().join("hook.sh");
    std::fs::write(&script, "cat >/dev/null\nprintf '%s' \"$1\"\n")
        .map_err(super::super::storage)?;
    std::fs::write(
        home.join(".claude/settings.json"),
        settings(&script, "hook-ran").to_string(),
    )
    .map_err(super::super::storage)?;
    for (name, bash) in [("hooked", false), ("bash", true)] {
        let workspace = root.path().join(name);
        std::fs::create_dir(&workspace).map_err(super::super::storage)?;
        // A Git checkout sees no untracked mount point while one is live.
        let status = std::process::Command::new("git")
            .args(["init", "-q"])
            .current_dir(&workspace)
            .status()
            .map_err(super::super::storage)?;
        assert!(status.success());
        let turns = bash
            .then(|| {
                super::checked_commands::call(
                    0,
                    "bash",
                    "test -d .volumes/scratch && printf 'MOUNTED STATUS[%s]'                      \"$(git status --porcelain --untracked-files=all)\"",
                )
            })
            .into_iter()
            .chain([ScriptedTurn::Text("done".into())]);
        let provider = scripted(turns);
        let harness = open_checkout(root.path(), name, &home, &provider).await?;
        harness.run("go").await?;
        assert!(request_has(&provider, 0, "hook-ran")?);
        if bash {
            assert!(request_has(&provider, 1, "MOUNTED STATUS[]")?);
        }
        let exclude = std::fs::read_to_string(workspace.join(".git/info/exclude"))
            .map_err(super::super::storage)?;
        assert_eq!(
            exclude
                .lines()
                .filter(|line| *line == "/.volumes/scratch")
                .count(),
            1,
            "{name}: {exclude}"
        );
        assert!(
            !exclude
                .lines()
                .any(|line| matches!(line, "/.volumes" | "/.volumes/"))
        );
        let authored = workspace.join(".volumes/user-owned");
        std::fs::write(&authored, "keep").map_err(super::super::storage)?;
        let ignored = std::process::Command::new("git")
            .args(["check-ignore", "--no-index", "-q", "--"])
            .arg(&authored)
            .current_dir(&workspace)
            .status()
            .map_err(super::super::storage)?;
        assert_eq!(ignored.code(), Some(1), "{name}: authored sibling hidden");
        std::fs::remove_file(&authored).map_err(super::super::storage)?;
        assert!(!workspace.join(".gitignore").exists());
        assert_eq!(volume_residue(&workspace)?, Vec::<String>::new(), "{name}");
        harness.disconnect().await?;
        assert_eq!(volume_residue(&workspace)?, Vec::<String>::new(), "{name}");
    }
    Ok(())
}

// Native cleanup owns its projection, not arbitrary checkout or runtime files.
#[tokio::test]
async fn native_cleanup_preserves_preexisting_checkout_artifacts() -> Result<()> {
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    let home = root.path().join("home");
    let workspace = root.path().join("preserved");
    std::fs::create_dir_all(workspace.join(".volumes")).map_err(super::super::storage)?;
    std::fs::create_dir(workspace.join("__pycache__")).map_err(super::super::storage)?;
    std::fs::create_dir_all(home.join(".claude")).map_err(super::super::storage)?;
    let authored = workspace.join(".volumes/user-owned");
    let bytecode = workspace.join("__pycache__/user-owned.pyc");
    std::fs::write(&authored, b"authored bytes").map_err(super::super::storage)?;
    std::fs::write(&bytecode, b"preexisting runtime bytes").map_err(super::super::storage)?;
    let status = std::process::Command::new("git")
        .args(["init", "-q"])
        .current_dir(&workspace)
        .status()
        .map_err(super::super::storage)?;
    assert!(status.success());
    let script = root.path().join("hook.sh");
    std::fs::write(&script, "cat >/dev/null\nprintf hook-ran\n").map_err(super::super::storage)?;
    std::fs::write(
        home.join(".claude/settings.json"),
        settings(&script, "unused").to_string(),
    )
    .map_err(super::super::storage)?;
    let provider = scripted([
        super::checked_commands::call(0, "bash", "test -d .volumes/scratch && printf mounted"),
        ScriptedTurn::Text("done".into()),
    ]);
    let harness = open_checkout(root.path(), "preserved", &home, &provider).await?;
    assert_eq!(harness.run("go").await?.text, "done");
    assert!(request_has(&provider, 0, "hook-ran")?);
    assert!(request_has(&provider, 1, "mounted")?);
    harness.disconnect().await?;
    assert_eq!(
        std::fs::read(&authored).map_err(super::super::storage)?,
        b"authored bytes"
    );
    assert_eq!(
        std::fs::read(&bytecode).map_err(super::super::storage)?,
        b"preexisting runtime bytes"
    );
    assert_eq!(volume_residue(&workspace)?, vec!["user-owned".to_owned()]);
    let status = std::process::Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=all"])
        .current_dir(&workspace)
        .output()
        .map_err(super::super::storage)?;
    assert!(status.status.success());
    let status = String::from_utf8(status.stdout).map_err(super::super::storage)?;
    assert!(status.contains("?? .volumes/user-owned"), "{status}");
    assert!(status.contains("?? __pycache__/user-owned.pyc"), "{status}");
    assert!(!workspace.join(".gitignore").exists());
    Ok(())
}

// Negative control: a projection that escapes the execution scope is exactly
// the residue the clean-checkout detector above must see.
#[cfg(feature = "native-execution")]
#[tokio::test]
async fn a_projection_outside_the_execution_scope_is_visible_residue() -> Result<()> {
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    let home = root.path().join("home");
    std::fs::create_dir_all(root.path().join("leaky")).map_err(super::super::storage)?;
    let harness = open_checkout(root.path(), "leaky", &home, &scripted([])).await?;
    let mounts = Arc::clone(&harness.workspace.as_ref().unwrap().mounts);
    crate::local::workspace::volumes::project_unscoped(&mounts).await?;
    assert!(
        !volume_residue(&root.path().join("leaky"))?.is_empty(),
        "negative control left no visible projection"
    );
    crate::local::workspace::volumes::retire(&mounts).await?;
    assert_eq!(
        volume_residue(&root.path().join("leaky"))?,
        Vec::<String>::new()
    );
    harness.disconnect().await?;
    Ok(())
}

// A reopened checkout's hooks see volume data persisted by an earlier bash
// turn: the shared scope projects it for the hook, then retires it.
#[tokio::test]
async fn reopened_start_hooks_read_persisted_scratch_data() -> Result<()> {
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    let home = root.path().join("home");
    std::fs::create_dir_all(home.join(".claude")).map_err(super::super::storage)?;
    let workspace = root.path().join("reopen");
    std::fs::create_dir(&workspace).map_err(super::super::storage)?;
    let first = scripted([
        super::checked_commands::call(
            0,
            "bash",
            "printf SCRATCH-NOTE-WREN > .volumes/scratch/note",
        ),
        ScriptedTurn::Text("written".into()),
    ]);
    let harness = open_checkout(root.path(), "reopen", &home, &first).await?;
    assert_eq!(harness.run("write the note").await?.text, "written");
    harness.disconnect().await?;
    assert_eq!(volume_residue(&workspace)?, Vec::<String>::new());
    let script = root.path().join("read-note.sh");
    std::fs::write(&script, "cat >/dev/null\ncat .volumes/scratch/note\n")
        .map_err(super::super::storage)?;
    std::fs::write(
        home.join(".claude/settings.json"),
        settings(&script, "unused").to_string(),
    )
    .map_err(super::super::storage)?;
    let second = scripted([ScriptedTurn::Text("read".into())]);
    let harness = open_checkout(root.path(), "reopen", &home, &second).await?;
    assert_eq!(harness.run("read the note").await?.text, "read");
    assert!(request_has(&second, 0, "SCRATCH-NOTE-WREN")?);
    assert_eq!(volume_residue(&workspace)?, Vec::<String>::new());
    harness.disconnect().await?;
    Ok(())
}
