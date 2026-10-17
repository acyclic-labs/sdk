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
