use super::*;
use crate::{
    context::{ContextManifest, ContextPipeline},
    conversation::{ContentResidencyVerifier, FileRef, Limits},
    hooks::{HookFailure, HookRule, HookRun, Hooks},
    native_settings::SettingsConfig,
};

async fn pinned(
    harness: &LocalHarness,
    operation: OperationId,
) -> Result<(FileRef, Vec<HookRule>)> {
    // Recover solely from the retained production prompt command, not a saved
    // test reference or the newly opened harness's current configuration.
    let (_, command) = harness
        .recover_turn_inputs(operation)
        .await?
        .expect("prompt input missing");
    let reader = harness.context_reader()?;
    let bytes = reader
        .read(&command.payload, harness.limits.file_bytes, None)?
        .await?;
    let command: crate::filesystem::ModelTaskCommand =
        serde_json::from_slice(&bytes).map_err(super::super::storage)?;
    let (_, stage) = ContextManifest::bind(
        command.context_manifest,
        reader.as_ref(),
        harness.tools().clone(),
        harness.limits,
    )
    .await?;
    let pipeline = ContextPipeline::default().with(stage);
    let mut sources = pipeline.configuration_sources();
    let file = sources
        .next()
        .expect("bound hook configuration missing")
        .clone();
    assert!(sources.next().is_none());
    assert_eq!(file.volume(), harness.journal_volume());
    assert!(file.path().starts_with("inputs/config/hooks/"));
    assert_eq!(file.descriptor().media_type(), Hooks::MEDIA_TYPE);
    let bytes = reader.read(&file, harness.limits.file_bytes, None)?.await?;
    let hooks: Hooks = serde_json::from_slice(&bytes).map_err(super::super::storage)?;
    assert_eq!(
        serde_json::to_vec(&hooks)
            .map_err(super::super::storage)?
            .as_slice(),
        bytes.as_ref()
    );
    let rules: Vec<HookRule> = serde_json::from_slice(&bytes).map_err(super::super::storage)?;
    Ok((file, rules))
}

fn settings(command: &str) -> String {
    json!({"hooks":{"PreToolUse":[{"matcher":"Bash","hooks":[{
        "type":"command","command":command,"args":["one argument"],"onFailure":"block"
    }]}]}})
    .to_string()
}

fn guard(rules: &[HookRule], executable: &str) {
    assert_eq!(
        rules.len(),
        1,
        "untrusted project hooks entered the snapshot"
    );
    assert_eq!(rules[0].matcher, "Bash");
    assert_eq!(rules[0].hooks.len(), 1);
    assert_eq!(rules[0].hooks[0].failure, HookFailure::Block);
    assert_eq!(
        rules[0].hooks[0].run,
        HookRun::Executable {
            executable: executable.into(),
            argv: vec!["one argument".into()]
        }
    );
}

#[tokio::test]
async fn queued_prompts_keep_normalized_user_hooks_after_settings_change_and_reopen() -> Result<()>
{
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    let workspace = root.path().join("workspace");
    let home = root.path().join("home");
    std::fs::create_dir_all(workspace.join(".claude")).map_err(super::super::storage)?;
    std::fs::create_dir_all(home.join(".claude")).map_err(super::super::storage)?;
    std::fs::write(
        workspace.join(".claude/settings.json"),
        settings("untrusted-project-guard"),
    )
    .map_err(super::super::storage)?;
    std::fs::write(home.join(".claude/settings.json"), settings("user-guard-A"))
        .map_err(super::super::storage)?;
    let provider = scripted([
        ScriptedTurn::Text("original".into()),
        ScriptedTurn::Text("replacement".into()),
    ]);
    let config = || -> Result<LocalHarnessConfig> {
        Ok(
            crate::test_support::local_harness_config(&workspace, (model()?, provider.clone()))?
                .settings(SettingsConfig::new(&workspace, &home).codex_home(home.join(".codex")))
                .state_directory(root.path().join("state")),
        )
    };
    let harness = LocalHarness::open(config()?).await?;
    let operation = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) =
        harness.submit(operation, "original prompt").await?
    else {
        panic!("prompt was not admitted");
    };
    let (original, rules) = pinned(&harness, operation).await?;
    guard(&rules, "user-guard-A");
    assert!(provider.requests().is_empty());
    let hooks: Hooks =
        serde_json::from_slice(&serde_json::to_vec(&rules).map_err(super::super::storage)?)
            .map_err(super::super::storage)?;
    let journal = crate::filesystem::workspace_ref(
        harness.journal.provider().clone(),
        &harness.journal.storage_name(&mut None)?,
    )?;
    let generation = harness.files.resolve(&journal, None).await?.generation;
    for limits in [
        crate::conversation::Limits {
            path_bytes: 64,
            ..harness.limits
        },
        crate::conversation::Limits {
            file_bytes: 1,
            ..harness.limits
        },
    ] {
        let error = super::super::settings::capture_hooks(
            &hooks,
            harness.input_publisher()?.as_ref(),
            limits,
        )
        .await
        .err()
        .expect("out-of-bounds hook file was accepted");
        assert!(matches!(error, Error::Invalid(_)), "{error:?}");
        assert_eq!(
            harness.files.resolve(&journal, None).await?.generation,
            generation,
            "rejected capture published a file"
        );
    }
    drop(harness);
    std::fs::write(home.join(".claude/settings.json"), settings("user-guard-B"))
        .map_err(super::super::storage)?;
    let reopened = LocalHarness::open(config()?).await?;
    let (recovered, rules) = pinned(&reopened, operation).await?;
    assert_eq!(recovered, original);
    guard(&rules, "user-guard-A");
    assert_eq!(reopened.resume(task_id).await?.text, "original");
    let next = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) =
        reopened.submit(next, "next prompt").await?
    else {
        panic!("prompt was not admitted");
    };
    let (current, rules) = pinned(&reopened, next).await?;
    assert_ne!(current, original);
    guard(&rules, "user-guard-B");
    assert_eq!(reopened.resume(task_id).await?.text, "replacement");
    let requests = serde_json::to_string(&provider.requests()).map_err(super::super::storage)?;
    assert!(
        !requests.contains("user-guard-"),
        "configuration became model context"
    );
    assert!(!requests.contains("untrusted-project-guard"));
    Ok(())
}

async fn oversized_current_hooks(explicit_override: bool) -> Result<()> {
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    let workspace = root.path().join("workspace");
    let home = root.path().join("home");
    std::fs::create_dir(&workspace).map_err(super::super::storage)?;
    std::fs::create_dir_all(home.join(".claude")).map_err(super::super::storage)?;
    let settings_path = home.join(".claude/settings.json");
    std::fs::write(&settings_path, settings("guard-A")).map_err(super::super::storage)?;
    let provider = scripted([ScriptedTurn::Text("original context".into())]);
    let limits = Limits {
        file_bytes: 8192,
        ..Default::default()
    };
    let config = || -> Result<LocalHarnessConfig> {
        Ok(
            crate::test_support::local_harness_config(&workspace, (model()?, provider.clone()))?
                .without_default_tools()
                .limits(limits)
                .state_directory(root.path().join("state"))
                .settings(SettingsConfig::new(&workspace, &home).codex_home(home.join(".codex"))),
        )
    };
    let harness = LocalHarness::open(config()?).await?;
    let operation = OperationId::new();
    let task = if explicit_override {
        harness.stage_turn(operation, "retained prompt").await?;
        None
    } else {
        Some(super::context::admitted(
            harness.submit(operation, "retained prompt").await?,
        )?)
    };
    let (reference, _) = super::context::manifest(&harness, operation).await?;
    let (hooks, rules) = pinned(&harness, operation).await?;
    guard(&rules, "guard-A");
    assert!(provider.requests().is_empty());
    drop(harness);
    // Valid selected configuration fits the settings read bound, but cannot fit
    // the task's file bound. It must matter only to a new automatic selection.
    let large = format!("guard-B-{}", "x".repeat(limits.file_bytes as usize));
    std::fs::write(&settings_path, settings(&large)).map_err(super::super::storage)?;
    let mut selected = config()?;
    if explicit_override {
        selected = selected.context_manifest(reference.clone());
    }
    let reopened = LocalHarness::open(selected).await?;
    assert!(
        reopened.hooks.encode(limits.file_bytes).is_err(),
        "fixture no longer exceeds the task file bound"
    );
    if explicit_override {
        let next = OperationId::new();
        let next_task =
            super::context::admitted(reopened.submit(next, "explicit selection").await?)?;
        let (_, command) = reopened.recover_turn_inputs(next).await?.unwrap();
        let bytes = reopened
            .context_reader()?
            .read(&command.payload, limits.file_bytes, None)?
            .await?;
        let command: crate::filesystem::ModelTaskCommand =
            serde_json::from_slice(&bytes).map_err(super::super::storage)?;
        assert_eq!(command.context_manifest, reference);
        let (selected_hooks, rules) = pinned(&reopened, next).await?;
        assert_eq!(selected_hooks, hooks);
        guard(&rules, "guard-A");
        assert_eq!(reopened.resume(next_task).await?.text, "original context");
    } else {
        // Retry the original production submission without supplying any old
        // manifest/configuration refs. Recovery must find the retained files.
        let recovered_task =
            super::context::admitted(reopened.submit(operation, "retained prompt").await?)?;
        assert_eq!(Some(recovered_task), task);
        let (retained, _) = super::context::manifest(&reopened, operation).await?;
        assert_eq!(retained, reference);
        let (selected_hooks, rules) = pinned(&reopened, operation).await?;
        assert_eq!(selected_hooks, hooks);
        guard(&rules, "guard-A");
        assert_eq!(
            reopened.resume(task.unwrap()).await?.text,
            "original context"
        );
        let next = OperationId::new();
        assert!(matches!(
            reopened.submit(next, "new selection").await,
            Err(Error::Invalid(_))
        ));
        assert!(reopened.recover_turn_inputs(next).await?.is_none());
    }
    assert_eq!(provider.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn reopening_retained_prompts_does_not_publish_oversized_current_hooks() -> Result<()> {
    oversized_current_hooks(false).await
}

#[tokio::test]
async fn explicit_manifests_do_not_publish_unused_current_hooks() -> Result<()> {
    oversized_current_hooks(true).await
}
