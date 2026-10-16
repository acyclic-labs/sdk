use super::*;
use crate::{
    context::ContextManifest,
    conversation::{ContentResidencyVerifier, FileRef},
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
    let bytes = reader
        .read(&command.context_manifest, harness.limits.file_bytes, None)?
        .await?;
    let manifest = ContextManifest::decode(&bytes, harness.limits)?;
    assert_eq!(manifest.sources.len(), 1);
    let file = manifest.sources.into_iter().next().unwrap();
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
                .settings(SettingsConfig::new(&workspace, &home).codex_home(home.join(".codex"))),
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
    assert_eq!(harness.hook_configuration.as_ref(), Some(&original));
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
    assert_ne!(reopened.hook_configuration.as_ref(), Some(&original));
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
