//! Public checkout admission, provider visibility and deterministic reopen.
use super::*;
use crate::model::{ModelContent, ModelRole};
use std::sync::atomic::Ordering;

const NOW: u64 = 1_792_324_800_000;
struct Clock(u64);
impl acyclic_stream::UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        self.0
    }
}

fn git(root: &Path, args: &[&str]) -> Result<()> {
    let status = std::process::Command::new("git")
        .current_dir(root)
        .env("GIT_CONFIG_GLOBAL", root.join("missing-config"))
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .args([
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .status()
        .map_err(super::super::storage)?;
    assert!(status.success(), "git {args:?}");
    Ok(())
}

fn system_text(request: &crate::model::ModelRequest) -> String {
    request
        .messages
        .iter()
        .filter_map(|message| match &message.content {
            ModelContent::Text(text) if message.role == ModelRole::System => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn first_request_contains_each_environment_fact_and_resume_never_rediscovers() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let root = directory.path();
    git(
        root,
        &["init", "--quiet", "--initial-branch=environment-fixture"],
    )?;
    git(
        root,
        &["commit", "--quiet", "--allow-empty", "--message", "fixture"],
    )?;
    let canonical = root.canonicalize().map_err(super::super::storage)?;
    let cwd = canonical
        .to_string_lossy()
        .trim_start_matches(r"\\?\")
        .to_owned();
    let expected = [
        format!("<cwd>{cwd}</cwd>"),
        "<os>fixture-platform</os>".into(),
        "<shell>fixture-shell</shell>".into(),
        "<current_date>2026-10-18</current_date>".into(),
        "<timezone>UTC</timezone>".into(),
        "<git_status>branch environment-fixture; clean</git_status>".into(),
        "<model>selected-environment</model>".into(),
    ];
    // The scripted provider asserts on the first request at admission, before generation.
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Text("done".into())])
            .with_context_capacity(capacity())
            .with_token_counter(move |request| {
                assert_eq!(request.request().model.name, "selected-environment");
                let text = system_text(request.request());
                for fact in &expected {
                    assert!(text.contains(fact), "missing {fact}: {text}");
                }
                Ok(crate::context::ModelTokenCount::from_upper_bounds(
                    request.request_digest(),
                    u32::try_from(request.bytes().len()).unwrap(),
                    vec![0; request.request().messages.len()],
                ))
            }),
    );
    let mut config = checkout_config(root, (model()?, provider.clone()))?
        .state_directory(state.path())
        .clock(Arc::new(Clock(NOW)));
    config.environment_platform = crate::native_environment::PlatformPolicy::Fixture {
        os: "fixture-platform".into(),
        shell: "fixture-shell".into(),
    };
    let harness = LocalHarness::open(config).await?;
    let operation = OperationId::new();
    harness
        .retain_settings(operation, &json!({"model": "selected-environment"}))
        .await?;
    let task = super::context::admitted(harness.submit(operation, "where are you?").await?)?;
    let (reference, manifest) = super::context::manifest(&harness, operation).await?;
    assert!(manifest.environment.is_some());
    assert_eq!(harness.environment_reads.load(Ordering::SeqCst), 1);
    assert_eq!(harness.stage_context_manifest(operation).await?, reference);
    assert_eq!(harness.environment_reads.load(Ordering::SeqCst), 1);
    drop(harness);
    git(
        root,
        &["checkout", "--quiet", "-b", "changed-after-admission"],
    )?;
    std::fs::write(root.join("untracked"), "changed").map_err(super::super::storage)?;
    let mut config = checkout_config(root, (model()?, provider.clone()))?
        .state_directory(state.path())
        .clock(Arc::new(Clock(NOW + 86_400_000)));
    config.environment_platform = crate::native_environment::PlatformPolicy::Fixture {
        os: "changed-platform".into(),
        shell: "changed-shell".into(),
    };
    let reopened = LocalHarness::open(config).await?;
    assert_eq!(reopened.resume(task).await?.text, "done");
    assert_eq!(reopened.resume(task).await?.text, "done");
    assert_eq!(reopened.environment_reads.load(Ordering::SeqCst), 0);
    assert_eq!(provider.request_count(), 1);
    assert_eq!(
        super::context::manifest(&reopened, operation).await?,
        (reference, manifest)
    );
    // A new admission must collect the changed binding, unlike replay.
    let next = OperationId::new();
    reopened.stage_context_manifest(next).await?;
    let facts = super::context::manifest(&reopened, next)
        .await?
        .1
        .environment
        .unwrap();
    assert_eq!(facts.os, "changed-platform");
    assert_eq!(facts.current_date, "2026-10-19");
    assert!(
        facts
            .git_status
            .unwrap()
            .contains("changed-after-admission")
    );
    assert_eq!(reopened.environment_reads.load(Ordering::SeqCst), 1);
    reopened.close().await
}

#[tokio::test]
async fn volume_binding_does_not_claim_a_native_checkout_environment() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = text();
    let harness =
        LocalHarness::open(volume_config(directory.path(), project, provider.clone())?).await?;
    assert_eq!(harness.run("hello").await?.text, "done");
    assert!(!system_text(&provider.requests()[0]).contains("<environment_context>"));
    assert_eq!(harness.environment_reads.load(Ordering::SeqCst), 0);
    harness.close().await
}

#[tokio::test]
async fn child_admission_records_its_inherited_selected_model() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Text("child".into())])
            .with_context_capacity(capacity())
            .with_token_counter(|request| {
                assert_eq!(request.request().model.name, "inherited-environment");
                assert!(
                    system_text(request.request()).contains("<model>inherited-environment</model>")
                );
                Ok(crate::context::ModelTokenCount::from_upper_bounds(
                    request.request_digest(),
                    u32::try_from(request.bytes().len()).unwrap(),
                    vec![0; request.request().messages.len()],
                ))
            }),
    );
    let harness = LocalHarness::open(
        checkout_config(directory.path(), (model()?, provider.clone()))?
            .state_directory(state.path()),
    )
    .await?;
    let operation = OperationId::new();
    harness
        .retain_settings(operation, &json!({"model":"inherited-environment"}))
        .await?;
    let parent = super::context::admitted(harness.submit(operation, "parent").await?)?;
    // Keep the parent live so the public parented route prepares inheritance
    // before constructing the child's first model request.
    let child = super::context::admitted(
        harness
            .submit_to(OperationId::new(), Some(parent), "child")
            .await?,
    )?;
    assert_eq!(harness.resume(child).await?.text, "child");
    assert_eq!(provider.request_count(), 1);
    harness.close().await
}

#[tokio::test]
async fn logical_git_switch_never_retains_the_physical_branch_summary() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let root = directory.path();
    std::fs::write(root.join("marker"), "base").map_err(super::super::storage)?;
    git(root, &["init", "--quiet", "--initial-branch=main"])?;
    git(root, &["add", "--all"])?;
    git(root, &["commit", "--quiet", "--message", "fixture"])?;
    let provider = scripted([
        super::checked_commands::call(0, "fs", "git switch -c feature"),
        ScriptedTurn::Text("switched".into()),
        ScriptedTurn::Text("next".into()),
    ]);
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(root, (model()?, provider.clone()))?
            .state_directory(state.path()),
    )
    .await?;
    assert_eq!(harness.run("switch").await?.text, "switched");
    let branch = std::process::Command::new("git")
        .current_dir(root)
        .args(["branch", "--show-current"])
        .output()
        .map_err(super::super::storage)?;
    assert!(branch.status.success());
    assert_eq!(
        branch.stdout, b"main\n",
        "fixture needs distinct physical Git state"
    );
    let workspace = harness.workspace.as_ref().unwrap();
    let (selected, _) = workspace.selected_view().await?;
    assert!(selected.is_some(), "logical branch did not switch");
    assert_eq!(harness.run("next").await?.text, "next");
    let requests = provider.requests();
    let environment = system_text(requests.last().unwrap());
    assert!(
        !environment.contains("<git_status>"),
        "physical Git leaked into the selected view: {environment}"
    );
    harness.close().await
}
