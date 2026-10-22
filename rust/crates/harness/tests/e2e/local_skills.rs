use super::TestResult;
use acyclic_harness::{
    Error, LocalHarness, LocalHarnessConfig,
    context::{CompactionPolicy, ModelContextCapacity, SkillMetadata},
    model::{Model, ModelContent},
    native_settings::SettingsConfig,
    test_support::{ScriptedModelProvider, ScriptedTurn},
};
use std::{path::Path, sync::Arc};

fn write(root: &Path, path: &str, text: &str) -> TestResult {
    let path = root.join(path);
    std::fs::create_dir_all(path.parent().ok_or("missing parent")?)?;
    std::fs::write(path, text)?;
    Ok(())
}

fn config(
    root: &Path,
    home: &Path,
    state: &Path,
    provider: Arc<ScriptedModelProvider>,
) -> TestResult<LocalHarnessConfig> {
    Ok(LocalHarnessConfig::new(
        root,
        (
            Model::new(
                "scripted".into(),
                "test".into(),
                "1".into(),
                serde_json::Value::Null,
            )?,
            provider,
        ),
    )
    .hermetic()
    .state_directory(state)
    .without_default_tools()
    .compaction(CompactionPolicy::Disabled)
    .goal_continuations(None)
    .settings(SettingsConfig::new(root, home).codex_home(home.join(".codex"))))
}

fn provider(turns: usize) -> Arc<ScriptedModelProvider> {
    Arc::new(
        ScriptedModelProvider::new((0..turns).map(|_| ScriptedTurn::Text("done".into())))
            .with_context_capacity(ModelContextCapacity {
                context_tokens: 32_768,
                output_tokens: Some(4096),
            }),
    )
}

fn catalog(provider: &ScriptedModelProvider, index: usize) -> TestResult<Vec<SkillMetadata>> {
    let requests = provider.requests();
    let request = requests.get(index).ok_or("missing provider request")?;
    for message in &request.messages {
        if let ModelContent::Text(text) = &message.content
            && let Ok(skills) = serde_json::from_str::<Vec<SkillMetadata>>(text)
            && !skills.is_empty()
        {
            return Ok(skills);
        }
    }
    Err("first provider request is missing its skill catalog".into())
}

#[tokio::test]
async fn first_request_declares_project_and_user_skills_without_loading_bodies() -> TestResult {
    let root = super::local_workspace::temporary()?;
    let home = super::local_workspace::temporary()?;
    let state = super::local_workspace::temporary()?;
    for (base, path, name) in [
        (root.path(), ".agents/skills/greet2/SKILL.md", "greet2"),
        (root.path(), ".claude/skills/project/SKILL.md", "project"),
        (
            home.path(),
            ".agents/skills/user-agents/SKILL.md",
            "user-agents",
        ),
        (
            home.path(),
            ".claude/skills/user-claude/SKILL.md",
            "user-claude",
        ),
        (
            home.path(),
            ".codex/skills/user-codex/SKILL.md",
            "user-codex",
        ),
    ] {
        // The body is larger than the complete file limit. Admission can only
        // succeed if discovery uses a prefix and the model receives metadata.
        write(
            base,
            path,
            &format!(
                "---\nname: {name}\ndescription: Greet formally\n---\nBODY-MUST-STAY-LAZY{}",
                "x".repeat(100_000)
            ),
        )?;
    }
    let model = provider(1);
    let harness = LocalHarness::open(
        config(root.path(), home.path(), state.path(), model.clone())?.limits(
            acyclic_harness::conversation::Limits {
                file_bytes: 65_536,
                ..Default::default()
            },
        ),
    )
    .await?;
    assert_eq!(harness.run("inventory").await?.text, "done");
    let skills = catalog(&model, 0)?;
    assert_eq!(skills.len(), 5);
    for name in [
        "greet2",
        "project",
        "user-agents",
        "user-claude",
        "user-codex",
    ] {
        assert!(
            skills
                .iter()
                .any(|skill| skill.name == name && skill.description == "Greet formally")
        );
    }
    assert!(!serde_json::to_string(&model.requests())?.contains("BODY-MUST-STAY-LAZY"));
    Ok(())
}

#[tokio::test]
async fn overlapping_user_roots_are_scanned_once_before_the_first_request() -> TestResult {
    let root = super::local_workspace::temporary()?;
    let home = super::local_workspace::temporary()?;
    let state = super::local_workspace::temporary()?;
    write(
        home.path(),
        ".agents/skills/greet2/SKILL.md",
        "---\nname: greet2\ndescription: Greet formally\n---\nbody",
    )?;
    let model = provider(1);
    let harness = LocalHarness::open(
        config(root.path(), home.path(), state.path(), model.clone())?.settings(
            SettingsConfig::new(root.path(), home.path()).codex_home(home.path().join(".agents")),
        ),
    )
    .await?;
    assert_eq!(harness.run("inventory").await?.text, "done");
    let skills = catalog(&model, 0)?;
    let [skill] = skills.as_slice() else {
        return Err("expected one skill from the overlapping roots".into());
    };
    assert_eq!(skill.name, "greet2");
    assert_eq!(skill.description, "Greet formally");
    Ok(())
}

#[tokio::test]
async fn different_user_roots_still_reject_duplicate_skill_names() -> TestResult {
    let root = super::local_workspace::temporary()?;
    let home = super::local_workspace::temporary()?;
    let state = super::local_workspace::temporary()?;
    for prefix in [".agents", ".claude"] {
        write(
            home.path(),
            &format!("{prefix}/skills/greet2/SKILL.md"),
            "---\nname: greet2\ndescription: Greet formally\n---\nbody",
        )?;
    }
    let model = provider(1);
    let harness = LocalHarness::open(config(
        root.path(),
        home.path(),
        state.path(),
        model.clone(),
    )?)
    .await?;
    assert!(matches!(
        harness.run("inventory").await,
        Err(Error::Conflict(_))
    ));
    assert!(model.requests().is_empty());
    Ok(())
}

#[tokio::test]
async fn lazy_skill_source_survives_reopen_and_retains_materialized_bytes() -> TestResult {
    let root = super::local_workspace::temporary()?;
    let home = super::local_workspace::temporary()?;
    let state = super::local_workspace::temporary()?;
    let body = "---\nname: greet2\ndescription: Greet formally\n---\nRetained body";
    write(root.path(), ".agents/skills/greet2/SKILL.md", body)?;
    write(
        root.path(),
        ".agents/skills/unread/SKILL.md",
        "---\nname: unread\ndescription: Load later\n---\nOriginal body",
    )?;
    let model = provider(1);
    let harness = LocalHarness::open(config(
        root.path(),
        home.path(),
        state.path(),
        model.clone(),
    )?)
    .await?;
    harness.run("inventory").await?;
    let skills = catalog(&model, 0)?;
    drop(harness);
    let harness =
        LocalHarness::open(config(root.path(), home.path(), state.path(), provider(0))?).await?;
    let reader = harness.client_reader()?;
    let retained = skills
        .iter()
        .find(|skill| skill.name == "greet2")
        .ok_or("greet2 skill absent")?;
    let unread = skills
        .iter()
        .find(|skill| skill.name == "unread")
        .ok_or("unread skill absent")?;
    write(
        root.path(),
        ".agents/skills/unread/SKILL.md",
        "replacement body",
    )?;
    assert!(unread.source.read(reader.as_ref(), 4096).await.is_err());
    let (_, bytes) = retained.source.read(reader.as_ref(), 4096).await?;
    assert_eq!(bytes.as_ref(), body.as_bytes());
    write(
        root.path(),
        ".agents/skills/greet2/SKILL.md",
        "changed source",
    )?;
    // A materialized body must remain pinned after the source changes.
    let (_, bytes) = retained.source.read(reader.as_ref(), 4096).await?;
    assert_eq!(bytes.as_ref(), body.as_bytes());
    Ok(())
}

#[tokio::test]
async fn malformed_skill_is_a_typed_error_before_first_provider_dispatch() -> TestResult {
    for user in [false, true] {
        let root = super::local_workspace::temporary()?;
        let home = super::local_workspace::temporary()?;
        let state = super::local_workspace::temporary()?;
        write(
            if user { home.path() } else { root.path() },
            ".agents/skills/broken/SKILL.md",
            "---\nname: broken\n---\nmissing description",
        )?;
        let model = provider(1);
        let harness = LocalHarness::open(config(
            root.path(),
            home.path(),
            state.path(),
            model.clone(),
        )?)
        .await?;
        assert!(matches!(
            harness.run("inventory").await,
            Err(Error::Invalid(_))
        ));
        assert!(model.requests().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn volume_first_request_declares_skills_and_refreshes_after_generation_change() -> TestResult
{
    use acyclic_harness::{IdempotencyKey, filesystem::WorkspaceMutation};
    let root = super::local_workspace::temporary()?;
    let home = super::local_workspace::temporary()?;
    let state = super::local_workspace::temporary()?;
    let (host, volume, workspace, _) =
        super::local_workspace::seed(root.path(), &Default::default()).await?;
    host.apply(
        &workspace,
        None,
        &[
            WorkspaceMutation::CreateDirectory {
                path: "/.agents/skills/greet2".into(),
            },
            WorkspaceMutation::PutFile {
                path: "/.agents/skills/greet2/SKILL.md".into(),
                bytes: b"---\nname: greet2\ndescription: Greet formally\n---\nPRIVATE-BODY"
                    .to_vec()
                    .into(),
            },
        ],
        &IdempotencyKey::new("seed-skill")?,
    )
    .await?;
    let model = provider(2);
    let harness = LocalHarness::open(
        config(root.path(), home.path(), state.path(), model.clone())?.volume(volume),
    )
    .await?;
    harness.run("first inventory").await?;
    let first = catalog(&model, 0)?;
    let [skill] = first.as_slice() else {
        return Err("expected one skill".into());
    };
    assert_eq!(skill.description, "Greet formally");
    host.apply(
        &workspace,
        None,
        &[WorkspaceMutation::PutFile {
            path: "/.agents/skills/greet2/SKILL.md".into(),
            bytes: b"---\nname: greet2\ndescription: Updated generation\n---\nPRIVATE-BODY"
                .to_vec()
                .into(),
        }],
        &IdempotencyKey::new("update-skill")?,
    )
    .await?;
    harness.run("second inventory").await?;
    let second = catalog(&model, 1)?;
    let [skill] = second.as_slice() else {
        return Err("expected one updated skill".into());
    };
    assert_eq!(skill.description, "Updated generation");
    assert!(!serde_json::to_string(&model.requests())?.contains("PRIVATE-BODY"));
    Ok(())
}

#[tokio::test]
async fn large_skill_directory_stops_at_the_existing_discovery_bound() -> TestResult {
    let root = super::local_workspace::temporary()?;
    let home = super::local_workspace::temporary()?;
    let state = super::local_workspace::temporary()?;
    for index in 0..300 {
        write(
            root.path(),
            &format!(".agents/skills/s{index}/SKILL.md"),
            &format!("---\nname: s{index}\ndescription: bounded\n---\nbody"),
        )?;
    }
    let model = provider(1);
    let harness = LocalHarness::open(config(
        root.path(),
        home.path(),
        state.path(),
        model.clone(),
    )?)
    .await?;
    assert!(
        matches!(harness.run("inventory").await, Err(Error::Invalid(message)) if message.contains("bound"))
    );
    assert!(model.requests().is_empty());
    Ok(())
}
