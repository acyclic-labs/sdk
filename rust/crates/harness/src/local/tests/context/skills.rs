use super::*;

#[tokio::test]
async fn unchanged_checkout_generation_reuses_skill_catalog_without_rescanning() -> Result<()> {
    let root = tempfile::tempdir().map_err(super::super::super::storage)?;
    let skill = root.path().join(".agents/skills/greet2/SKILL.md");
    std::fs::create_dir_all(skill.parent().unwrap()).map_err(super::super::super::storage)?;
    std::fs::write(
        &skill,
        "---\nname: greet2\ndescription: Greet formally\n---\nbody",
    )
    .map_err(super::super::super::storage)?;
    let harness = LocalHarness::open(
        checkout_config(root.path(), (model()?, text()))?.without_default_tools(),
    )
    .await?;
    let first = harness.skills().await?;
    assert_eq!(first.len(), 1);
    // No capture boundary has advanced the checkout generation. A rescan would
    // parse this malformed file and fail, instead of reusing the pinned catalog.
    std::fs::write(skill, "malformed replacement").map_err(super::super::super::storage)?;
    assert_eq!(harness.skills().await?, first);
    Ok(())
}

#[tokio::test]
async fn binding_an_existing_runtime_discovers_checkout_and_user_skills() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(super::super::super::storage)?;
    let home = tempfile::tempdir().map_err(super::super::super::storage)?;
    for (base, name) in [(checkout.path(), "project"), (home.path(), "user")] {
        let path = base.join(format!(".agents/skills/{name}/SKILL.md"));
        std::fs::create_dir_all(path.parent().unwrap()).map_err(super::super::super::storage)?;
        std::fs::write(
            path,
            format!("---\nname: {name}\ndescription: Greet formally\n---\nbody"),
        )
        .map_err(super::super::super::storage)?;
    }
    let harness = LocalHarness::open(
        checkout_config(checkout.path(), (model()?, text()))?.without_default_tools(),
    )
    .await?;
    let stream = harness.task_stream();
    let storage = harness.workspace.as_ref().unwrap().filesystem.clone();
    let volume = harness.task_files().volume().clone();
    let verifier = harness.verifier.clone();
    let signed = harness.signed.clone();
    let LocalHarness { runtime, files, .. } = harness;
    let runtime = Arc::try_unwrap(runtime)
        .map_err(|_| Error::Conflict("fixture runtime is still shared".into()))?;
    let provider = scripted([ScriptedTurn::Text("done".into())]);
    let config = checkout_config(checkout.path(), (model()?, provider.clone()))?
        .volume(volume)
        .without_default_tools()
        .goal_continuations(None)
        .settings(
            crate::native_settings::SettingsConfig::new(checkout.path(), home.path())
                .codex_home(home.path().join(".codex")),
        );
    let bound = LocalHarness::bind(
        config,
        runtime,
        files,
        (storage, stream),
        (&verifier, &signed),
        Some(checkout.path()),
    )
    .await?;
    assert_eq!(bound.run("inventory").await?.text, "done");
    let supplied =
        serde_json::to_string(&provider.requests()).map_err(super::super::super::storage)?;
    assert!(supplied.contains("Greet formally"));
    let catalog = bound.skills().await?;
    assert_eq!(catalog.len(), 2);
    assert!(catalog.iter().any(|skill| skill.name == "project"));
    assert!(catalog.iter().any(|skill| skill.name == "user"));
    Ok(())
}
