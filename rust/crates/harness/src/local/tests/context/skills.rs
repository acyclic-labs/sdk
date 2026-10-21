use super::*;

#[tokio::test]
async fn hard_linked_skill_pins_the_shadow_identity_without_loading_its_body() -> Result<()> {
    let root = tempfile::tempdir().map_err(super::super::super::storage)?;
    let skill = root.path().join(".agents/skills/greet2/SKILL.md");
    std::fs::create_dir_all(skill.parent().unwrap()).map_err(super::super::super::storage)?;
    std::fs::write(
        &skill,
        "---\nname: greet2\ndescription: Original\n---\nbody",
    )
    .map_err(super::super::super::storage)?;
    std::fs::hard_link(&skill, root.path().join("alias.md"))
        .map_err(super::super::super::storage)?;
    let harness = LocalHarness::open(
        checkout_config(root.path(), (model()?, text()))?.without_default_tools(),
    )
    .await?;
    let view = harness.workspace.as_ref().unwrap().view();
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
        work: &work,
        cancellation: &cancellation,
    };
    let body = "---\nname: greet2\ndescription: Updated through alias\n---\nRetained shadow body";
    view.write(
        "/alias.md",
        acyclic_stream::OwnedBytes::copy_from_slice(body.as_bytes()),
        acyclic_fs::IdempotencyKey::new(),
        acyclic_fs::PublicationPermit::Unrestricted,
        &mut accounting,
    )
    .await?;
    let cut = view.snapshot(&mut accounting).await?;
    let skills = harness.skills(None).await?;
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].description, "Updated through alias");
    // Changing the authored alias after discovery must not change the retained cut.
    view.write(
        "/alias.md",
        acyclic_stream::OwnedBytes::from_static(b"later body"),
        acyclic_fs::IdempotencyKey::new(),
        acyclic_fs::PublicationPermit::Unrestricted,
        &mut accounting,
    )
    .await?;
    let reader = harness.context_reader()?;
    // Negative control for the former catalog pin: the original alias is not
    // present in the authored namespace, even though its shadow is readable.
    let former = crate::context::PinnedContextPath {
        root: crate::context::ContextRoot {
            volume: harness.project.clone(),
            directory: ".agents/skills".into(),
        },
        generation: crate::resources::GenerationRef::new(
            harness.project.provider(),
            &cut.authored_generation.digest().into_bytes(),
            None,
            None,
        )?
        .0,
        path: ".agents/skills/greet2/SKILL.md".into(),
    };
    assert!(former.read(reader.as_ref(), 4096).await.is_err());
    let (_, bytes) = skills[0].source.read(reader.as_ref(), 4096).await?;
    assert_eq!(bytes.as_ref(), body.as_bytes());
    Ok(())
}

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
    let discovery = harness.skills(None);
    assert!(std::mem::size_of_val(&discovery) <= 4096);
    let first = discovery.await?;
    assert_eq!(first.len(), 1);
    // No capture boundary has advanced the checkout generation. A rescan would
    // parse this malformed file and fail, instead of reusing the pinned catalog.
    std::fs::write(skill, "malformed replacement").map_err(super::super::super::storage)?;
    assert_eq!(harness.skills(None).await?, first);
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
    let volume = harness.journal.clone();
    let verifier = harness.verifier.clone();
    let files = harness.files.clone();
    let provider = scripted([ScriptedTurn::Text("done".into())]);
    let config = checkout_config(checkout.path(), (model()?, provider.clone()))?
        .volume(volume.clone())
        .without_default_tools()
        .goal_continuations(None)
        .settings(
            crate::native_settings::SettingsConfig::new(checkout.path(), home.path())
                .codex_home(home.path().join(".codex")),
        );
    // bind() composes one authenticated volume. Build that composition rather
    // than reusing open()'s runtime scope for three distinct private volumes.
    let (runtime, signed, _) =
        super::super::super::runtime::open(super::super::super::runtime::Configuration {
            files: files.clone(),
            workspace: None,
            stream: stream.clone(),
            task_volume: &volume,
            issuer: harness.issuer.as_ref().unwrap(),
            verifier: &verifier,
            agent: harness.signed.agent().unwrap(),
            project: &volume,
            scratch: &volume,
            journal: &volume,
            tools: &crate::tool::ToolRegistry::default(),
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            capabilities: &crate::Capabilities::default(),
            limits: config.limits,
            run_limits: config.run_limits,
            session_limits: config.session_limits,
            concurrency: config.concurrency,
            clock: config.clock.clone(),
        })
        .await?;
    let runtime = Arc::try_unwrap(runtime)
        .map_err(|_| Error::Conflict("fixture runtime is still shared".into()))?;
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
    let catalog = bound.skills(None).await?;
    assert_eq!(catalog.len(), 2);
    assert!(catalog.iter().any(|skill| skill.name == "project"));
    assert!(catalog.iter().any(|skill| skill.name == "user"));
    Ok(())
}

#[tokio::test]
async fn selected_child_authored_skill_loads_with_native_work_from_its_retained_cut() -> Result<()>
{
    let root = tempfile::tempdir().map_err(super::super::super::storage)?;
    let provider = scripted([
        super::super::checked_commands::call(0, "fs", "checkout -b skill-child"),
        ScriptedTurn::Text("done".into()),
    ]);
    let harness = LocalHarness::open(checkout_config(root.path(), (model()?, provider))?).await?;
    assert_eq!(harness.run("select a child").await?.text, "done");
    let workspace = harness.workspace.as_ref().unwrap();
    let selected = workspace.selected_view().await?.0.unwrap();
    assert_ne!(selected.workspace().id(), workspace.view().workspace().id());
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
        work: &work,
        cancellation: &cancellation,
    };
    let path = "/.agents/skills/child/SKILL.md";
    let body = "---\nname: child\ndescription: Selected child skill\n---\nRetained authored body";
    selected
        .write(
            path,
            acyclic_stream::OwnedBytes::copy_from_slice(body.as_bytes()),
            acyclic_fs::IdempotencyKey::new(),
            acyclic_fs::PublicationPermit::Unrestricted,
            &mut accounting,
        )
        .await?;
    let skills = harness.skills(None).await?;
    let [skill] = skills.as_slice() else {
        return Err(Error::Invalid("expected one selected child skill".into()));
    };
    assert_eq!(skill.description, "Selected child skill");
    // Advance the child after discovery; body loading must still use its cut.
    selected
        .write(
            path,
            acyclic_stream::OwnedBytes::from_static(b"later body"),
            acyclic_fs::IdempotencyKey::new(),
            acyclic_fs::PublicationPermit::Unrestricted,
            &mut accounting,
        )
        .await?;
    let reader = harness.context_reader()?;
    let io = crate::executor::PreparationIo::from_native_work(&work)?;
    let (_, bytes, _) = reader
        .read_private_path(
            &skill.source.root.volume,
            &skill.source.root.directory,
            &skill.source.path,
            Some(&skill.source.generation),
            4096,
            Some(&io),
        )?
        .await?;
    assert_eq!(bytes.as_ref(), body.as_bytes());
    assert_eq!(skill.source.root.volume, harness.journal);
    Ok(())
}
