use super::*;
use acyclic_harness::client::{CommandControl, CommandFileRoot, CommandSession, CommandSources};
use acyclic_harness::task_volume::{FileKey, FileValue, Query, TaskVolume};
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;
use std::sync::Arc;

#[tokio::test]
async fn source_add_and_modify_are_followed_after_first_catalog_install() -> Result<()> {
    let fixture = Fixture::new(&[
        (".claude/commands/first.md", "First $ARGUMENTS"),
        (
            "skills/base/SKILL.md",
            "---\nname: base\ndescription: initial skill\n---\nInitial skill",
        ),
    ])
    .await?;
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("sources")?,
        )
        .await
        .map_err(volume_error)?,
    );
    let mut client =
        CommandSession::open(volume.clone(), uuid::Uuid::new_v4(), 32_768, 128).await?;
    let mut sources = CommandSources::observe(
        &mut client,
        acyclic_harness::client::DEFAULT_COMMANDS_JSON.to_vec(),
        vec![CommandRoot {
            scope: CommandScope::Project,
            root: fixture.root.clone(),
        }],
        vec![ContextRoot {
            directory: "skills".into(),
            ..fixture.root.clone()
        }],
        fixture.reader.clone(),
        Default::default(),
        32_768,
        128,
    )
    .await?;
    for (path, body, expected) in [
        (".claude/commands/added.md", "Added $ARGUMENTS", "Added now"),
        (
            ".claude/commands/added.md",
            "Changed $ARGUMENTS",
            "Changed now",
        ),
    ] {
        let cut = volume
            .snapshot(&Query::Root, 128)
            .await
            .map_err(volume_error)?;
        volume
            .append(
                cut.generation,
                &FileValue::new(
                    FileKey::parse(path).map_err(volume_error)?,
                    body.as_bytes().to_vec(),
                ),
            )
            .await
            .map_err(volume_error)?;
        loop {
            sources.changed().await?;
            sources.refresh(&mut client).await?;
            if let Ok(CommandResolution::Prompt { text, .. }) = client
                .catalog()
                .resolve("/added now", fixture.reader.as_ref(), 32_768)
                .await
                && text == expected
            {
                break;
            }
        }
    }
    for description in ["new skill", "modified skill"] {
        let path = "skills/added/SKILL.md";
        let body = format!("---\nname: drawing\ndescription: {description}\n---\nDraw");
        let cut = volume
            .snapshot(&Query::Root, 128)
            .await
            .map_err(volume_error)?;
        volume
            .append(
                cut.generation,
                &FileValue::new(
                    FileKey::parse(path).map_err(volume_error)?,
                    body.into_bytes(),
                ),
            )
            .await
            .map_err(volume_error)?;
        loop {
            sources.changed().await?;
            sources.refresh(&mut client).await?;
            if client
                .catalog()
                .commands()
                .any(|spec| spec.name == "drawing" && spec.description == description)
            {
                break;
            }
        }
    }
    // Restart must rescan declarations even though a catalog is already installed.
    let cut = volume
        .snapshot(&Query::Inputs, 128)
        .await
        .map_err(volume_error)?;
    volume
        .append(
            cut.generation,
            &FileValue::new(
                FileKey::parse(".claude/commands/added.md").map_err(volume_error)?,
                b"Restart $ARGUMENTS".to_vec(),
            ),
        )
        .await
        .map_err(volume_error)?;
    let mut reopened =
        CommandSession::open(volume.clone(), uuid::Uuid::new_v4(), 32_768, 128).await?;
    let _sources = CommandSources::observe(
        &mut reopened,
        acyclic_harness::client::DEFAULT_COMMANDS_JSON.to_vec(),
        vec![CommandRoot {
            scope: CommandScope::Project,
            root: fixture.root.clone(),
        }],
        vec![ContextRoot {
            directory: "skills".into(),
            ..fixture.root.clone()
        }],
        fixture.reader.clone(),
        Default::default(),
        32_768,
        128,
    )
    .await?;
    let CommandResolution::Prompt { text, .. } = reopened
        .catalog()
        .resolve("/added now", fixture.reader.as_ref(), 32_768)
        .await?
    else {
        panic!("expected prompt")
    };
    assert_eq!(text, "Restart now");
    Ok(())
}

#[cfg(all(feature = "filesystem-local", feature = "test-support"))]
#[path = "client_command_local_owner.rs"]
mod local_owner;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // A shrinking reference fold defines scope precedence and JSON stability.
    #[test]
    fn latest_matching_control_wins_and_round_trips(actions in prop::collection::vec((0u8..4, any::<bool>()), 0..40)) {
        let task = |byte| acyclic_harness::TaskId::from_bytes([byte;16]);
        let mut json = json!({"all":null,"tasks":{}});
        let mut expected = false;
        for (index, (scope, paused)) in actions.iter().enumerate() {
            if *scope == 0 { json["all"] = json!([index + 1, paused]); }
            else { json["tasks"][task(*scope).to_string()] = json!([index + 1, paused]); }
            if *scope < 3 { expected = *paused; }
        }
        let bytes = serde_json::to_vec(&json).unwrap();
        let control = CommandControl::from_bytes(&bytes, bytes.len() as u64).unwrap();
        prop_assert_eq!(control.paused(&[task(1), task(2)]), expected);
        prop_assert_eq!(control.paused(&[task(2), task(1)]), expected);
        let encoded = serde_json::to_vec(&control).unwrap();
        prop_assert_eq!(CommandControl::from_bytes(&encoded, encoded.len() as u64).unwrap(), control);
        if !bytes.is_empty() { prop_assert!(CommandControl::from_bytes(&bytes, (bytes.len()-1) as u64).is_err()); }
    }
}

struct NoReads;
impl ContentResidencyVerifier for NoReads {}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Every bounded Unicode source file round-trips through the live catalog
    // and expands only its placeholders, independent of the native reader.
    #[test]
    fn followed_file_templates_round_trip(suffix in "[a-z]{1,8}", body in prop::collection::vec(any::<char>(), 0..32), arguments in "[A-Za-z0-9]{0,12}") {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let name = format!("custom-{suffix}");
        let body: String = body.into_iter().map(|ch| if ch == '\0' { '\u{fffd}' } else { ch }).collect();
        let body = format!("prefix {body} $ARGUMENTS");
        runtime.block_on(async {
            for skill in [false, true] {
            let volume = Arc::new(TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("source-property").unwrap()).await.unwrap());
            let mut client = CommandSession::open(volume.clone(), uuid::Uuid::new_v4(), 32_768, 64).await.unwrap();
            let (path, source) = if skill {
                (format!("skills/{name}/SKILL.md"), format!("---\nname: {name}\ndescription: Generated skill\n---\n{body}"))
            } else {
                (format!(".codex/prompts/{name}.md"), body.clone())
            };
            let cut = volume.snapshot(&Query::Inputs, 64).await.unwrap();
            volume.append(cut.generation, &FileValue::new(FileKey::parse(&path).unwrap(), source.into_bytes())).await.unwrap();
            let _sources = CommandSources::observe(&mut client, acyclic_harness::client::DEFAULT_COMMANDS_JSON.to_vec(), vec![], vec![], Arc::new(NoReads), Default::default(), 32_768, 64).await.unwrap();
            let encoded = serde_json::to_vec(&client.catalog().commands().collect::<Vec<_>>()).unwrap();
            let decoded = CommandCatalog::from_bytes(&encoded, 32_768).unwrap();
            let result = decoded.resolve(&format!("/{name} {arguments}"), &NoReads, 32_768).await.unwrap();
            let CommandResolution::Prompt { text, .. } = result else { panic!("template did not resolve") };
            prop_assert_eq!(text, body.replace("$ARGUMENTS", &arguments));
            }
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn catalog_alias_edits_survive_source_reload_and_bad_source_can_be_corrected() -> Result<()> {
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("catalog-edits")?,
        )
        .await
        .map_err(volume_error)?,
    );
    let mut client = CommandSession::open(volume.clone(), uuid::Uuid::new_v4(), 32_768, 64).await?;
    let mut catalog: Value = client
        .state(CommandFileRoot::Config, "commands.json")?
        .unwrap();
    catalog
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|spec| spec["name"] == "model")
        .unwrap()["aliases"] = json!(["choose"]);
    client
        .install_catalog(serde_json::to_vec(&catalog).unwrap())
        .await?;
    let mut sources = CommandSources::observe(
        &mut client,
        acyclic_harness::client::DEFAULT_COMMANDS_JSON.to_vec(),
        vec![],
        vec![],
        Arc::new(NoReads),
        Default::default(),
        32_768,
        64,
    )
    .await?;
    for (path, body, valid) in [
        (".codex/prompts/added.md", b"bad\0source".as_slice(), false),
        (
            ".codex/prompts/added.md",
            b"Hello $ARGUMENTS".as_slice(),
            true,
        ),
        (
            "skills/corrected/SKILL.md",
            b"---\nname: model\ndescription: reserved\n---\nBad".as_slice(),
            false,
        ),
        (
            "skills/corrected/SKILL.md",
            b"---\nname: corrected\n---\nMissing description".as_slice(),
            false,
        ),
        (
            "skills/corrected/SKILL.md",
            b"---\nname: corrected\ndescription: corrected\n---\nGood".as_slice(),
            true,
        ),
    ] {
        let cut = volume
            .snapshot(&Query::Inputs, 64)
            .await
            .map_err(volume_error)?;
        volume
            .append(
                cut.generation,
                &FileValue::new(FileKey::parse(path).map_err(volume_error)?, body.to_vec()),
            )
            .await
            .map_err(volume_error)?;
        sources.changed().await?;
        let result = sources.refresh(&mut client).await;
        assert_eq!(result.is_ok(), valid);
        if valid {
            assert!(client.catalog().commands().any(|spec| spec.name == "added"));
            if path.starts_with("skills/") {
                assert!(
                    client
                        .catalog()
                        .commands()
                        .any(|spec| spec.name == "corrected")
                );
            }
            // Consume this fold's own catalog publication before the next edit.
            sources.changed().await?;
            sources.refresh(&mut client).await?;
        }
        assert_eq!(
            client.catalog().complete("CHOOSE").next().unwrap().name,
            "model"
        );
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Confirmation serialization preserves the captured target and creates a
    // request exactly when accepted, regardless of later UI navigation.
    #[test]
    fn confirmation_target_and_acceptance_survive_file_round_trip(target in any::<[u8;16]>(), other in any::<[u8;16]>(), delete in any::<bool>(), accepted in any::<bool>()) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let target = acyclic_harness::TaskId::from_bytes(target);
            let other = acyclic_harness::TaskId::from_bytes(other);
            let volume = Arc::new(TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("confirmation-property").unwrap()).await.unwrap());
            let mut client = CommandSession::open(volume.clone(), uuid::Uuid::from_bytes([1;16]), 32_768, 64).await.unwrap();
            client.write_state(CommandFileRoot::Session, "layout.json", &json!({"selected_task":target})).await.unwrap();
            client.submit(if delete { "/delete" } else { "/clearall" }, &NoReads).await.unwrap();
            prop_assert!(client.take_request().is_none());
            let mut layout = client.state(CommandFileRoot::Session, "layout.json").unwrap().unwrap();
            let encoded = serde_json::to_vec(&layout).unwrap();
            layout = serde_json::from_slice(&encoded).unwrap();
            layout["selected_task"] = json!(other);
            client.write_state(CommandFileRoot::Session, "layout.json", &layout).await.unwrap();
            client.submit(if accepted { "yes" } else { "no" }, &NoReads).await.unwrap();
            let request = client.take_request();
            prop_assert_eq!(request.is_some(), accepted);
            if let Some(request) = request {
                let decoded: acyclic_harness::client::CommandRequest = serde_json::from_slice(&request.bytes).unwrap();
                prop_assert_eq!(decoded, if delete { acyclic_harness::client::CommandRequest::Delete } else { acyclic_harness::client::CommandRequest::ClearAll });
                if delete { prop_assert!(matches!(&request.key, FileKey::Task { task, .. } if task.as_str() == target.to_string()), "confirmation target changed"); }
                else { prop_assert!(matches!(&request.key, FileKey::Session { .. }), "clear request lost session identity"); }
            }
            prop_assert!(client.state(CommandFileRoot::Session, "layout.json").unwrap().unwrap().get("confirmation").is_none());
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn generated_alias_survives_source_edit_and_reopening() -> Result<()> {
    let fixture = Fixture::new(&[]).await?;
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("generated-alias")?,
        )
        .await
        .map_err(volume_error)?,
    );
    let session = uuid::Uuid::new_v4();
    let mut client = CommandSession::open(volume.clone(), session, 32_768, 128).await?;
    let publish = |text: &str| {
        FileValue::new(
            FileKey::parse(".claude/commands/custom.md").unwrap(),
            text.as_bytes().to_vec(),
        )
    };
    let file = publish("Original $ARGUMENTS");
    let cut = volume
        .snapshot(&Query::File(file.key.clone()), 1)
        .await
        .map_err(volume_error)?
        .generation;
    volume.append(cut, &file).await.map_err(volume_error)?;
    let mut sources = CommandSources::observe(
        &mut client,
        acyclic_harness::client::DEFAULT_COMMANDS_JSON.to_vec(),
        vec![],
        vec![],
        fixture.reader.clone(),
        Default::default(),
        32_768,
        128,
    )
    .await?;
    let mut specs = serde_json::to_value(client.catalog().commands().collect::<Vec<_>>()).unwrap();
    specs
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|spec| spec["name"] == "custom")
        .unwrap()["aliases"] = json!(["shortcut"]);
    client
        .install_catalog(serde_json::to_vec(&specs).unwrap())
        .await?;
    let file = publish("Updated $ARGUMENTS");
    let cut = volume
        .snapshot(&Query::File(file.key.clone()), 1)
        .await
        .map_err(volume_error)?
        .generation;
    volume.append(cut, &file).await.map_err(volume_error)?;
    for _ in 0..16 {
        sources.changed().await?;
        if let Err(error) = sources.refresh(&mut client).await {
            if !matches!(error, Error::Conflict(_)) {
                return Err(error);
            }
        }
        if matches!(client.catalog().resolve("/shortcut now",fixture.reader.as_ref(),32_768).await,Ok(CommandResolution::Prompt { text,.. }) if text=="Updated now")
        {
            break;
        }
    }
    assert!(
        matches!(client.catalog().resolve("/shortcut now",fixture.reader.as_ref(),32_768).await?,CommandResolution::Prompt { text,.. } if text=="Updated now")
    );
    drop(sources);
    drop(client);
    let mut client = CommandSession::open(volume, session, 32_768, 128).await?;
    CommandSources::observe(
        &mut client,
        acyclic_harness::client::DEFAULT_COMMANDS_JSON.to_vec(),
        vec![],
        vec![],
        fixture.reader.clone(),
        Default::default(),
        32_768,
        128,
    )
    .await?;
    assert!(
        matches!(client.catalog().resolve("/shortcut again",fixture.reader.as_ref(),32_768).await?,CommandResolution::Prompt { text,.. } if text=="Updated again")
    );
    Ok(())
}

async fn declared_root_aliases(suffix: &str) -> Result<()> {
    let aliases = [
        format!("user-run-{suffix}"),
        format!("project-run-{suffix}"),
        format!("audit-run-{suffix}"),
    ];
    let fixture = Fixture::new(&[
        (
            "home/.claude/commands/user-cmd.md",
            "User original $ARGUMENTS",
        ),
        (
            "project/.codex/prompts/project-cmd.md",
            "Project original $ARGUMENTS",
        ),
        (
            "skills/audit/SKILL.md",
            "---\nname: audit\ndescription: Audit\n---\nAudit original",
        ),
    ])
    .await?;
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("root-aliases")?,
        )
        .await
        .map_err(volume_error)?,
    );
    let session = uuid::Uuid::new_v4();
    let roots = vec![
        CommandRoot {
            scope: CommandScope::User,
            root: ContextRoot {
                directory: "home".into(),
                ..fixture.root.clone()
            },
        },
        CommandRoot {
            scope: CommandScope::Project,
            root: ContextRoot {
                directory: "project".into(),
                ..fixture.root.clone()
            },
        },
    ];
    let skills = vec![ContextRoot {
        directory: "skills".into(),
        ..fixture.root.clone()
    }];
    let mut client = CommandSession::open(volume.clone(), session, 32_768, 128).await?;
    let sources = CommandSources::observe(
        &mut client,
        acyclic_harness::client::DEFAULT_COMMANDS_JSON.to_vec(),
        roots.clone(),
        skills.clone(),
        fixture.reader.clone(),
        Default::default(),
        32_768,
        128,
    )
    .await?;
    let mut specs = serde_json::to_value(client.catalog().commands().collect::<Vec<_>>()).unwrap();
    for (name, alias) in [
        ("user-cmd", aliases[0].as_str()),
        ("project-cmd", aliases[1].as_str()),
        ("audit", aliases[2].as_str()),
    ] {
        specs
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|spec| spec["name"] == name)
            .unwrap()["aliases"] = json!([alias]);
    }
    client
        .install_catalog(serde_json::to_vec(&specs).unwrap())
        .await?;
    drop(sources);
    drop(client);
    for (path, body) in [
        (
            "home/.claude/commands/user-cmd.md",
            "User reopened $ARGUMENTS",
        ),
        (
            "project/.codex/prompts/project-cmd.md",
            "Project reopened $ARGUMENTS",
        ),
        (
            "skills/audit/SKILL.md",
            "---\nname: audit\ndescription: Audit\n---\nAudit reopened",
        ),
    ] {
        fixture
            .put(path, body, &format!("root-edit-{path}"))
            .await?;
    }
    let mut client = CommandSession::open(volume, session, 32_768, 128).await?;
    let mut sources = CommandSources::observe(
        &mut client,
        acyclic_harness::client::DEFAULT_COMMANDS_JSON.to_vec(),
        roots,
        skills,
        fixture.reader.clone(),
        Default::default(),
        32_768,
        128,
    )
    .await?;
    for alias in &aliases {
        let CommandResolution::Prompt { text, .. } = client
            .catalog()
            .resolve(&format!("/{alias} now"), fixture.reader.as_ref(), 32_768)
            .await?
        else {
            panic!("source alias was not a prompt")
        };
        assert!(text.contains("reopened"), "{alias}: {text}");
    }
    fixture
        .put(
            "project/.codex/prompts/project-cmd.md",
            "Project live $ARGUMENTS",
            "live-edit",
        )
        .await?;
    // A catalog edit is a followed source-refresh event without mirroring a
    // source file into the input volume. The imported action stays a Prompt.
    let mut specs = serde_json::to_value(client.catalog().commands().collect::<Vec<_>>()).unwrap();
    specs[0]["description"] = json!("Refresh external source declarations");
    client
        .install_catalog(serde_json::to_vec(&specs).unwrap())
        .await?;
    for _ in 0..16 {
        sources.changed().await?;
        match sources.refresh(&mut client).await {
            Ok(()) | Err(Error::Conflict(_)) => {}
            Err(error) => return Err(error),
        }
        if matches!(client.catalog().resolve(&format!("/{} now", aliases[1]), fixture.reader.as_ref(), 32_768).await, Ok(CommandResolution::Prompt { text,.. }) if text=="Project live now")
        {
            break;
        }
    }
    assert!(
        matches!(client.catalog().resolve(&format!("/{} now", aliases[1]), fixture.reader.as_ref(), 32_768).await?, CommandResolution::Prompt { text,.. } if text=="Project live now")
    );
    assert!(
        client
            .catalog()
            .commands()
            .filter(|spec| ["user-cmd", "project-cmd", "audit"].contains(&spec.name.as_str()))
            .all(|spec| !matches!(
                spec.action,
                acyclic_harness::client::CommandAction::Template { .. }
            )),
        "the configured-root regression must not fall back to mirrored templates"
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn declared_root_prompt_and_skill_aliases_survive_edits_and_reopening(suffix in "[a-z]{1,8}") {
        futures::executor::block_on(declared_root_aliases(&suffix)).unwrap();
    }
}
