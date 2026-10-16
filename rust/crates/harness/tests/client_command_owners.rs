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
        .await?,
    );
    let mut client =
        CommandSession::open(volume.clone(), uuid::Uuid::new_v4(), 32_768, 128).await?;
    let mut sources = CommandSources::observe(
        &mut client,
        &volume,
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
        let cut = volume.snapshot(&Query::Root, 128).await?;
        volume
            .append(
                cut.generation,
                &FileValue::new(FileKey::parse(path)?, body.as_bytes().to_vec()),
            )
            .await?;
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
        let cut = volume.snapshot(&Query::Root, 128).await?;
        volume
            .append(
                cut.generation,
                &FileValue::new(FileKey::parse(path)?, body.into_bytes()),
            )
            .await?;
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
    let cut = volume.snapshot(&Query::Inputs, 128).await?;
    volume
        .append(
            cut.generation,
            &FileValue::new(
                FileKey::parse(".claude/commands/added.md")?,
                b"Restart $ARGUMENTS".to_vec(),
            ),
        )
        .await?;
    let mut reopened =
        CommandSession::open(volume.clone(), uuid::Uuid::new_v4(), 32_768, 128).await?;
    let _sources = CommandSources::observe(
        &mut reopened,
        &volume,
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

#[cfg(feature = "filesystem-local")]
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
