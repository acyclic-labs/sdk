use super::*;
use crate::{
    client::CommandResolution,
    model::{ModelContent, ModelContentPart},
};

#[tokio::test]
async fn composer_commands_are_typed_and_unknown_commands_never_admit() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = text();
    let harness = LocalHarness::open(checkout_config(
        directory.path(),
        (model()?, provider.clone()),
    )?)
    .await?;
    let operation = OperationId::new();
    assert!(matches!(
        harness
            .resolve_input(operation, "/shell echo hello")
            .await?,
        CommandResolution::Tool {
            tool: crate::client::CommandTool::Shell,
            ..
        }
    ));
    assert!(matches!(
        harness
            .resolve_input(OperationId::new(), "/missing-input-command")
            .await,
        Err(Error::NotFound(_))
    ));
    assert!(provider.requests().is_empty());
    // The previous construction demonstrably admits the slash spelling as a
    // model prompt. The production resolver must choose a different effect.
    let legacy = crate::client::prepare_local_prompt(operation, "/shell echo hello")?;
    assert!(matches!(
        serde_json::from_slice::<CommandResolution>(&legacy.bytes)
            .map_err(super::super::storage)?,
        CommandResolution::Prompt { .. }
    ));
    harness.disconnect().await
}

#[tokio::test]
async fn composer_templates_and_mentions_retain_the_original_cut_through_dispatch() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::create_dir_all(directory.path().join(".claude/commands"))
        .map_err(super::super::storage)?;
    std::fs::write(
        directory.path().join(".claude/commands/marker.md"),
        "Inspect $ARGUMENTS @calc.py",
    )
    .map_err(super::super::storage)?;
    std::fs::write(directory.path().join("calc.py"), "value = 42\n")
        .map_err(super::super::storage)?;
    let provider = text();
    let harness = LocalHarness::open(checkout_config(
        directory.path(),
        (model()?, provider.clone()),
    )?)
    .await?;
    let operation = OperationId::new();
    let resolved = harness.resolve_input(operation, "/marker foo").await?;
    let CommandResolution::Prompt { text, mentions, .. } = &resolved else {
        panic!("not a prompt")
    };
    assert_eq!(text, "Inspect foo @calc.py");
    assert_eq!(mentions.len(), 1);
    let attached = mentions[0].file.clone();
    assert_eq!(
        harness
            .client_reader()?
            .read(&attached, 1024, None)?
            .await?
            .as_ref(),
        b"value = 42\n"
    );
    std::fs::write(directory.path().join("calc.py"), "value = 99\n")
        .map_err(super::super::storage)?;
    std::fs::write(
        directory.path().join(".claude/commands/marker.md"),
        "changed",
    )
    .map_err(super::super::storage)?;
    assert_eq!(
        harness.resolve_input(operation, "/marker foo").await?,
        resolved
    );
    assert!(matches!(
        harness.resolve_input(operation, "/marker changed").await,
        Err(Error::Conflict(_))
    ));
    harness.dispatch_command(operation, None, resolved).await?;
    let (_, model) = harness
        .recover_turn_inputs(operation)
        .await?
        .expect("model input");
    let bytes = harness
        .client_reader()?
        .read(&model.payload, harness.limits.file_bytes, None)?
        .await?;
    let staged: crate::filesystem::ModelTaskCommand =
        serde_json::from_slice(&bytes).map_err(super::super::storage)?;
    let ModelContent::Parts(parts) = staged.input else {
        panic!("mention was dropped")
    };
    assert!(matches!(&parts[0], ModelContentPart::Text { text } if text == "Inspect foo @calc.py"));
    assert!(matches!(&parts[1], ModelContentPart::File { file, .. } if file == &attached));
    assert_eq!(
        harness
            .resume(crate::runtime::task_id_for_admission(operation))
            .await?
            .text,
        "done"
    );
    assert_eq!(provider.requests().len(), 1);
    harness.disconnect().await
}

#[tokio::test]
async fn composer_duplicate_and_unsafe_sources_fail_before_model_work() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    for path in [".claude/commands", ".codex/prompts"] {
        std::fs::create_dir_all(directory.path().join(path)).map_err(super::super::storage)?;
        std::fs::write(directory.path().join(path).join("duplicate.md"), "body")
            .map_err(super::super::storage)?;
    }
    let provider = text();
    let harness = LocalHarness::open(checkout_config(
        directory.path(),
        (model()?, provider.clone()),
    )?)
    .await?;
    assert!(matches!(
        harness
            .resolve_input(OperationId::new(), "/duplicate")
            .await,
        Err(Error::Conflict(_))
    ));
    // Unrelated invalid sources never block a command no source defines.
    assert!(matches!(
        harness
            .resolve_input(OperationId::new(), "/shell echo hello")
            .await?,
        CommandResolution::Tool {
            tool: crate::client::CommandTool::Shell,
            ..
        }
    ));
    for input in [
        "inspect @../outside",
        "inspect @/.system/secret",
        "inspect @.system/secret",
        "inspect @\"unterminated",
    ] {
        assert!(
            matches!(
                harness.resolve_input(OperationId::new(), input).await,
                Err(Error::Invalid(_))
            ),
            "{input}"
        );
    }
    assert!(provider.requests().is_empty());
    harness.disconnect().await
}

#[cfg(feature = "tui")]
#[tokio::test]
async fn native_tui_submission_uses_the_same_pinned_composer_resolution() -> Result<()> {
    use crate::client;
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::create_dir_all(directory.path().join(".claude/commands"))
        .map_err(super::super::storage)?;
    std::fs::write(
        directory.path().join(".claude/commands/tui-marker.md"),
        "TUI-MARKER $ARGUMENTS @calc.py",
    )
    .map_err(super::super::storage)?;
    std::fs::write(directory.path().join("calc.py"), "tui_unique_value = 42\n")
        .map_err(super::super::storage)?;
    let provider = text();
    let harness = LocalHarness::open(checkout_config(
        directory.path(),
        (model()?, provider.clone()),
    )?)
    .await?;
    let session = OperationId::new();
    let mut ui = client::local_ui(&harness, &session.to_string(), &harness.client_owner()).await?;
    let reader = harness.client_reader()?;
    client::apply_tui_input(
        &mut ui,
        reader.as_ref(),
        crate::tui::InputEffect::Submit {
            text: "/tui-marker argument".into(),
            queued: false,
            edit: None,
        },
    )
    .await?;
    std::fs::write(directory.path().join("calc.py"), "changed = 99\n")
        .map_err(super::super::storage)?;
    let mut owner = client::CommandSession::open_local(
        &harness,
        uuid::Uuid::from_bytes(session.into_bytes()),
        65_536,
        4096,
    )
    .await?;
    let request = owner.next_request().await?;
    client::run_local_command(&mut owner, &harness, &request).await?;
    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    let sent = serde_json::to_string(&requests[0]).map_err(super::super::storage)?;
    assert!(sent.contains("TUI-MARKER argument"), "{sent}");
    assert!(!sent.contains("changed = 99"), "{sent}");
    let mut attachments = Vec::new();
    for message in &requests[0].messages {
        if let ModelContent::Parts(parts) = &message.content {
            for part in parts {
                if let ModelContentPart::File { file, .. } = part {
                    attachments.push(file);
                }
            }
        }
    }
    assert_eq!(attachments.len(), 1);
    assert_eq!(
        reader.read(attachments[0], 1024, None)?.await?.as_ref(),
        b"tui_unique_value = 42\n"
    );
    assert!(matches!(
        ui.submit("/unknown-tui-command", reader.as_ref()).await,
        Err(Error::NotFound(_))
    ));
    drop(owner);
    drop(ui);
    harness.disconnect().await
}

#[tokio::test]
async fn composer_uses_the_saved_catalog_instead_of_reinstalling_builtins() -> Result<()> {
    use crate::task_volume::{FileKey, FileValue, Query};
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = text();
    let harness = LocalHarness::open(checkout_config(
        directory.path(),
        (model()?, provider.clone()),
    )?)
    .await?;
    let key = FileKey::parse("commands/catalog.json").map_err(super::super::storage)?;
    let cut = harness
        .task_files
        .inner
        .snapshot(&Query::File(key.clone()), 1)
        .await
        .map_err(super::super::storage)?;
    harness
        .task_files
        .inner
        .append(cut.generation, &FileValue::new(key, b"[]".to_vec()))
        .await
        .map_err(super::super::storage)?;
    assert!(matches!(
        harness
            .resolve_input(OperationId::new(), "/shell echo hidden-builtin")
            .await,
        Err(Error::NotFound(_))
    ));
    assert!(provider.requests().is_empty());
    harness.disconnect().await
}
