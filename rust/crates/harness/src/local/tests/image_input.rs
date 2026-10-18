use super::*;
use crate::{
    client::{CommandFileRoot, PromptFiles, run_local_command},
    conversation::verified_content_bytes,
    model::{FileProjectionPolicy, ModelContentPart},
    tui::{InputEffect, PromptInput},
};

#[tokio::test]
async fn headless_embedded_binding_keeps_bounded_unsupported_image_fallback() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let harness = LocalHarness::open(
        LocalHarnessConfig::new(directory.path(), (model()?, text())).without_default_tools(),
    )
    .await?;
    let commands =
        crate::client::CommandSession::open_local(&harness, uuid::Uuid::new_v4(), 65_536, 128)
            .await?;
    assert_eq!(
        commands
            .state(CommandFileRoot::Config, "image-input.json")?
            .unwrap()["models"]["scripted/test"],
        serde_json::to_value(crate::model::ImageInput::default()).unwrap(),
    );
    drop(commands);
    harness.close().await
}

#[tokio::test]
async fn wrapper_only_image_initialization_is_detected_negative_control() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = Arc::new(
        ScriptedModelProvider::new([])
            .with_context_capacity(capacity())
            .with_image_input(Default::default()),
    );
    let harness = LocalHarness::open(
        LocalHarnessConfig::new(directory.path(), (model()?, provider))
            .without_default_tools()
            .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await?;
    super::super::client_files::OMIT_IMAGE_POLICY.with(|flag| flag.set(true));
    let opened = crate::client::local_ui(
        &harness,
        &OperationId::new().to_string(),
        &harness.client_owner(),
    )
    .await;
    super::super::client_files::OMIT_IMAGE_POLICY.with(|flag| flag.set(false));
    let commands = opened?;
    let policy = commands.state(CommandFileRoot::Config, "image-input.json")?;
    drop(commands);
    harness.close().await?;
    let failed = std::panic::catch_unwind(|| {
        assert!(policy.is_some(), "shared binding omitted image admission");
    });
    assert!(
        failed.is_err(),
        "wrapper-only initialization escaped the oracle"
    );
    Ok(())
}

#[tokio::test]
async fn host_image_files_reach_model_and_reopen_with_exact_pins() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let policy = crate::model::ImageInput {
        maximum_tokens: 20_000,
        ..Default::default()
    };
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Text("done".into())])
            .with_context_capacity(capacity())
            .with_image_input(policy.clone()),
    );
    let harness = LocalHarness::open(
        LocalHarnessConfig::new(directory.path(), (model()?, provider.clone()))
            .without_default_tools()
            .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await?;
    let owner = harness.client_owner();
    let session = OperationId::new();
    let mut commands = crate::client::local_ui(&harness, &session.to_string(), &owner).await?;
    assert_eq!(
        commands
            .state(CommandFileRoot::Config, "image-input.json")?
            .unwrap()["models"]["scripted/test"],
        serde_json::to_value(&policy).unwrap(),
        "shared client binding did not install the selected image policy"
    );
    let reader = harness.client_reader()?;
    let publisher = harness.client_publisher()?;
    let prepared = crate::tui::prepare_image(2, 1, vec![255; 8]).unwrap();
    let crate::tui::ClipboardContent::Image { jpeg, .. } = prepared else {
        unreachable!()
    };
    let source = directory.path().join("selected 图像.jpg");
    std::fs::write(&source, &jpeg).map_err(super::super::storage)?;
    let mut input = PromptInput::default();
    crate::client::host_tui_input(
        &mut commands,
        reader.as_ref(),
        publisher.as_ref(),
        &mut input,
        InputEffect::Submit {
            text: format!("/attach {}", source.display()),
            edit: None,
            queued: false,
        },
        20,
        0,
        0,
    )
    .await?;
    let files = commands
        .state(CommandFileRoot::Session, "attachments.json")?
        .unwrap();
    let pins: Vec<crate::conversation::FileRef> =
        serde_json::from_value(files).map_err(super::super::storage)?;
    assert_eq!(pins.len(), 1);
    assert_eq!(pins[0].volume().class(), VolumeClass::AgentPrivate);
    assert_eq!(
        verified_content_bytes(reader.as_ref(), &pins[0], 4096, None)
            .await?
            .0
            .as_ref(),
        jpeg.as_slice()
    );
    let mut prompt_files = PromptFiles::default();
    prompt_files.refresh(&commands, &mut input)?;
    assert_eq!(input.images, 1);
    std::fs::write(&source, b"changed host file").map_err(super::super::storage)?;
    drop(commands);
    let mut commands = crate::client::local_ui(&harness, &session.to_string(), &owner).await?;
    let mut input = PromptInput::default();
    PromptFiles::default().refresh(&commands, &mut input)?;
    assert_eq!(
        input.images, 1,
        "a reopened UI folds the private draft file"
    );
    commands
        .submit("describe the attached image", reader.as_ref())
        .await?;
    let request = commands.next_request().await?;
    assert!(
        commands
            .state(CommandFileRoot::Session, "attachments.json")?
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty()
    );
    let resolution: crate::client::CommandResolution =
        serde_json::from_slice(&request.bytes).map_err(super::super::storage)?;
    let crate::client::CommandResolution::Prompt { mentions, .. } = resolution else {
        panic!("expected prompt")
    };
    assert_eq!(mentions[0].file, pins[0]);
    let mut executor = crate::client::CommandSession::open_local(
        &harness,
        uuid::Uuid::from_bytes(session.into_bytes()),
        65_536,
        128,
    )
    .await?;
    run_local_command(&mut executor, &harness, &request).await?;
    let requests = provider.requests();
    let images: Vec<_> = requests[0]
        .messages
        .iter()
        .flat_map(|m| m.content.parts())
        .filter_map(|p| match p {
            ModelContentPart::File {
                file,
                policy: FileProjectionPolicy::Native(actual),
            } => {
                assert_eq!(actual.maximum_work, u64::from(policy.maximum_tokens));
                Some(file)
            }
            _ => None,
        })
        .collect();
    assert_eq!(images, vec![&pins[0]]);
    drop(commands);
    drop(executor);
    drop(reader);
    drop(publisher);
    harness.close().await?;
    let reopened = LocalHarness::open(
        LocalHarnessConfig::new(directory.path(), (model()?, provider.clone()))
            .without_default_tools()
            .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await?;
    let reader = reopened.client_reader()?;
    assert_eq!(
        verified_content_bytes(reader.as_ref(), &pins[0], 4096, None)
            .await?
            .0
            .as_ref(),
        jpeg.as_slice()
    );
    assert_eq!(provider.requests().len(), 1);
    drop(reader);
    reopened.close().await?;
    Ok(())
}

#[tokio::test]
async fn host_image_rejections_preserve_the_followed_draft() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let harness = LocalHarness::open(
        LocalHarnessConfig::new(directory.path(), (model()?, text())).without_default_tools(),
    )
    .await?;
    let mut commands = crate::client::local_ui(
        &harness,
        &OperationId::new().to_string(),
        &harness.client_owner(),
    )
    .await?;
    install_image_policy(&mut commands, 4 * 1024 * 1024).await?;
    let reader = harness.client_reader()?;
    let publisher = harness.client_publisher()?;
    let mut input = PromptInput::default();
    for (name, bytes) in [
        ("empty.png", Vec::new()),
        ("text.png", b"not an image".to_vec()),
        ("oversized.png", vec![0; 4 * 1024 * 1024 + 1]),
    ] {
        let path = directory.path().join(name);
        std::fs::write(&path, bytes).map_err(super::super::storage)?;
        crate::client::host_tui_input(
            &mut commands,
            reader.as_ref(),
            publisher.as_ref(),
            &mut input,
            InputEffect::Submit {
                text: format!("/attach {}", path.display()),
                edit: None,
                queued: false,
            },
            20,
            0,
            0,
        )
        .await?;
        assert!(
            commands
                .state(CommandFileRoot::Session, "attachments.json")?
                .is_none()
        );
        assert!(
            !commands
                .state(CommandFileRoot::Session, "notifications.json")?
                .unwrap()
                .as_array()
                .unwrap()
                .is_empty(),
            "{name}"
        );
        assert!(commands.take_request().is_none());
    }
    let crate::tui::ClipboardContent::Image { jpeg, .. } =
        crate::tui::prepare_image(1, 1, vec![255; 4]).unwrap()
    else {
        unreachable!()
    };
    let path = directory.path().join("valid.jpg");
    std::fs::write(&path, jpeg).map_err(super::super::storage)?;
    crate::client::host_tui_input(
        &mut commands,
        reader.as_ref(),
        publisher.as_ref(),
        &mut input,
        InputEffect::Submit {
            text: format!("/attach {}", path.display()),
            edit: None,
            queued: false,
        },
        20,
        0,
        0,
    )
    .await?;
    assert!(
        commands
            .state(CommandFileRoot::Session, "notifications.json")?
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty(),
        "successful host actions clear stale errors"
    );
    Ok(())
}

async fn install_image_policy<P: acyclic_stream::StreamProvider + ?Sized>(
    commands: &mut crate::client::CommandSession<P>,
    maximum: u64,
) -> Result<()> {
    let model = model()?;
    let name = format!("{}/{}", model.provider, model.name);
    let policy = crate::model::ImageInput {
        maximum_bytes: maximum,
        maximum_total_bytes: maximum * 2,
        maximum_images: 16,
        maximum_dimension: 4096,
        maximum_tokens: 16384,
    };
    let policies = std::collections::BTreeMap::from([(name.clone(), policy)]);
    commands
        .write_state(
            CommandFileRoot::Config,
            "image-input.json",
            &json!({"default":name,"models":policies}),
        )
        .await
}

#[tokio::test]
async fn host_images_follow_catalog_byte_count_total_and_dimension_policy() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let harness = LocalHarness::open(
        LocalHarnessConfig::new(directory.path(), (model()?, text())).without_default_tools(),
    )
    .await?;
    let mut commands = crate::client::local_ui(
        &harness,
        &OperationId::new().to_string(),
        &harness.client_owner(),
    )
    .await?;
    let reader = harness.client_reader()?;
    let publisher = harness.client_publisher()?;
    let crate::tui::ClipboardContent::Image { mut jpeg, .. } =
        crate::tui::prepare_image(2, 1, vec![255; 8]).unwrap()
    else {
        unreachable!()
    };
    jpeg.resize(5 * 1024 * 1024, 0); // A JPEG permits trailing bytes after EOI.
    let source = directory.path().join("five megabytes.jpg");
    std::fs::write(&source, jpeg).map_err(super::super::storage)?;
    let mut input = PromptInput::default();
    for (maximum, dimension, count, total, expected) in [
        (2, 4096, 2, 4, 0),
        (8, 1, 2, 16, 0),
        (8, 2, 1, 16, 1),
        (8, 2, 1, 16, 1),
        (8, 2, 2, 8, 1),
        (8, 2, 2, 16, 2),
    ] {
        install_image_policy(&mut commands, maximum * 1024 * 1024).await?;
        let mut catalog = commands
            .state(CommandFileRoot::Config, "image-input.json")?
            .unwrap();
        let name = catalog["default"].as_str().unwrap().to_owned();
        let policy = &mut catalog["models"][&name];
        policy["maximum_dimension"] = json!(dimension);
        policy["maximum_images"] = json!(count);
        policy["maximum_total_bytes"] = json!(total * 1024 * 1024u64);
        commands
            .write_state(CommandFileRoot::Config, "image-input.json", &catalog)
            .await?;
        crate::client::host_tui_input(
            &mut commands,
            reader.as_ref(),
            publisher.as_ref(),
            &mut input,
            InputEffect::Submit {
                text: format!("/attach {}", source.display()),
                edit: None,
                queued: false,
            },
            20,
            0,
            0,
        )
        .await?;
        PromptFiles::default().refresh(&commands, &mut input)?;
        assert_eq!(input.images, expected, "followed model policy: {catalog}");
    }
    commands.submit("describe", reader.as_ref()).await?;
    let request = commands.take_request().unwrap();
    let resolution: crate::client::CommandResolution =
        serde_json::from_slice(&request.bytes).map_err(super::super::storage)?;
    assert!(
        matches!(resolution, crate::client::CommandResolution::Prompt { mentions, .. } if mentions.len()==2 && mentions.iter().all(|m| m.file.descriptor().byte_length()==5*1024*1024))
    );
    drop(commands);
    drop(publisher);
    drop(reader);
    harness.close().await
}
