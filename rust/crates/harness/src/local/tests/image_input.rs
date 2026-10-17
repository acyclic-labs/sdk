use super::*;
use crate::{
    client::{CommandFileRoot, PromptFiles, run_local_command},
    conversation::verified_content_bytes,
    model::{FileProjectionPolicy, ModelContentPart},
    tui::{InputEffect, PromptInput},
};

#[tokio::test]
async fn host_image_files_reach_model_and_reopen_with_exact_pins() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = text();
    let harness = LocalHarness::open(
        LocalHarnessConfig::new(directory.path(), (model()?, provider.clone()))
            .without_default_tools()
            .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await?;
    let owner = harness.client_owner();
    let mut commands =
        crate::client::local_ui(&harness, &OperationId::new().to_string(), &owner).await?;
    let reader = harness.client_reader()?;
    let publisher = harness.client_publisher()?;
    let prepared = crate::tui::prepare_image(2, 1, vec![255; 8]).unwrap();
    let crate::tui::ClipboardContent::Image { jpeg, .. } = prepared else {
        unreachable!()
    };
    let source = directory.path().join("selected image.jpg");
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
    run_local_command(&mut commands, &harness, &request).await?;
    let requests = provider.requests();
    let images: Vec<_> = requests[0]
        .messages
        .iter()
        .flat_map(|m| m.content.parts())
        .filter_map(|p| match p {
            ModelContentPart::File {
                file,
                policy: FileProjectionPolicy::Native(_),
            } => Some(file),
            _ => None,
        })
        .collect();
    assert_eq!(images, vec![&pins[0]]);
    drop(commands);
    drop(harness);
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
    Ok(())
}
