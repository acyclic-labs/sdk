//! Host acquisition publishes ordinary private files; draft state is a live file fold.
use super::{CommandFileRoot, CommandResolution, CommandSession};
use crate::{
    Error, OperationId, Result,
    conversation::{ContentPublisher, ContentResidencyVerifier, FileRef, VolumeClass},
    tui::{InputEffect, PromptInput},
};
use acyclic_stream::StreamProvider;
use std::io::Read;

pub(super) fn draft_images<P: StreamProvider + ?Sized>(
    files: &CommandSession<P>,
) -> Result<Vec<FileRef>> {
    let images: Vec<FileRef> = serde_json::from_value(
        files
            .state(CommandFileRoot::Session, "attachments.json")?
            .unwrap_or_else(|| serde_json::json!([])),
    )
    .map_err(|e| Error::Invalid(e.to_string()))?;
    for file in &images {
        file.validate(&mut None)?;
        if file.volume().class() != VolumeClass::AgentPrivate
            || !file.descriptor().media_type().starts_with("image/")
            || file.descriptor().byte_length() == 0
        {
            return Err(Error::Invalid(
                "draft image exceeds its private-volume allowance".into(),
            ));
        }
    }
    Ok(images)
}
async fn publish<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
    publisher: &dyn ContentPublisher,
    bytes: &[u8],
    mime: &str,
    extension: &str,
) -> Result<()> {
    let policy = files.image_policy()?;
    policy.parse(bytes, Some(mime))?;
    let mut images = draft_images(files)?;
    let total = images.iter().try_fold(0u64, |total, file| {
        total
            .checked_add(file.descriptor().byte_length())
            .ok_or_else(|| Error::Invalid("image size overflow".into()))
    })?;
    if publisher.volume().class() != VolumeClass::AgentPrivate
        || bytes.is_empty()
        || bytes.len() as u64 > policy.maximum_bytes
        || total
            .checked_add(bytes.len() as u64)
            .is_none_or(|total| total > policy.maximum_total_bytes)
        || images.len() >= policy.maximum_images as usize
    {
        return Err(Error::Invalid("image exceeds draft allowance".into()));
    }
    let op = OperationId::new();
    let path = format!("attachments/{op}.{extension}");
    let (file, hold) = publisher
        .stage_once(op, &path, bytes, mime, "attached image", None)?
        .await?;
    file.descriptor().verify(bytes, &mut None)?;
    if file.volume() != publisher.volume()
        || file.path() != path
        || file.descriptor().media_type() != mime
    {
        return Err(Error::Storage(
            "image publisher returned another file".into(),
        ));
    }
    images.push(file);
    files
        .write_unchanged_state(
            CommandFileRoot::Session,
            "attachments.json",
            &serde_json::to_value(images).map_err(|e| Error::Invalid(e.to_string()))?,
        )
        .await?;
    drop(hold);
    Ok(())
}
fn attached(
    path: &str,
    policy: &crate::model::ImageInput,
) -> Result<(Vec<u8>, &'static str, &'static str)> {
    if !std::fs::metadata(path)
        .map_err(|e| Error::Invalid(e.to_string()))?
        .is_file()
    {
        return Err(Error::Invalid("attachment must be a regular file".into()));
    }
    let file = std::fs::File::open(path).map_err(|e| Error::Invalid(e.to_string()))?;
    if !file
        .metadata()
        .map_err(|e| Error::Invalid(e.to_string()))?
        .is_file()
    {
        return Err(Error::Invalid("attachment must be a regular file".into()));
    }
    let mut bytes = Vec::new();
    file.take(policy.maximum_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| Error::Invalid(e.to_string()))?;
    if bytes.is_empty() || bytes.len() as u64 > policy.maximum_bytes {
        return Err(Error::Invalid("image exceeds byte allowance".into()));
    }
    let header = policy.parse(&bytes, None)?;
    let (mime, ext) = header.format.media();
    Ok((bytes, mime, ext))
}
/// Shared native host binding used by GraphCoder and embedded terminal clients.
/// Image bytes are published before their refs enter the followed draft file.
async fn apply_host_input<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
    reader: &dyn ContentResidencyVerifier,
    publisher: &dyn ContentPublisher,
    input: &mut PromptInput,
    effect: InputEffect,
    page_rows: usize,
    maximum_scroll: usize,
    now_ms: u64,
) -> Result<()> {
    let result = match effect {
        InputEffect::Paste => {
            let mut clipboard =
                arboard::Clipboard::new().map_err(|e| Error::Unsupported(e.to_string()))?;
            match clipboard.get_image() {
                Ok(image) => {
                    let policy = files.image_policy()?;
                    let decoded = u64::from(policy.maximum_dimension)
                        .checked_pow(2)
                        .and_then(|n| n.checked_mul(4))
                        .ok_or_else(|| Error::Invalid("image dimension overflow".into()))?;
                    if image.bytes.len() as u64 > decoded {
                        return Err(Error::Invalid(
                            "clipboard image exceeds decoded allowance".into(),
                        ));
                    }
                    let width =
                        u32::try_from(image.width).map_err(|e| Error::Invalid(e.to_string()))?;
                    let height =
                        u32::try_from(image.height).map_err(|e| Error::Invalid(e.to_string()))?;
                    let prepared = crate::tui::prepare_image_with_settings(
                        width,
                        height,
                        image.bytes.into_owned(),
                        *input.editor.settings(),
                    )
                    .map_err(|e| Error::Invalid(e.to_string()))?;
                    let crate::tui::ClipboardContent::Image { jpeg, .. } = prepared else {
                        unreachable!()
                    };
                    publish(files, publisher, &jpeg, "image/jpeg", "jpg").await
                }
                Err(_) => {
                    let text = clipboard
                        .get_text()
                        .map_err(|e| Error::Unsupported(e.to_string()))?;
                    for effect in input.paste(&text, now_ms) {
                        super::apply_tui_input(files, reader, effect).await?;
                    }
                    Ok(())
                }
            }
        }
        InputEffect::Submit {
            ref text,
            edit: None,
            ..
        } if text.trim_start().starts_with('/') => {
            match files
                .catalog()
                .resolve(text, reader, files.image_policy()?.maximum_bytes)
                .await?
            {
                CommandResolution::Sdk { command, arguments } if command == "attach" => {
                    let policy = files.image_policy()?;
                    let (bytes, mime, ext) = attached(arguments.trim().trim_matches('"'), &policy)?;
                    publish(files, publisher, &bytes, mime, ext).await
                }
                _ => super::file_tui_input(files, reader, effect, page_rows, maximum_scroll).await,
            }
        }
        effect => super::file_tui_input(files, reader, effect, page_rows, maximum_scroll).await,
    };
    result
}

/// Apply a native clipboard/attachment action and retain ordinary errors as feedback.
pub async fn host_tui_input<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
    reader: &dyn ContentResidencyVerifier,
    publisher: &dyn ContentPublisher,
    input: &mut PromptInput,
    effect: InputEffect,
    page_rows: usize,
    maximum_scroll: usize,
    now_ms: u64,
) -> Result<()> {
    let host_action = match &effect {
        InputEffect::Paste => true,
        InputEffect::Submit {
            text, edit: None, ..
        } => text
            .trim_start()
            .strip_prefix('/')
            .and_then(|text| text.split_whitespace().next())
            .is_some_and(|name| {
                files.catalog().commands().any(|spec| {
                    matches!(&spec.action,super::CommandAction::Sdk{command} if command=="attach")
                        && (spec.name.eq_ignore_ascii_case(name)
                            || spec
                                .aliases
                                .iter()
                                .any(|alias| alias.eq_ignore_ascii_case(name)))
                })
            }),
        _ => false,
    };
    if !host_action {
        return super::file_tui_input(files, reader, effect, page_rows, maximum_scroll).await;
    }
    let result = apply_host_input(
        files,
        reader,
        publisher,
        input,
        effect,
        page_rows,
        maximum_scroll,
        now_ms,
    )
    .await;
    super::input_files::input_feedback(files, result).await
}
