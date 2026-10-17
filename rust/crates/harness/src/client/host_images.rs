//! Host acquisition publishes ordinary private files; draft state is a live file fold.
use super::{CommandFileRoot, CommandResolution, CommandSession};
use crate::{
    Error, OperationId, Result,
    conversation::{ContentPublisher, ContentResidencyVerifier, FileRef, VolumeClass},
    tui::{InputEffect, PromptInput},
};
use acyclic_stream::StreamProvider;
use std::io::Read;

pub(super) const MAX_IMAGE: u64 = 4 * 1024 * 1024;
const MAX_TOTAL: u64 = 8 * 1024 * 1024;

pub(super) fn draft_images<P: StreamProvider + ?Sized>(
    files: &CommandSession<P>,
) -> Result<Vec<FileRef>> {
    let images: Vec<FileRef> = serde_json::from_value(
        files
            .state(CommandFileRoot::Session, "attachments.json")?
            .unwrap_or_else(|| serde_json::json!([])),
    )
    .map_err(|e| Error::Invalid(e.to_string()))?;
    let mut bytes = 0u64;
    if images.len() > 16 {
        return Err(Error::Invalid("too many draft images".into()));
    }
    for file in &images {
        file.validate(&mut None)?;
        bytes = bytes
            .checked_add(file.descriptor().byte_length())
            .ok_or_else(|| Error::Invalid("image size overflow".into()))?;
        if file.volume().class() != VolumeClass::AgentPrivate
            || !file.descriptor().media_type().starts_with("image/")
            || file.descriptor().byte_length() == 0
            || file.descriptor().byte_length() > MAX_IMAGE
            || bytes > MAX_TOTAL
        {
            return Err(Error::Invalid(
                "draft image exceeds its private-volume allowance".into(),
            ));
        }
    }
    Ok(images)
}
pub(crate) async fn publish<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
    publisher: &dyn ContentPublisher,
    bytes: &[u8],
    mime: &str,
    extension: &str,
) -> Result<()> {
    let mut images = draft_images(files)?;
    let total: u64 = images.iter().map(|f| f.descriptor().byte_length()).sum();
    if publisher.volume().class() != VolumeClass::AgentPrivate
        || bytes.is_empty()
        || bytes.len() as u64 > MAX_IMAGE
        || total + bytes.len() as u64 > MAX_TOTAL
        || images.len() == 16
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
fn attached(path: &str) -> Result<(Vec<u8>, &'static str, &'static str)> {
    let file = std::fs::File::open(path).map_err(|e| Error::Invalid(e.to_string()))?;
    if !file
        .metadata()
        .map_err(|e| Error::Invalid(e.to_string()))?
        .is_file()
    {
        return Err(Error::Invalid("attachment must be a regular file".into()));
    }
    let mut bytes = Vec::new();
    file.take(MAX_IMAGE + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| Error::Invalid(e.to_string()))?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_IMAGE {
        return Err(Error::Invalid("image exceeds byte allowance".into()));
    }
    let (mime, ext) =
        match imagesize::image_type(&bytes).map_err(|e| Error::Invalid(e.to_string()))? {
            imagesize::ImageType::Png => ("image/png", "png"),
            imagesize::ImageType::Jpeg => ("image/jpeg", "jpg"),
            imagesize::ImageType::Gif => ("image/gif", "gif"),
            imagesize::ImageType::Webp => ("image/webp", "webp"),
            _ => return Err(Error::Invalid("unsupported image format".into())),
        };
    let size = imagesize::blob_size(&bytes).map_err(|e| Error::Invalid(e.to_string()))?;
    if size.width == 0 || size.height == 0 || size.width > 4096 || size.height > 4096 {
        return Err(Error::Invalid("image exceeds dimension allowance".into()));
    }
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
                    if image.bytes.len() > 64 * 1024 * 1024 {
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
                        super::file_tui_input(files, reader, effect, page_rows, maximum_scroll)
                            .await?;
                    }
                    Ok(())
                }
            }
        }
        InputEffect::Submit {
            ref text,
            edit: None,
            ..
        } if text.starts_with('/') => {
            match files.catalog().resolve(text, reader, MAX_IMAGE).await? {
                CommandResolution::Sdk { command, arguments } if command == "attach" => {
                    let (bytes, mime, ext) = attached(arguments.trim().trim_matches('"'))?;
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
    match apply_host_input(
        files,
        reader,
        publisher,
        input,
        effect,
        page_rows,
        maximum_scroll,
        now_ms,
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(
            error @ (Error::Invalid(_)
            | Error::Unsupported(_)
            | Error::NotFound(_)
            | Error::Conflict(_)),
        ) => {
            files
                .write_unchanged_state(
                    CommandFileRoot::Session,
                    "notifications.json",
                    &serde_json::json!([error.to_string()]),
                )
                .await
        }
        Err(error) => Err(error),
    }
}
