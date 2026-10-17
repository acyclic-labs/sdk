use crate::{
    Error, Result,
    conversation::{Attachment, ContentResidencyVerifier, FileRef},
};

/// Pins a selected image in the request without copying it into another store.
pub async fn attach_image(
    file: FileRef,
    reader: &dyn ContentResidencyVerifier,
    maximum_bytes: u64,
) -> Result<Attachment> {
    image_bound(&file, maximum_bytes)?;
    reader.verify(&file, maximum_bytes, None)?.await?;
    Ok(Attachment { file, label: None })
}

fn image_bound(file: &FileRef, maximum_bytes: u64) -> Result<()> {
    file.validate(&mut None)?;
    if maximum_bytes == 0
        || maximum_bytes > crate::conversation::MAX_EXACT_JS_INTEGER
        || file.descriptor().byte_length() == 0
        || file.descriptor().byte_length() > maximum_bytes
        || !file.descriptor().media_type().starts_with("image/")
    {
        return Err(Error::Invalid(
            "image attachment exceeds its byte or media allowance".into(),
        ));
    }
    Ok(())
}

/// Writes the prepared clipboard JPEG to the agent private volume. Persist the
/// returned reference with the draft's ordinary attachment manifest; submission
/// pins that same reference even if the draft changes afterwards.
#[cfg(all(feature = "tui", not(target_arch = "wasm32")))]
pub async fn paste_image(
    publisher: &dyn crate::conversation::ContentPublisher,
    operation: crate::OperationId,
    clipboard: &crate::tui::ClipboardContent,
    maximum_bytes: u64,
) -> Result<Attachment> {
    let crate::tui::ClipboardContent::Image {
        jpeg,
        width,
        height,
    } = clipboard
    else {
        return Err(Error::Invalid("clipboard content is not an image".into()));
    };
    if publisher.volume().class() != crate::conversation::VolumeClass::AgentPrivate
        || maximum_bytes == 0
        || maximum_bytes > crate::conversation::MAX_EXACT_JS_INTEGER
        || jpeg.is_empty()
        || jpeg.len() as u64 > maximum_bytes
        || *width == 0
        || *height == 0
        || *width > 1568
        || *height > 1568
    {
        return Err(Error::Invalid(
            "clipboard image exceeds its private-volume allowance".into(),
        ));
    }
    let path = format!("attachments/{operation}.jpg");
    let (file, hold) = publisher
        .stage_once(operation, &path, jpeg, "image/jpeg", "clipboard.jpg", None)?
        .await?;
    image_bound(&file, maximum_bytes)?;
    if file.volume() != publisher.volume()
        || file.path() != path
        || file.descriptor().media_type() != "image/jpeg"
    {
        return Err(Error::Storage(
            "image publisher returned another file".into(),
        ));
    }
    file.descriptor().verify(jpeg, &mut None)?;
    drop(hold);
    Ok(Attachment { file, label: None })
}
