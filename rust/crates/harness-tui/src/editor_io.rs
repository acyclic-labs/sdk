use crate::editor_args::editor_command;
use std::{
    io::{self, Write},
    path::Path,
};

/// Clipboard text or an image ready for host attachment storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClipboardContent {
    /// Committed clipboard text.
    Text(String),
    /// JPEG attachment, fitted inside 1568 by 1568 pixels at quality 85.
    Image {
        /// Encoded JPEG bytes.
        jpeg: Vec<u8>,
        /// Encoded width.
        width: u32,
        /// Encoded height.
        height: u32,
    },
}

/// Prepare an RGBA clipboard image without allowing malformed dimensions or
/// overflowing byte counts to reach the image encoder.
pub fn prepare_image(width: u32, height: u32, rgba: Vec<u8>) -> io::Result<ClipboardContent> {
    prepare_image_with_settings(width, height, rgba, crate::DEFAULT_SETTINGS)
}

/// Prepare an attachment using followed image preferences.
pub fn prepare_image_with_settings(
    width: u32,
    height: u32,
    rgba: Vec<u8>,
    settings: crate::UiSettings,
) -> io::Result<ClipboardContent> {
    if !settings.valid() {
        return Err(io::Error::other("invalid image preferences"));
    }
    let expected = u64::from(width)
        .checked_mul(u64::from(height))
        .and_then(|n| n.checked_mul(4));
    if width == 0 || height == 0 || expected != Some(rgba.len() as u64) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid RGBA image dimensions",
        ));
    }
    let image = image::RgbaImage::from_raw(width, height, rgba)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid RGBA image"))?;
    let image = image::DynamicImage::ImageRgba8(image);
    let limit = u32::try_from(settings.image_max_dimension).unwrap_or(u32::MAX);
    let image = if width > limit || height > limit {
        image.thumbnail(limit, limit)
    } else {
        image
    };
    let image = image.to_rgb8();
    let (width, height) = image.dimensions();
    let mut jpeg = vec![];
    image::codecs::jpeg::JpegEncoder::new_with_quality(
        &mut jpeg,
        u8::try_from(settings.image_quality).unwrap_or(u8::MAX),
    )
    .encode_image(&image)
    .map_err(io::Error::other)?;
    Ok(ClipboardContent::Image {
        jpeg,
        width,
        height,
    })
}

/// Invoke $EDITOR on a caller-selected private-volume scratch file. The host
/// suspends its terminal guard first and restores it after this call, on success
/// or error. Cancellation and failed editors leave the prompt unchanged.
pub fn edit_external(text: &str, path: &Path, command: Option<&str>) -> io::Result<Option<String>> {
    let configured = command
        .map(str::to_owned)
        .or_else(|| std::env::var("EDITOR").ok())
        .or_else(|| std::env::var("VISUAL").ok())
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "set $EDITOR to edit this prompt")
        })?;
    let (program, arguments) = editor_command(&configured)?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    let result = (|| {
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        drop(file);
        let status = std::process::Command::new(program)
            .args(arguments)
            .arg(path)
            .status()?;
        if !status.success() {
            return Ok(None);
        }
        std::fs::read_to_string(path).map(Some)
    })();
    let _ = std::fs::remove_file(path);
    result
}
