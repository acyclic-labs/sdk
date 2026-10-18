//! Image admission shared by hosts and provider projections.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

/// Declared image support and finite bounds for one exact model.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(target_arch = "wasm32", feature = "browser"), derive(tsify::Tsify))]
pub struct ImageInput {
    /// Maximum raw bytes in one image.
    pub maximum_bytes: u64,
    /// Maximum combined raw image bytes in one request.
    pub maximum_total_bytes: u64,
    /// Maximum image occurrences, including history and tool results.
    pub maximum_images: u32,
    /// Maximum width or height before provider preprocessing.
    pub maximum_dimension: u32,
    /// Model-specific upper bound for one image's input tokens.
    pub maximum_tokens: u32,
}

impl Default for ImageInput {
    fn default() -> Self {
        Self {
            maximum_bytes: 4 * 1024 * 1024,
            maximum_total_bytes: 8 * 1024 * 1024,
            maximum_images: 16,
            maximum_dimension: 4096,
            maximum_tokens: 16_384,
        }
    }
}

/// Verified header metadata. Pixel decoding remains the provider's responsibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct ImageHeader {
    /// Header-derived MIME type.
    pub format: ImageFormat,
    /// Nonzero admitted width.
    pub width: usize,
    /// Nonzero admitted height.
    pub height: usize,
}

/// Image encodings supported by the provider adapters.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageFormat {
    /// PNG.
    Png,
    /// JPEG.
    Jpeg,
    /// GIF.
    Gif,
    /// WebP.
    Webp,
    /// HEIF/HEIC.
    Heif,
}
impl ImageFormat {
    /// MIME type and private-file extension for this encoding.
    pub const fn media(self) -> (&'static str, &'static str) {
        match self {
            Self::Png => ("image/png", "png"),
            Self::Jpeg => ("image/jpeg", "jpg"),
            Self::Gif => ("image/gif", "gif"),
            Self::Webp => ("image/webp", "webp"),
            Self::Heif => ("image/heif", "heif"),
        }
    }
}
impl ImageInput {
    /// Rejects non-finite or inconsistent catalog limits.
    pub fn validate(&self) -> Result<()> {
        let portable = crate::conversation::MAX_EXACT_JS_INTEGER;
        if self.maximum_bytes == 0
            || self.maximum_bytes > portable
            || self.maximum_total_bytes < self.maximum_bytes
            || self.maximum_total_bytes > portable
            || matches!(self.maximum_images, 0 | u32::MAX)
            || matches!(self.maximum_dimension, 0 | u32::MAX)
            || self.maximum_tokens == 0
        {
            return Err(Error::Invalid("invalid image input bounds".into()));
        }
        Ok(())
    }

    /// Parses bounded header metadata, checking the optional declared MIME type.
    pub fn parse(&self, bytes: &[u8], mime: Option<&str>) -> Result<ImageHeader> {
        self.validate()?;
        if bytes.is_empty() || bytes.len() as u64 > self.maximum_bytes {
            return Err(Error::Invalid("image exceeds byte allowance".into()));
        }
        let format = match imagesize::image_type(bytes)
            .map_err(|_| Error::Invalid("invalid image header".into()))?
        {
            imagesize::ImageType::Png => ImageFormat::Png,
            imagesize::ImageType::Jpeg => ImageFormat::Jpeg,
            imagesize::ImageType::Gif => ImageFormat::Gif,
            imagesize::ImageType::Webp => ImageFormat::Webp,
            imagesize::ImageType::Heif(_) => ImageFormat::Heif,
            _ => return Err(Error::Invalid("unsupported image format".into())),
        };
        if mime.is_some_and(|mime| {
            mime != format.media().0 && !(format == ImageFormat::Heif && mime == "image/heic")
        }) {
            return Err(Error::Invalid(
                "image header differs from its declared media type".into(),
            ));
        }
        let size = imagesize::blob_size(bytes)
            .map_err(|_| Error::Invalid("invalid image dimensions".into()))?;
        if size.width == 0
            || size.height == 0
            || size.width > self.maximum_dimension as usize
            || size.height > self.maximum_dimension as usize
        {
            return Err(Error::Invalid("image exceeds dimension allowance".into()));
        }
        Ok(ImageHeader {
            format,
            width: size.width,
            height: size.height,
        })
    }
}
