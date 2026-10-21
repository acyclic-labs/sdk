use crate::invalid;
use acyclic_harness_core::{
    Error, Result,
    conversation::{ContentResidencyVerifier, FileRef, Limits, verified_content_bytes},
    model::{FileProjectionPolicy, ModelContent, ModelRequest},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct Files {
    pub reader: Option<Arc<dyn ContentResidencyVerifier>>,
    pub maximum_bytes: u64,
}

impl Default for Files {
    fn default() -> Self {
        Self {
            reader: None,
            maximum_bytes: Limits::default().render_bytes,
        }
    }
}

#[derive(Default)]
pub(crate) struct FileText(pub(crate) std::vec::IntoIter<ResolvedFile>);

pub(crate) enum ResolvedFile {
    Text(String),
    Image(String),
}

impl From<String> for ResolvedFile {
    fn from(text: String) -> Self {
        Self::Text(text)
    }
}

impl FileText {
    pub(crate) fn project(
        &mut self,
        file: &FileRef,
        policy: &FileProjectionPolicy,
    ) -> Result<String> {
        match policy {
            FileProjectionPolicy::Reference => reference(file),
            FileProjectionPolicy::BoundedFull | FileProjectionPolicy::ProjectInstructions(_) => {
                match self.0.next() {
                    Some(ResolvedFile::Text(text)) => Ok(text),
                    _ => Err(invalid("missing resolved model text file")),
                }
            }
            FileProjectionPolicy::Native(_) => Err(Error::Unsupported(
                "native model media is unsupported by this adapter".into(),
            )),
        }
    }

    pub(crate) fn image(&mut self) -> Result<String> {
        match self.0.next() {
            Some(ResolvedFile::Image(data)) => Ok(data),
            _ => Err(invalid("missing resolved model image")),
        }
    }
}

fn validate(file: &FileRef, policy: &FileProjectionPolicy, maximum: u64) -> Result<()> {
    match policy {
        FileProjectionPolicy::Reference => (),
        FileProjectionPolicy::BoundedFull | FileProjectionPolicy::ProjectInstructions(_) => {
            if !file.descriptor().media_type().starts_with("text/") {
                return Err(Error::Unsupported(
                    "bounded model files must be text".into(),
                ));
            }
        }
        FileProjectionPolicy::Native(policy) => {
            if !matches!(
                policy.intent,
                acyclic_harness::model::NativeMediaIntent::Image { .. }
            ) || policy.configuration.is_some()
                || !matches!(
                    file.descriptor().media_type(),
                    "image/png" | "image/jpeg" | "image/gif" | "image/webp"
                )
            {
                return Err(Error::Unsupported(
                    "unsupported native model image policy".into(),
                ));
            }
        }
    }
    if !matches!(policy, FileProjectionPolicy::Reference) {
        let bytes = projected_bytes(file, policy)?;
        let mut byte_limit = maximum;
        if let FileProjectionPolicy::Native(native) = policy {
            byte_limit = byte_limit.min(native.maximum_bytes);
            let mut work = native.maximum_work;
            charge(&mut work, bytes, "work")?;
        }
        charge(&mut byte_limit, bytes, "byte")?;
    }
    Ok(())
}

// Every projection, including native media, pays through the same pre-I/O
// subtraction. Work is admitted source bytes (framed bytes for instructions).
fn charge(remaining: &mut u64, bytes: u64, kind: &str) -> Result<()> {
    *remaining = remaining.checked_sub(bytes)
        .ok_or_else(|| invalid(&format!("model files exceed projection {kind} limit")))?;
    Ok(())
}

fn projected_bytes(file: &FileRef, policy: &FileProjectionPolicy) -> Result<u64> {
    if let FileProjectionPolicy::ProjectInstructions(source) = policy {
        acyclic_harness_core::model::InstructionSource::with_identity(file, source).framed_bytes()
    } else {
        Ok(file.descriptor().byte_length())
    }
}

pub(crate) fn extra_bytes(content: &ModelContent, maximum: u64) -> Result<usize> {
    let mut extra = 0usize;
    for (file, policy) in content.file_parts() {
        validate(file, policy, maximum)?;
        if !matches!(policy, FileProjectionPolicy::Reference) {
            // Planning never performs I/O. Six bytes covers JSON escaping of
            // every source byte, including control characters.
            let bytes = usize::try_from(projected_bytes(file, policy)?)
                .ok()
                .and_then(|bytes| bytes.checked_mul(6))
                .ok_or_else(|| invalid("model file token estimate overflow"))?;
            extra = extra
                .checked_add(bytes)
                .ok_or_else(|| invalid("model file token estimate overflow"))?;
        }
    }
    Ok(extra)
}

pub(crate) async fn resolve(
    files: &Files,
    request: &ModelRequest,
    io: Option<&acyclic_harness_core::conversation::ContentIo>,
) -> Result<FileText> {
    // Reject the entire unsupported request before invoking any owner reader.
    let mut remaining = files.maximum_bytes;
    for message in &request.messages {
        for (file, policy) in message.content.file_parts() {
            validate(file, policy, files.maximum_bytes)?;
            if !matches!(policy, FileProjectionPolicy::Reference) {
                charge(&mut remaining, projected_bytes(file, policy)?, "aggregate byte")?;
            }
        }
    }
    let mut remaining = files.maximum_bytes;
    let mut text = Vec::new();
    for message in &request.messages {
        for (file, policy) in message.content.file_parts() {
            if matches!(policy, FileProjectionPolicy::Reference) {
                continue;
            }
            charge(&mut remaining, projected_bytes(file, policy)?, "aggregate byte")?;
            let reader = files.reader.as_ref().ok_or_else(|| {
                Error::Unsupported("model file byte resolution is unavailable".into())
            })?;
            let (bytes, hold) =
                verified_content_bytes(reader.as_ref(), file, file.descriptor().byte_length(), io)
                    .await?;
            if let FileProjectionPolicy::ProjectInstructions(source) = policy {
                let value =
                    acyclic_harness_core::model::InstructionSource::with_identity(file, source)
                        .render(&bytes, io)?;
                drop(hold);
                text.push(ResolvedFile::Text(value));
                continue;
            }
            if matches!(policy, FileProjectionPolicy::Native(_)) {
                if acyclic_harness::model::image_media_type(bytes.as_slice())
                    != Some(file.descriptor().media_type())
                {
                    return Err(invalid("model image bytes differ from their media type"));
                }
                text.push(ResolvedFile::Image(STANDARD.encode(bytes.as_slice())));
                drop(hold);
                continue;
            }
            let bytes = bytes
                .try_into_ordinary_vec()
                .unwrap_or_else(|bytes| bytes.as_slice().to_vec());
            let value =
                String::from_utf8(bytes).map_err(|_| invalid("model text file is not UTF-8"))?;
            drop(hold);
            text.push(ResolvedFile::Text(value));
        }
    }
    Ok(FileText(text.into_iter()))
}

fn reference(file: &FileRef) -> Result<String> {
    let capacity = crate::projection::encoded_len(file)?
        .checked_add(8)
        .ok_or_else(|| invalid("file marker size overflows"))?;
    let mut bytes = Vec::with_capacity(capacity);
    bytes.extend_from_slice(b"[file: ");
    serde_json::to_writer(&mut bytes, file).map_err(|_| invalid("file marker encoding failed"))?;
    bytes.push(b']');
    String::from_utf8(bytes).map_err(|_| invalid("file marker is not UTF-8"))
}
