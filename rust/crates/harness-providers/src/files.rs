use crate::invalid;
use acyclic_harness_core::{
    Error, Result,
    conversation::{ContentResidencyVerifier, FileRef, verified_content_bytes},
    model::{FileProjectionPolicy, ModelContent, ModelRequest},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use std::sync::Arc;

#[derive(Clone, Default)]
pub(crate) struct Files {
    pub reader: Option<Arc<dyn ContentResidencyVerifier>>,
    pub maximum_bytes: Option<u64>,
}

#[derive(Default)]
pub(crate) struct FileText(pub(crate) std::vec::IntoIter<ResolvedFile>);

pub(crate) enum ResolvedFile {
    Text(String),
    Native(String),
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

    pub(crate) fn native(&mut self) -> Result<String> {
        match self.0.next() {
            Some(ResolvedFile::Native(data)) => Ok(data),
            _ => Err(invalid("missing resolved native model media")),
        }
    }
}

// Every projection, including native media, pays through the same pre-I/O
// subtraction. Work is admitted source bytes (framed bytes for instructions).
fn charge(remaining: &mut Option<u64>, bytes: u64, kind: &str) -> Result<()> {
    if let Some(remaining) = remaining {
        *remaining = remaining
            .checked_sub(bytes)
            .ok_or_else(|| invalid(&format!("model files exceed projection {kind} limit")))?;
    }
    Ok(())
}

pub(crate) fn extra_bytes(content: &ModelContent, maximum: Option<u64>) -> Result<usize> {
    let mut extra = 0usize;
    for (file, policy) in content.file_parts() {
        policy.validate_projection(file, maximum)?;
        if !matches!(policy, FileProjectionPolicy::Reference) {
            // Planning never performs I/O. Six bytes covers JSON escaping of
            // every source byte, including control characters.
            let bytes = usize::try_from(policy.projected_bytes(file)?)
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
            policy.validate_projection(file, files.maximum_bytes)?;
            if !matches!(policy, FileProjectionPolicy::Reference) {
                charge(
                    &mut remaining,
                    policy.projected_bytes(file)?,
                    "aggregate byte",
                )?;
            }
        }
    }
    let mut text = Vec::new();
    for message in &request.messages {
        for (file, policy) in message.content.file_parts() {
            if matches!(policy, FileProjectionPolicy::Reference) {
                continue;
            }
            let reader = files.reader.as_ref().ok_or_else(|| {
                Error::Unsupported("model file byte resolution is unavailable".into())
            })?;
            let (bytes, hold) =
                verified_content_bytes(reader.as_ref(), file, None, None, io).await?;
            if let FileProjectionPolicy::ProjectInstructions(source) = policy {
                let value =
                    acyclic_harness_core::model::InstructionSource::with_identity(file, source)
                        .render(&bytes, io)?;
                drop(hold);
                text.push(ResolvedFile::Text(value));
                continue;
            }
            if let FileProjectionPolicy::Native(native) = policy {
                let valid = match native.intent {
                    acyclic_harness_core::model::NativeMediaIntent::Image { .. } => {
                        acyclic_harness_core::model::image_media_type(bytes.as_slice())
                            == Some(file.descriptor().media_type())
                    }
                    acyclic_harness_core::model::NativeMediaIntent::Document { .. } => {
                        bytes.starts_with(b"%PDF-")
                    }
                    _ => false,
                };
                if !valid {
                    return Err(invalid("model media bytes differ from their declared type"));
                }
                text.push(ResolvedFile::Native(STANDARD.encode(bytes.as_slice())));
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
