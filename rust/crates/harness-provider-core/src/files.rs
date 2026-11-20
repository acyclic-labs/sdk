use crate::invalid;
use acyclic_harness_core::{
    Error, Result,
    conversation::{ContentResidencyVerifier, FileRef, verified_content_bytes},
    model::{FileProjectionPolicy, ModelContent, PreparedModelRequest},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use std::sync::Arc;

/// The owner readers and byte budget a request's files are resolved through.
#[derive(Clone, Default)]
pub struct Files {
    /// The verifier that reads a file's bytes, when byte resolution is available.
    pub reader: Option<Arc<dyn ContentResidencyVerifier>>,
    /// The largest file the projection admits, when bounded.
    pub maximum_bytes: Option<u64>,
}

/// The resolved text of a request's files, in projection order.
#[derive(Default)]
pub struct FileText(pub std::vec::IntoIter<ResolvedFile>);

/// One resolved file: text the adapter renders, or native media it passes through.
pub enum ResolvedFile {
    /// Text the adapter renders into the request.
    Text(String),
    /// Base64 native media the adapter passes through unchanged.
    Native(String),
}

impl From<String> for ResolvedFile {
    fn from(text: String) -> Self {
        Self::Text(text)
    }
}

impl FileText {
    /// The text FILE projects to under POLICY.
    pub fn project(&mut self, file: &FileRef, policy: &FileProjectionPolicy) -> Result<String> {
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

    /// The next resolved native media payload.
    pub fn native(&mut self) -> Result<String> {
        match self.0.next() {
            Some(ResolvedFile::Native(data)) => Ok(data),
            _ => Err(invalid("missing resolved native model media")),
        }
    }
}

/// The bytes CONTENT's files add to the rendered request, before JSON escaping.
pub fn extra_bytes(content: &ModelContent, maximum: Option<u64>) -> Result<usize> {
    let mut extra = 0usize;
    for (file, policy) in content.file_parts() {
        let bytes = policy.projected_bytes(file, maximum)?;
        if !matches!(policy, FileProjectionPolicy::Reference) {
            // Planning never performs I/O. Six bytes covers JSON escaping of
            // every source byte, including control characters.
            let bytes = usize::try_from(bytes)
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

/// Resolves every file REQUEST projects through FILES, in projection order.
pub async fn resolve(
    files: &Files,
    request: &PreparedModelRequest,
    io: Option<&acyclic_harness_core::conversation::ContentIo>,
) -> Result<FileText> {
    // Reject the entire unsupported request before invoking any owner reader.
    request.validate_byte_projection()?;
    request.projected_file_bytes(files.maximum_bytes)?;
    let mut text = Vec::new();
    for message in &request.request().messages {
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
                    acyclic_harness_core::model::NativeMediaIntent::Document => {
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

            // Unmetered bytes move into the text when they own an ordinary
            // vector; charged bytes are copied so their charge still ends with them.
            let bytes = match bytes {
                acyclic_harness_core::conversation::Custody::Unmetered(bytes) => bytes
                    .try_into_ordinary_vec()
                    .unwrap_or_else(|bytes| bytes.as_slice().to_vec()),
                acyclic_harness_core::conversation::Custody::Charged(bytes) => {
                    bytes.as_slice().to_vec()
                }
            };
            let value =
                String::from_utf8(bytes).map_err(|_| invalid("model text file is not UTF-8"))?;
            drop(hold);
            text.push(ResolvedFile::Text(value));
        }
    }
    Ok(FileText(text.into_iter()))
}

fn reference(file: &FileRef) -> Result<String> {
    let capacity = acyclic_harness_provider_response::encoded_json_len(file)?
        .checked_add(8)
        .ok_or_else(|| invalid("file marker size overflows"))?;
    let mut bytes = Vec::with_capacity(capacity);
    bytes.extend_from_slice(b"[file: ");
    serde_json::to_writer(&mut bytes, file).map_err(|_| invalid("file marker encoding failed"))?;
    bytes.push(b']');
    String::from_utf8(bytes).map_err(|_| invalid("file marker is not UTF-8"))
}
