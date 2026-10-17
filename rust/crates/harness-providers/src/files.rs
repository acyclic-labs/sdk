use crate::invalid;
use acyclic_harness::{
    Error, Result,
    conversation::{ContentResidencyVerifier, FileRef, Limits, verified_content_bytes},
    model::{
        FileProjectionPolicy, ModelContent, ModelContentPart, ModelDataPart, ModelRequest,
        ToolResultContent,
    },
};
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
pub(crate) struct FileText(pub(crate) std::vec::IntoIter<String>);

impl FileText {
    pub(crate) fn project(
        &mut self,
        file: &FileRef,
        policy: &FileProjectionPolicy,
    ) -> Result<String> {
        match policy {
            FileProjectionPolicy::Reference => reference(file),
            FileProjectionPolicy::BoundedFull | FileProjectionPolicy::ProjectInstructions(_) => {
                self.0
                    .next()
                    .ok_or_else(|| invalid("missing resolved model file"))
            }
            FileProjectionPolicy::Native(_) => Err(Error::Unsupported(
                "native model media is unsupported by this adapter".into(),
            )),
        }
    }
}

fn parts(content: &ModelContent) -> impl Iterator<Item = (&FileRef, &FileProjectionPolicy)> {
    content.parts().iter().flat_map(|part| {
        let direct = match part {
            ModelContentPart::File { file, policy } => Some((file, policy)),
            _ => None,
        };
        let result = match part {
            ModelContentPart::ToolResult {
                content: ToolResultContent::Parts { parts },
                ..
            } => parts.as_slice(),
            _ => &[],
        };
        direct
            .into_iter()
            .chain(result.iter().filter_map(|part| match part {
                ModelDataPart::File { file, policy } => Some((file, policy)),
                _ => None,
            }))
    })
}

fn validate(file: &FileRef, policy: &FileProjectionPolicy, maximum: u64) -> Result<()> {
    match policy {
        FileProjectionPolicy::Reference => Ok(()),
        FileProjectionPolicy::BoundedFull | FileProjectionPolicy::ProjectInstructions(_) => {
            if !file.descriptor().media_type().starts_with("text/") {
                return Err(Error::Unsupported(
                    "bounded model files must be text".into(),
                ));
            }
            if projected_bytes(file, policy)? > maximum {
                return Err(invalid("model file exceeds projection byte limit"));
            }
            Ok(())
        }
        FileProjectionPolicy::Native(_) => Err(Error::Unsupported(
            "native model media is unsupported by this adapter".into(),
        )),
    }
}

fn projected_bytes(file: &FileRef, policy: &FileProjectionPolicy) -> Result<u64> {
    if let FileProjectionPolicy::ProjectInstructions(source) = policy {
        acyclic_harness::model::InstructionSource::with_identity(file, source).framed_bytes()
    } else {
        Ok(file.descriptor().byte_length())
    }
}

pub(crate) fn extra_bytes(content: &ModelContent, maximum: u64) -> Result<usize> {
    let mut extra = 0usize;
    for (file, policy) in parts(content) {
        validate(file, policy, maximum)?;
        if matches!(
            policy,
            FileProjectionPolicy::BoundedFull | FileProjectionPolicy::ProjectInstructions(_)
        ) {
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
    io: Option<&acyclic_harness::conversation::ContentIo>,
) -> Result<FileText> {
    // Reject the entire unsupported request before invoking any owner reader.
    let mut remaining = files.maximum_bytes;
    for message in &request.messages {
        for (file, policy) in parts(&message.content) {
            validate(file, policy, files.maximum_bytes)?;
            if !matches!(policy, FileProjectionPolicy::Reference) {
                remaining = remaining
                    .checked_sub(projected_bytes(file, policy)?)
                    .ok_or_else(|| invalid("model files exceed aggregate projection byte limit"))?;
            }
        }
    }
    let mut remaining = files.maximum_bytes;
    let mut text = Vec::new();
    for message in &request.messages {
        for (file, policy) in parts(&message.content) {
            if !matches!(
                policy,
                FileProjectionPolicy::BoundedFull | FileProjectionPolicy::ProjectInstructions(_)
            ) {
                continue;
            }
            remaining = remaining
                .checked_sub(projected_bytes(file, policy)?)
                .ok_or_else(|| invalid("model files exceed aggregate projection byte limit"))?;
            let reader = files.reader.as_ref().ok_or_else(|| {
                Error::Unsupported("model file byte resolution is unavailable".into())
            })?;
            let (bytes, hold) =
                verified_content_bytes(reader.as_ref(), file, file.descriptor().byte_length(), io)
                    .await?;
            if let FileProjectionPolicy::ProjectInstructions(source) = policy {
                let value = acyclic_harness::model::InstructionSource::with_identity(file, source)
                    .render(&bytes, io)?;
                drop(hold);
                text.push(value);
                continue;
            }
            let bytes = bytes
                .try_into_ordinary_vec()
                .unwrap_or_else(|bytes| bytes.as_slice().to_vec());
            let value =
                String::from_utf8(bytes).map_err(|_| invalid("model text file is not UTF-8"))?;
            drop(hold);
            text.push(value);
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
