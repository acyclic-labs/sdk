//! File mentions resolve through the same authorized pinned reads as context.

use crate::{
    Error, Result,
    context::{ContextRoot, PinnedContextPath},
    conversation::{ContentResidencyVerifier, FileRef},
    resources::GenerationRef,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Exact composer range and immutable file selected by an `@` mention.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMention {
    /// Inclusive UTF-8 byte offset of `@`.
    pub start: usize,
    /// Exclusive UTF-8 byte offset, including closing quotes when present.
    pub end: usize,
    /// Owner-verified immutable content reference.
    pub file: FileRef,
    /// Exact model admission policy selected at the prompt's file-query cut.
    pub image_input: Option<crate::model::ImageInput>,
}

/// Resolves whitespace-delimited `@path` and `@"path with spaces"` at one revision.
/// Email addresses, escaped `\@` and embedded `@` are ordinary text. Paths are
/// relative to the declared root; absolute, parent and internal paths are rejected
/// by the existing context validator. Repeated paths read once. The caller's byte
/// limit bounds the input and the combined unique file bodies independently.
pub async fn resolve_file_mentions(
    input: &str,
    root: &ContextRoot,
    generation: &GenerationRef,
    reader: &dyn ContentResidencyVerifier,
    maximum_bytes: u64,
) -> Result<Vec<FileMention>> {
    if maximum_bytes == 0 || input.len() as u64 > maximum_bytes {
        return Err(Error::Invalid("mention input exceeds byte bound".into()));
    }
    let mut result = Vec::new();
    let mut files: BTreeMap<String, FileRef> = BTreeMap::new();
    let mut remaining = maximum_bytes;
    let mut offset = 0;
    while offset < input.len() {
        let tail = &input[offset..];
        let character = tail
            .chars()
            .next()
            .ok_or_else(|| Error::Invalid("invalid mention offset".into()))?;
        if character != '@' || (offset != 0 && !input[..offset].ends_with(char::is_whitespace)) {
            offset += character.len_utf8();
            continue;
        }
        let start = offset;
        let tail = &tail[1..];
        let (path, end) = if let Some(quoted) = tail.strip_prefix('"') {
            let close = quoted
                .find('"')
                .ok_or_else(|| Error::Invalid("unclosed file mention quote".into()))?;
            (&quoted[..close], start + 3 + close)
        } else {
            let length = tail.find(char::is_whitespace).unwrap_or(tail.len());
            (&tail[..length], start + 1 + length)
        };
        if path.is_empty() {
            return Err(Error::Invalid("empty file mention".into()));
        }
        if end < input.len() && !input[end..].starts_with(char::is_whitespace) {
            return Err(Error::Invalid("file mention must end at whitespace".into()));
        }
        let file = if let Some(file) = files.get(path) {
            file.clone()
        } else {
            let source = PinnedContextPath {
                root: root.clone(),
                generation: generation.clone(),
                path: if root.directory.is_empty() {
                    path.into()
                } else {
                    format!("{}/{path}", root.directory)
                },
            };
            let (file, bytes) = source.read_remaining(reader, remaining).await?;
            remaining = remaining
                .checked_sub(bytes.len() as u64)
                .ok_or_else(|| Error::Storage("mention reader exceeded bound".into()))?;
            files.insert(path.into(), file.clone());
            file
        };
        result.push(FileMention {
            start,
            end,
            file,
            image_input: None,
        });
        offset = end;
    }
    Ok(result)
}
