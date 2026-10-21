//! File mentions resolve through the same authorized pinned reads as context.

use crate::{
    Error, Result,
    context::{ContextRoot, PinnedContextPath},
    conversation::{ContentResidencyVerifier, FileRef},
    resources::GenerationRef,
};
use std::collections::BTreeMap;

pub use crate::command_request::FileMention;

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
    for (start, end, path) in file_mention_paths(input)? {
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
        result.push(FileMention { start, end, file });
    }
    Ok(result)
}

/// Parse once before checkout capture and again against the captured revision.
/// Both routes use precisely the same ranges and path validation.
pub(crate) fn file_mention_paths(input: &str) -> Result<Vec<(usize, usize, &str)>> {
    let mut result = Vec::new();
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
        crate::conversation::validate_content_path(path, &mut None)?;
        if crate::conversation::is_internal_path(path) {
            return Err(Error::Invalid(
                "file mention targets internal content".into(),
            ));
        }
        result.push((start, end, path));
        offset = end;
    }
    Ok(result)
}
