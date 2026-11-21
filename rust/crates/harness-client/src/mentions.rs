//! File mentions resolve through the same authorized pinned reads as context.

use crate::{
    Error, Result,
    context::{ContextRoot, PinnedContextPath},
    conversation::{ContentResidencyVerifier, FileRef},
    resources::GenerationRef,
};
use std::collections::BTreeMap;

pub use crate::command_request::FileMention;

/// Resolves whitespace-delimited `@path` and `@"path with spaces"` at one revision,
/// reading each mentioned path where `locate` places it below `root`.
/// Email addresses, escaped `\@` and embedded `@` are ordinary text. Paths are
/// relative to the declared root; absolute and parent paths are rejected
/// by the existing context validator. Repeated paths read once. The caller's byte
/// limit bounds the input and the combined unique file bodies independently.
#[doc(hidden)]
pub async fn resolve_file_mentions_at(
    input: &str,
    root: &ContextRoot,
    generation: &GenerationRef,
    reader: &dyn ContentResidencyVerifier,
    maximum_bytes: impl Into<Option<u64>>,
    locate: impl Fn(&str) -> String,
) -> Result<Vec<FileMention>> {
    let maximum_bytes = maximum_bytes.into();
    if maximum_bytes.is_some_and(|maximum| maximum == 0 || input.len() as u64 > maximum) {
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
                path: locate(path),
            };
            let (file, bytes) = source.read_remaining(reader, remaining).await?;
            if let Some(remaining) = &mut remaining {
                *remaining = remaining
                    .checked_sub(bytes.len() as u64)
                    .ok_or_else(|| Error::Storage("mention reader exceeded bound".into()))?;
            }
            files.insert(path.into(), file.clone());
            file
        };
        result.push(FileMention { start, end, file });
    }
    Ok(result)
}

/// Parse once before checkout capture and again against the captured revision.
/// Both routes use precisely the same ranges and path validation.
#[doc(hidden)]
pub fn file_mention_paths(input: &str) -> Result<Vec<(usize, usize, &str)>> {
    let mut result = Vec::new();
    let invalid_offset = || Error::Invalid("invalid mention offset".into());
    let mut offset = 0;
    let mut previous_whitespace = true;
    while offset < input.len() {
        let tail = input.get(offset..).ok_or_else(invalid_offset)?;
        let character = tail.chars().next().ok_or_else(invalid_offset)?;
        let Some(tail) = tail.strip_prefix('@').filter(|_| previous_whitespace) else {
            previous_whitespace = character.is_whitespace();
            offset += character.len_utf8();
            continue;
        };
        let start = offset;
        let (path, end) = if let Some(quoted) = tail.strip_prefix('"') {
            let (path, _) = quoted
                .split_once('"')
                .ok_or_else(|| Error::Invalid("unclosed file mention quote".into()))?;
            (path, start + 3 + path.len())
        } else {
            let length = tail.find(char::is_whitespace).unwrap_or(tail.len());
            (
                tail.get(..length).ok_or_else(invalid_offset)?,
                start + 1 + length,
            )
        };
        if path.is_empty() {
            return Err(Error::Invalid("empty file mention".into()));
        }
        if end < input.len()
            && !input
                .get(end..)
                .ok_or_else(invalid_offset)?
                .starts_with(char::is_whitespace)
        {
            return Err(Error::Invalid("file mention must end at whitespace".into()));
        }
        previous_whitespace = false;
        crate::conversation::validate_content_path(path, &mut None)?;
        result.push((start, end, path));
        offset = end;
    }
    Ok(result)
}
