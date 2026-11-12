//! Shared search adapter for original versioned content referenced by records.
//! It reuses the stream search frontier and the existing authorized content
//! reader. Neither constructing it nor an idle cursor reads or indexes data.
use crate::{
    Capabilities, Error, Result,
    conversation::{ContentResidencyVerifier, FileRef},
};
use acyclic_stream::{
    Record,
    search::{Matcher, Step, contains},
};
use serde::{Deserialize, Serialize};
mod files;
pub use files::{FileCursor, FileMatch, FilePage, FileRootKind};

fn source_window(offset: u64, pattern: &[u8]) -> acyclic_fs::ByteRange {
    let start = offset.saturating_sub(pattern.len().saturating_sub(1) as u64);
    acyclic_fs::ByteRange {
        offset: start,
        length: if pattern.is_empty() {
            0
        } else {
            offset - start + 64 * 1024
        },
    }
}

enum Window {
    Match(u64),
    More(u64),
    Done,
}

fn inspect_window(length: u64, offset: u64, pattern: &[u8], bytes: &[u8]) -> Result<Window> {
    if offset > length {
        return Err(Error::Invalid("search cursor exceeds its source".into()));
    }
    if pattern.is_empty() {
        return Ok(Window::Match(0));
    }
    let range = source_window(offset, pattern);
    let end = offset.saturating_add(64 * 1024).min(length);
    if bytes.len() as u64 != end - range.offset {
        return Err(Error::Storage(
            "search range ended before its source cut".into(),
        ));
    }
    if let Some(found) = acyclic_stream::search::find(bytes, pattern) {
        return Ok(Window::Match(range.offset + found as u64));
    }
    Ok(if end == length {
        Window::Done
    } else {
        Window::More(end)
    })
}

/// Match record values and exact file references in canonical agent or JSON records.
pub struct ContentMatcher<'a> {
    reader: &'a dyn ContentResidencyVerifier,
    ceiling: Option<&'a Capabilities>,
    user_prompts: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Position {
    file: usize,
    offset: u64,
}

impl<'a> ContentMatcher<'a> {
    /// Retain the caller's existing reader and current capability ceiling.
    pub fn new(
        reader: &'a dyn ContentResidencyVerifier,
        ceiling: Option<&'a Capabilities>,
    ) -> Self {
        Self {
            reader,
            ceiling,
            user_prompts: false,
        }
    }

    /// Project committed human prompts from the selected session subtree.
    /// Only original user-message bodies match: inherited records, agent/host
    /// messages, attachments and record metadata are excluded. Empty literals
    /// enumerate original body references without materializing their text.
    /// The caller's provider and content grants remain authoritative per page.
    pub fn user_prompts(
        reader: &'a dyn ContentResidencyVerifier,
        ceiling: Option<&'a Capabilities>,
    ) -> Self {
        Self {
            reader,
            ceiling,
            user_prompts: true,
        }
    }
}

fn prompt_reference(record: &Record) -> Vec<FileRef> {
    use crate::agent::{AgentRecord as _, InboxRecord, Sender};
    match InboxRecord::decode(&record.value) {
        Ok(InboxRecord::Message(message) | InboxRecord::FollowUp(message))
            if matches!(message.from, Sender::User { .. }) =>
        {
            vec![*message.body]
        }
        _ => Vec::new(),
    }
}

fn references(record: &Record) -> Result<Vec<FileRef>> {
    let value = if record
        .value
        .as_slice()
        .starts_with(&crate::agent::RECORD_VERSION.to_be_bytes())
    {
        crate::agent::decode_record::<serde_json::Value>(&record.value)
            .map_err(|error| Error::Invalid(error.to_string()))?
    } else {
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(&record.value) else {
            return Ok(Vec::new());
        };
        value
    };
    let mut pending = vec![&value];
    let mut files = Vec::new();
    while let Some(value) = pending.pop() {
        match value {
            serde_json::Value::Object(fields) => {
                if fields.contains_key("descriptor")
                    && let Ok(file) = FileRef::deserialize(value)
                {
                    files.push(file);
                } else {
                    pending.extend(fields.values().rev());
                }
            }
            serde_json::Value::Array(values) => pending.extend(values.iter().rev()),
            _ => {}
        }
    }
    Ok(files)
}

fn progress(position: Position, files: usize) -> Result<Step<FileRef>> {
    Ok(Step {
        matched: false,
        source: None,
        next: (position.file < files)
            .then(|| serde_json::to_vec(&position))
            .transpose()
            .map_err(|error| Error::Invalid(error.to_string()))?,
    })
}

impl Matcher for ContentMatcher<'_> {
    type Source = FileRef;
    type Error = Error;
    fn identity(&self) -> [u8; 32] {
        if self.user_prompts {
            *b"acyclic:search:prompt-content:v1"
        } else {
            *b"acyclic:search:linked-content:v1"
        }
    }
    fn selects(&self, path: &acyclic_stream::StreamPath, record: &Record) -> bool {
        !self.user_prompts || path == &record.origin
    }
    fn step<'a>(
        &'a self,
        record: &'a Record,
        pattern: &'a [u8],
        after: Option<&'a [u8]>,
    ) -> impl std::future::Future<Output = Result<Step<FileRef>>> + acyclic_stream::ProviderTask + 'a
    {
        async move {
            if !self.user_prompts && after.is_none() && contains(record.value.as_slice(), pattern) {
                return Ok(Step {
                    matched: true,
                    source: None,
                    next: None,
                });
            }
            let files = if self.user_prompts {
                prompt_reference(record)
            } else {
                references(record)?
            };
            let position = match after {
                Some(after) => serde_json::from_slice::<Position>(after)
                    .map_err(|_| Error::Invalid("invalid content search cursor".into()))?,
                None => Position { file: 0, offset: 0 },
            };
            let Some(file) = files.get(position.file) else {
                if after.is_some() {
                    return Err(Error::Invalid(
                        "content search cursor differs from record".into(),
                    ));
                }
                return Ok(Step {
                    matched: false,
                    source: None,
                    next: None,
                });
            };
            let length = file.descriptor().byte_length();
            if position.offset > length {
                return Err(Error::Invalid(
                    "content search cursor exceeds source".into(),
                ));
            }
            if position.offset == length && !pattern.is_empty() {
                return progress(
                    Position {
                        file: position.file + 1,
                        offset: 0,
                    },
                    files.len(),
                );
            }
            // Overlap comes from the original file, never from cursor-supplied
            // bytes. A forged continuation cannot manufacture a matching span.
            let mut range = source_window(position.offset, pattern);
            range.length = range.length.min(length - range.offset);
            let bytes = match self
                .reader
                .read_range(file, range, self.ceiling, None)?
                .await
            {
                Ok(bytes) => bytes,
                Err(Error::Unauthorized(_)) => {
                    return progress(
                        Position {
                            file: position.file + 1,
                            offset: 0,
                        },
                        files.len(),
                    );
                }
                Err(error) => return Err(error),
            };
            let next = match inspect_window(length, position.offset, pattern, bytes.as_slice())? {
                Window::Match(_) => {
                    return Ok(Step {
                        matched: true,
                        source: Some(file.clone()),
                        next: None,
                    });
                }
                Window::More(offset) => Position {
                    file: position.file,
                    offset,
                },
                Window::Done => Position {
                    file: position.file + 1,
                    offset: 0,
                },
            };
            progress(next, files.len())
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
