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

/// Match record values and exact file references in canonical agent or JSON records.
pub struct ContentMatcher<'a> {
    reader: &'a dyn ContentResidencyVerifier,
    ceiling: Option<&'a Capabilities>,
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
        Self { reader, ceiling }
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
        *b"acyclic:search:linked-content:v1"
    }
    fn step<'a>(
        &'a self,
        record: &'a Record,
        pattern: &'a [u8],
        after: Option<&'a [u8]>,
    ) -> impl std::future::Future<Output = Result<Step<FileRef>>> + acyclic_stream::ProviderTask + 'a
    {
        async move {
            if after.is_none() && contains(record.value.as_slice(), pattern) {
                return Ok(Step {
                    matched: true,
                    source: None,
                    next: None,
                });
            }
            let files = references(record)?;
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
            if position.offset == length {
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
            let start = position
                .offset
                .saturating_sub(pattern.len().saturating_sub(1) as u64);
            let end = position.offset.saturating_add(64 * 1024).min(length);
            let bytes = match self
                .reader
                .read_range(
                    file,
                    acyclic_fs::ByteRange {
                        offset: start,
                        length: end - start,
                    },
                    self.ceiling,
                    None,
                )?
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
            if bytes.len() as u64 != end - start {
                return Err(Error::Storage(
                    "search range ended before its source cut".into(),
                ));
            }
            if contains(bytes.as_slice(), pattern) {
                return Ok(Step {
                    matched: true,
                    source: Some(file.clone()),
                    next: None,
                });
            }
            progress(
                if end == length {
                    Position {
                        file: position.file + 1,
                        offset: 0,
                    }
                } else {
                    Position {
                        file: position.file,
                        offset: end,
                    }
                },
                files.len(),
            )
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
