//! Linked-content matcher for the shared stream search frontier.
//! A record matches on its own bytes, or on an immutable file it references,
//! read through the caller's existing authorized content reader. Neither
//! constructing a matcher nor an idle cursor reads or indexes data.
use crate::{
    Capabilities, Error, Result,
    conversation::{ContentResidencyVerifier, FileRef},
};
use acyclic_stream::{
    Record,
    search::{Matcher, Step, contains, find},
};
use serde::Deserialize as _;

/// Bytes read from a referenced file per step. A chunking size only: a long
/// file continues in later steps, so no match is ever missed.
const WINDOW: u64 = 64 * 1024;

/// Matches record bytes, then each referenced file through one reader.
/// A file the reader refuses is skipped, never reported: a continuation token
/// carries no read authority.
pub struct ContentMatcher<'a> {
    reader: &'a dyn ContentResidencyVerifier,
    ceiling: Option<&'a Capabilities>,
}

impl<'a> ContentMatcher<'a> {
    /// Retains the existing reader and the caller's current capability ceiling.
    pub fn new(
        reader: &'a dyn ContentResidencyVerifier,
        ceiling: Option<&'a Capabilities>,
    ) -> Self {
        Self { reader, ceiling }
    }

    async fn inspect(
        &self,
        record: &Record,
        pattern: &[u8],
        after: Option<&[u8]>,
    ) -> Result<Step<FileRef>> {
        let (file, offset) = match after {
            None if contains(record.value.as_slice(), pattern) => return Ok(done(None, true)),
            None => (0, 0),
            Some(after) => position(after)?,
        };
        let (reference, more) = reference(record, file)?;
        let Some(reference) = reference else {
            return if after.is_some() {
                Err(invalid("content search continuation differs from record"))
            } else {
                Ok(done(None, false))
            };
        };
        let length = reference.descriptor().byte_length();
        if offset > length {
            return Err(invalid("content search continuation exceeds source"));
        }
        let next_file = file
            .checked_add(1)
            .ok_or_else(|| invalid("content reference count overflow"))?;
        if offset == length {
            return Ok(progress(next_file, 0, more));
        }
        // Overlap the previous window so a hit crossing its edge is found.
        // An empty literal matched the record bytes and never continues here.
        let start = offset.saturating_sub((pattern.len() as u64).saturating_sub(1));
        let end = offset.saturating_add(WINDOW).min(length);
        let range = acyclic_fs::ByteRange {
            offset: start,
            length: end - start,
        };
        let bytes = match self
            .reader
            .read_range(&reference, range, self.ceiling, None)?
            .await
        {
            Ok(bytes) => bytes,
            Err(error) if error.code() == crate::wire::ErrorCode::Unauthorized => {
                return Ok(progress(next_file, 0, more));
            }
            Err(error) => return Err(error),
        };
        if bytes.len() as u64 != range.length {
            return Err(Error::Storage(
                "search range ended before its source cut".into(),
            ));
        }
        Ok(if find(bytes.as_slice(), pattern).is_some() {
            done(Some(reference), true)
        } else if end == length {
            progress(next_file, 0, more)
        } else {
            progress(file, end, true)
        })
    }
}

fn invalid(message: &'static str) -> Error {
    Error::Invalid(message.into())
}

fn done(source: Option<FileRef>, matched: bool) -> Step<FileRef> {
    Step {
        matched,
        source,
        next: None,
    }
}

/// Continuation within one record: the selected reference and its byte offset.
fn progress(file: usize, offset: u64, more: bool) -> Step<FileRef> {
    if !more {
        return done(None, false);
    }
    let mut frame = Vec::with_capacity(17);
    frame.push(1);
    frame.extend_from_slice(&(file as u64).to_be_bytes());
    frame.extend_from_slice(&offset.to_be_bytes());
    Step {
        matched: false,
        source: None,
        next: Some(frame),
    }
}

fn position(after: &[u8]) -> Result<(usize, u64)> {
    let refused = || invalid("invalid content search continuation");
    let [1, rest @ ..] = after else {
        return Err(refused());
    };
    let (file, offset) = rest.split_at_checked(8).ok_or_else(refused)?;
    let file = u64::from_be_bytes(file.try_into().map_err(|_| refused())?);
    let offset = u64::from_be_bytes(offset.try_into().map_err(|_| refused())?);
    Ok((usize::try_from(file).map_err(|_| refused())?, offset))
}

/// The `selected`th file reference in document order, and whether another follows.
fn reference(record: &Record, selected: usize) -> Result<(Option<FileRef>, bool)> {
    let bytes = record.value.as_slice();
    let value = if bytes.starts_with(&crate::agent::RECORD_VERSION.to_be_bytes()) {
        crate::agent::decode_record::<serde_json::Value>(bytes)
            .map_err(|_| invalid("search source record is malformed"))?
    } else {
        match serde_json::from_slice::<serde_json::Value>(bytes) {
            Ok(value) => value,
            // Opaque records have no references; their bytes were already matched.
            Err(_) => return Ok((None, false)),
        }
    };
    let mut pending = vec![&value];
    let mut found = None;
    let mut count = 0;
    while let Some(value) = pending.pop() {
        match value {
            serde_json::Value::Object(fields) => {
                // A candidate filter only; the reference decoder validates it.
                if fields.contains_key("descriptor")
                    && let Ok(file) = FileRef::deserialize(value)
                {
                    if count > selected {
                        return Ok((found, true));
                    }
                    if count == selected {
                        found = Some(file);
                    }
                    count += 1;
                    continue;
                }
                pending.extend(fields.values().rev());
            }
            serde_json::Value::Array(values) => pending.extend(values.iter().rev()),
            _ => {}
        }
    }
    Ok((found, false))
}

impl Matcher for ContentMatcher<'_> {
    type Source = FileRef;
    type Error = Error;
    fn identity(&self) -> [u8; 32] {
        *b"acyclic:search:linked-content:v2"
    }
    fn step<'a>(
        &'a self,
        record: &'a Record,
        pattern: &'a [u8],
        after: Option<&'a [u8]>,
    ) -> impl std::future::Future<Output = Result<Step<FileRef>>> + acyclic_stream::ProviderTask + 'a
    {
        self.inspect(record, pattern, after)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
