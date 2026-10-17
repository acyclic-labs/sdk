//! GraphCoder and the harness headless runner use one filesystem JSONL dialect.
//!
//! Submit: one serialized `task_volume::FileValue` request per line. Its key is
//! `FileKey::Session { session, file: TaskFile::Request(operation) }`, and its
//! bytes are the serialized `CommandResolution` (or `CommandRequest`). The
//! caller fixes the operation/session before opening either `-p` or JSONL mode.
//! Reusing exactly those bytes and that identity is a retry, not another prompt.
//!
//! Observe: the first stdout line is `(Query, HeadSnapshot)`, represented as a
//! JSON array. `HeadSnapshot.cursor` is the exact accepted cut. Subsequent lines
//! are `task_volume::Change` values following from that cursor, with no gaps.
//! Requests, execution claims/outcomes, canonical chunks, and materialized text
//! heads use the same task-volume schemas as every other file client. `-p` and
//! `--input-format jsonl` produce this identical observation stream. EOF drains
//! accepted work; stderr contains diagnostics, never another stdout protocol.
//!
//! JSON values use their Rust serde representations; file bytes are the
//! `OwnedBytes` representation, with no protobuf wrappers or alternate dialect.
//! The helpers below are the codecs for this contract. Connection admission
//! separately checks owner, operation, immutable bytes and authorized paths.
use crate::{
    Error, Result,
    task_volume::{Change, FileValue, HeadSnapshot, Query},
};

/// Maximum serialized request line accepted before parsing.
pub const MAXIMUM_REQUEST_BYTES: usize = 65_536;

fn encode(value: &impl serde::Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(|error| Error::Invalid(error.to_string()))
}
fn decode<T: serde::de::DeserializeOwned>(line: &str) -> Result<T> {
    serde_json::from_str(line).map_err(|error| Error::Invalid(error.to_string()))
}
/// Serialize a request file without a trailing newline.
pub fn request(file: &FileValue) -> Result<String> {
    encode(file)
}
/// Parse a bounded request file; the connection enforces its admission policy.
pub fn parse_request(line: &str) -> Result<FileValue> {
    if line.len() > MAXIMUM_REQUEST_BYTES {
        return Err(Error::Invalid("stdin line exceeds byte bound".into()));
    }
    decode(line)
}
/// Serialize the initial query and exact materialized-head snapshot.
pub fn snapshot(query: &Query, head: &HeadSnapshot) -> Result<String> {
    encode(&(query, head))
}
/// Parse the first observation line.
pub fn parse_snapshot(line: &str) -> Result<(Query, HeadSnapshot)> {
    decode(line)
}
/// Serialize one cursor-follow publication.
pub fn change(value: &Change) -> Result<String> {
    encode(value)
}
/// Parse a subsequent observation publication.
pub fn parse_change(line: &str) -> Result<Change> {
    decode(line)
}
