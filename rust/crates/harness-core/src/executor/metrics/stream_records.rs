//! Offline logical record references from the original stream envelope.
//!
//! The caller supplies the authenticated source account and selected path.
//! This projection does not authenticate a fabricated record or discover its
//! provider. A shared logical reference does not prove physical bytes saved.

use super::{Batch, Row, Table, batch, digest, export_error};
use crate::Result;
use acyclic_auth::addressing::AccountId;
use acyclic_stream::{Record, StreamPath};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

/// Projects one already-authorized original record into the existing edge table.
///
/// `selected` is the path read, while `record.origin` is the path that appended
/// the record. A fork preserves the original origin, sequence and commit ID.
/// Zero is a valid stream sequence. The record value is neither read nor hashed.
/// Union `source` identities before summing `logical_value_bytes`: siblings can
/// share a record with each other and their parent. Account bytes are encoded
/// losslessly as URL-safe base64 without padding; display names are not keys.
///
/// This synchronous projection adds no wait, scan, publication or provider call.
/// Its retained state is one row, independent of history, ancestry and agents.
pub fn project(
    agent_path: &str,
    account: &AccountId,
    selected: &StreamPath,
    record: &Record,
) -> Result<Batch> {
    let mut row = Row::new(
        agent_path,
        None,
        record.sequence,
        selected.as_str(),
        if selected == &record.origin {
            "stream_record"
        } else {
            "stream_record_shared"
        },
    );
    let account = URL_SAFE_NO_PAD.encode(account.as_bytes());
    let commit = digest(*record.commit_id.as_bytes());
    // Framed original fields, not a content hash or a reconstructed ancestry.
    row.source = Some(
        serde_json::to_string(&(&account, &record.origin, record.sequence, &commit))
            .map_err(export_error)?,
    );
    row.source_type = Some("stream_record");
    row.target = Some(serde_json::to_string(&(&account, selected)).map_err(export_error)?);
    row.target_type = Some("stream");
    row.source_account = Some(account);
    row.source_origin = Some(record.origin.as_str().into());
    row.source_commit_id = Some(commit);
    row.logical_value_bytes = Some(record.value.len() as u64);
    row.coverage = Some("logical_record_value_bytes".into());
    batch(Table::Edges, &[row])
}

#[cfg(test)]
mod tests;
