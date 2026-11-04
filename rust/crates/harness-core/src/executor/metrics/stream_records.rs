//! Offline logical record references from the original stream envelope.
//!
//! The caller supplies the authenticated source account and selected path.
//! This projection does not authenticate a fabricated record or discover its
//! provider. A shared logical reference does not prove physical bytes saved.

use super::{Batch, Row, Table, batch, export_error};
use crate::{Result, live::resources::Sharing};
use acyclic_auth::addressing::AccountId;
use acyclic_stream::{Record, StreamPath};

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
    let sharing = Sharing::project(account, selected, record).map_err(export_error)?;
    let mut row = Row::stream(
        Some(agent_path),
        sharing.source,
        if sharing.inherited {
            "stream_record_shared"
        } else {
            "stream_record"
        },
    );
    // Framed original fields, not a content hash or a reconstructed ancestry.
    row.source = Some(
        serde_json::to_string(&(
            &row.source_account,
            sharing.source.origin,
            sharing.source.sequence,
            &row.source_commit_id,
        ))
        .map_err(export_error)?,
    );
    row.source_type = Some("stream_record");
    row.target = Some(
        serde_json::to_string(&(&row.source_account, sharing.source.stream))
            .map_err(export_error)?,
    );
    row.target_type = Some("stream");
    row.logical_value_bytes = Some(sharing.logical_value_bytes);
    row.physical_shared_bytes = sharing.physical_shared_bytes;
    row.physical_copied_bytes = sharing.physical_copied_bytes;
    row.coverage = Some("logical_record_value_bytes".into());
    batch(Table::Edges, &[row])
}

#[cfg(test)]
mod tests;
