//! Canonical provider-authored change lists with entry-level follow projection.
use crate::{AppendRecord, Attribute, RecordHeaders, StreamError, wire};
use prost::Message;

#[cfg(test)]
mod tests;
mod view;

const ATTRIBUTE: &str = "acyclic.record";
const FORMAT: &[u8] = b"changes/v1";

pub(crate) fn marked(headers: &RecordHeaders) -> bool {
    headers
        .attributes()
        .iter()
        .any(|attribute| attribute.name == ATTRIBUTE && attribute.value.as_ref() == FORMAT)
}

pub(crate) fn decode(
    bytes: &[u8],
    headers: &RecordHeaders,
) -> Result<wire::ChangeRecord, StreamError> {
    view::validate(bytes, headers)?;
    wire::ChangeRecord::decode(bytes).map_err(|_| StreamError::InvalidArgument)
}

pub(crate) fn validate(bytes: &[u8], headers: &RecordHeaders) -> Result<(), StreamError> {
    if !marked(headers) {
        return Ok(());
    }
    view::validate(bytes, headers)
}

fn bounded(bytes: &[u8], headers: &RecordHeaders) -> Result<(), StreamError> {
    if bytes.len().saturating_add(headers.payload_bytes()) > crate::MAX_RECORD_BYTES {
        return Err(StreamError::LimitExceeded);
    }
    Ok(())
}

impl AppendRecord {
    /// Encodes a typed change list. Entries must have sorted unique keys; the
    /// shared context contains publication identity, never path-specific facts.
    /// Large publications use consecutive chunks in the same atomic commit.
    pub fn changes(
        context: bytes::Bytes,
        entries: Vec<wire::ChangeEntry>,
        mut attributes: Vec<Attribute>,
    ) -> Result<Self, StreamError> {
        if attributes
            .iter()
            .any(|attribute| attribute.name == ATTRIBUTE)
        {
            return Err(StreamError::InvalidArgument);
        }
        attributes.push(Attribute {
            name: ATTRIBUTE.into(),
            value: FORMAT.into(),
        });
        attributes.sort_by(|left, right| left.name.cmp(&right.name));
        let headers = RecordHeaders::new(
            entries
                .iter()
                .map(|entry| entry.key.clone().into())
                .collect(),
            attributes,
        )?;
        let changes = wire::ChangeRecord { context, entries };
        if changes
            .encoded_len()
            .saturating_add(headers.payload_bytes())
            > crate::MAX_RECORD_BYTES
        {
            return Err(StreamError::LimitExceeded);
        }
        let value: crate::OwnedBytes = changes.encode_to_vec().into();
        validate(&value, &headers)?;
        Ok(Self { value, headers })
    }
}
