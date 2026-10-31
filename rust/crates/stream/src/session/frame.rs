//! Nonserialized custody accompanying one immutable physical session frame.

use crate::{Record, StreamError, StreamPath, wire, wire_codec};

/// One received frame and any original provider record it still owns.
///
/// Wire transports construct this from their decoded frame and make no claim
/// about the backing allocation. In-process publication retains the original
/// record, including its native allocation guards, through the same session.
#[derive(Debug)]
pub struct Frame {
    pub(super) wire: wire::SessionFrame,
    pub(super) record: Option<Record>,
}

impl Frame {
    /// The original canonical frame. Its body cannot be changed independently
    /// of the retained provider record.
    #[must_use]
    pub fn wire(&self) -> &wire::SessionFrame {
        &self.wire
    }

    /// Releases the nonserialized ownership metadata at a wire boundary.
    #[must_use]
    pub fn into_wire(self) -> wire::SessionFrame {
        self.wire
    }

    /// The original record retained by in-process publication. Wire-only
    /// transports cannot manufacture this process-local backing custody.
    #[must_use]
    pub fn original_record(&self) -> Option<&Record> {
        self.record.as_ref()
    }

    /// Validates a record in its original read/follow context. In-process
    /// frames reuse their immutable headers and allocation owners; wire-only
    /// frames pass through the canonical decoder.
    pub fn decode_record(&self, followed: Option<&StreamPath>) -> Result<Record, StreamError> {
        let Some(wire::session_frame::Body::Data(data)) = &self.wire.body else {
            return Err(StreamError::Unavailable);
        };
        let Some(wire::session_data::Item::Record(record)) = &data.item else {
            return Err(StreamError::Unavailable);
        };
        match &self.record {
            Some(original) => {
                validate_original(original, record, followed)?;
                Ok(original.clone())
            }
            None => wire_codec::record(record.clone(), followed),
        }
    }

    pub(super) fn into_data(self) -> Result<wire::SessionData, StreamError> {
        match self.wire.body {
            Some(wire::session_frame::Body::Data(data)) => Ok(data),
            _ => Err(StreamError::Unavailable),
        }
    }

    pub(super) fn into_record(self, path: &StreamPath) -> Result<Record, StreamError> {
        let original = self.record;
        let Some(wire::session_frame::Body::Data(data)) = self.wire.body else {
            return Err(StreamError::Unavailable);
        };
        let Some(wire::session_data::Item::Record(record)) = data.item else {
            return Err(StreamError::Unavailable);
        };
        if let Some(original) = original {
            validate_original(&original, &record, Some(path))?;
            return Ok(original);
        }
        wire_codec::record(record, Some(path))
    }
}

fn validate_original(
    original: &Record,
    wire: &wire::Record,
    followed: Option<&StreamPath>,
) -> Result<(), StreamError> {
    // Only Session publication constructs this immutable pair. RecordHeaders
    // already proves canonical metadata; its public producer can still change
    // the body, so validate the body/header relation before exposing the record.
    if original
        .value
        .len()
        .saturating_add(original.headers.payload_bytes())
        > crate::MAX_RECORD_BYTES
    {
        return Err(StreamError::LimitExceeded);
    }
    crate::changes::validate(&original.value, &original.headers)?;
    // Custody does not authorize a different read context. Use the encoder's
    // same origin rule, including elision and redundant spelling refusal.
    let origin = wire_codec::spelled_origin(original, followed).map_or("", StreamPath::as_str);
    if wire.origin != origin {
        return Err(StreamError::InvalidArgument);
    }
    Ok(())
}

impl From<wire::SessionFrame> for Frame {
    fn from(wire: wire::SessionFrame) -> Self {
        Self { wire, record: None }
    }
}
