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
        // Keep canonical validation and derived headers on every transport.
        // Transfer only the exact original publication's payload owner.
        let mut decoded = wire_codec::record(record, Some(path))?;
        if let Some(original) = original {
            decoded.value = original.value;
        }
        Ok(decoded)
    }
}

impl From<wire::SessionFrame> for Frame {
    fn from(wire: wire::SessionFrame) -> Self {
        Self { wire, record: None }
    }
}
