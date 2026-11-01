//! Nonserialized custody accompanying one immutable physical session frame.

use crate::{Record, StreamError, StreamPath, wire, wire_codec};

/// One received frame and any original provider record it still owns.
///
/// Wire transports construct this from their decoded frame and make no claim
/// about the backing allocation. In-process publication retains the original
/// record, including its native allocation guards, through the same session.
#[derive(Debug)]
pub struct Frame {
    wire: wire::SessionFrame,
    record: Option<Record>,
}

impl Frame {
    /// Freeze the provider's publicly mutable value at the one session boundary.
    /// Only this constructor pairs a record with its canonical encoded item.
    pub(super) fn from_record(
        identity: &wire::SessionChannel,
        record: Record,
        followed: Option<&StreamPath>,
        latest: bool,
    ) -> Result<(Self, Option<bytes::Bytes>), StreamError> {
        if record
            .value
            .len()
            .saturating_add(record.headers.payload_bytes())
            > crate::MAX_RECORD_BYTES
        {
            return Err(StreamError::LimitExceeded);
        }
        crate::changes::validate(&record.value, &record.headers)?;
        let next = record
            .sequence
            .checked_add(1)
            .ok_or(StreamError::OutOfRange)?;
        let (item, key) = if latest {
            let [key] = record.headers.keys() else {
                return Err(StreamError::InvalidArgument);
            };
            let attention = record
                .headers
                .attributes()
                .iter()
                .find(|attribute| attribute.name == "attention");
            let attention = match attention.map(|attribute| attribute.value.as_ref()) {
                None | Some(b"false") => false,
                Some(b"true") => true,
                _ => return Err(StreamError::InvalidArgument),
            };
            let value = wire::SessionLatestValue {
                key: bytes::Bytes::from_owner(key.clone()),
                value: bytes::Bytes::from_owner(record.value.clone()),
                attention,
            };
            let key = (!value.attention).then(|| value.key.clone());
            (wire::session_data::Item::LatestValue(value), key)
        } else {
            (
                wire::session_data::Item::Record(wire_codec::record_wire(record.clone(), followed)),
                None,
            )
        };
        let mut frame = Self::data(identity, super::cursor(next), item);
        frame.record = Some(record);
        Ok((frame, key))
    }

    pub(super) fn data(
        identity: &wire::SessionChannel,
        after: wire::SessionCursor,
        item: wire::session_data::Item,
    ) -> Self {
        super::frame(
            identity.channel_id,
            identity.generation,
            wire::session_frame::Body::Data(wire::SessionData {
                accepted: after.next,
                after: Some(after),
                durable: 0,
                item: Some(item),
            }),
        )
        .into()
    }

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

    /// The original record retained by in-process publication, with its
    /// body/header relation checked before admission. This does not authorize
    /// another read context. Wire-only transports cannot manufacture this
    /// process-local backing custody.
    #[must_use]
    pub fn original_record(&self) -> Option<&Record> {
        self.record.as_ref()
    }

    /// Checks the original read/follow context. In-process records have already
    /// passed body/header validation at session admission and retain their
    /// immutable headers and allocation owners. Wire-only frames pass through
    /// the canonical decoder on every projection.
    pub fn decode_record(&self, followed: Option<&StreamPath>) -> Result<Record, StreamError> {
        let Some(wire::session_frame::Body::Data(data)) = &self.wire.body else {
            return Err(StreamError::Unavailable);
        };
        let Some(wire::session_data::Item::Record(record)) = &data.item else {
            return Err(StreamError::Unavailable);
        };
        match &self.record {
            Some(original) => {
                validate_origin(original, record, followed)?;
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
            validate_origin(&original, &record, Some(path))?;
            return Ok(original);
        }
        wire_codec::record(record, Some(path))
    }
}

fn validate_origin(
    original: &Record,
    wire: &wire::Record,
    followed: Option<&StreamPath>,
) -> Result<(), StreamError> {
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
