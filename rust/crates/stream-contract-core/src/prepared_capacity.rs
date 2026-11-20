//! Storage selected by the caller before a prepared record's external effect.

use crate::OwnedBytes;
use crate::RecordHeaders;
use crate::StreamError;

/// Storage selected by the caller before a prepared record's external effect.
/// These capacities reserve the original operation's bytes and header controls;
/// there is no implicit header count or default capacity.
#[derive(Clone, Copy, Debug)]
pub struct PreparedRecordCapacity {
    /// Combined value, key, attribute-name and attribute-value bytes.
    pub payload_bytes: usize,
    /// Indexed key slots to reserve before issuance.
    pub keys: usize,
    /// Attribute slots to reserve before issuance.
    pub attributes: usize,
}

impl PreparedRecordCapacity {
    /// Reject a record whose bytes or header controls exceed the reservation.
    pub fn validate(&self, value: &OwnedBytes, headers: &RecordHeaders) -> Result<(), StreamError> {
        // Inspect counts before traversing header payload: a recovered record may
        // have been published by an independent writer after this reservation.
        if headers.keys().len() > self.keys
            || headers.attributes().len() > self.attributes
            || value.len() > self.payload_bytes
            || headers.payload_bytes() > self.payload_bytes - value.len()
        {
            return Err(StreamError::LimitExceeded);
        }
        Ok(())
    }
}
