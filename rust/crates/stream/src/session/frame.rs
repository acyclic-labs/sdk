//! Nonserialized custody accompanying one immutable physical session frame.

use crate::{Record, wire};

/// One received frame and any original provider record it still owns.
///
/// Wire transports construct this from their decoded frame and make no claim
/// about the backing allocation. In-process publication retains the original
/// record, including its native allocation guards, through the same session.
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
}

impl From<wire::SessionFrame> for Frame {
    fn from(wire: wire::SessionFrame) -> Self {
        Self { wire, record: None }
    }
}
