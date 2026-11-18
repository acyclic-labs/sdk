/// One checksummed append-only log frame layout, shared by every local log:
/// `length | body | sha256(domain | length | body)`, the length little-endian.
/// A frame is whole or invalid, so replay stops at a torn append and
/// Recovery can tell it from damage to a committed frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogFrame {
    /// Separates this log's checksums from every other log's.
    pub domain: &'static [u8],
    /// An eight-byte length prefix, for bodies of any size, instead of four.
    pub wide: bool,
    /// The smallest body a frame of this log holds.
    pub minimum_body: u64,
    /// The largest body a frame of this log holds.
    pub maximum_body: u64,
}

impl LogFrame {
    /// The checksum's bytes, after the body.
    pub const CHECKSUM_BYTES: usize = 32;

    /// The length prefix's bytes.
    #[must_use]
    pub const fn prefix_bytes(&self) -> usize {
        if self.wide { 8 } else { 4 }
    }

    /// The bytes of a frame holding `body` bytes, if its prefix and slice
    /// endpoints are representable on this platform.
    #[must_use]
    pub const fn frame_bytes(&self, body: u64) -> Option<u64> {
        if !self.wide && body > u32::MAX as u64 {
            return None;
        }
        let Some(bytes) =
            body.checked_add(self.prefix_bytes() as u64 + Self::CHECKSUM_BYTES as u64)
        else {
            return None;
        };
        if bytes > usize::MAX as u64 {
            return None;
        }
        Some(bytes)
    }

    /// The largest frame of this log.
    #[must_use]
    pub const fn maximum_frame_bytes(&self) -> Option<u64> {
        self.frame_bytes(self.maximum_body)
    }
}
