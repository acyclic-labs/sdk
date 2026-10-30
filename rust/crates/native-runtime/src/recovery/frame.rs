use sha2::{Digest as _, Sha256};
use std::io;

/// One checksummed append-only log frame layout, shared by every local log:
/// `length | body | sha256(domain | length | body)`, the length little-endian.
/// A frame is whole or invalid, so replay stops at a torn append and
/// [`crate::recover_log_tail`] tells it from damage to a committed frame.
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

    /// The bytes of a frame holding `body` bytes.
    #[must_use]
    pub const fn frame_bytes(&self, body: u64) -> u64 {
        self.prefix_bytes() as u64 + body + Self::CHECKSUM_BYTES as u64
    }

    /// The largest frame of this log.
    #[must_use]
    pub const fn maximum_frame_bytes(&self) -> u64 {
        self.frame_bytes(self.maximum_body)
    }

    /// The body length `prefix` declares, if it is one this log holds.
    #[must_use]
    pub fn body_length(&self, prefix: &[u8]) -> Option<u64> {
        let length = if self.wide {
            u64::from_le_bytes(*prefix.first_chunk::<8>()?)
        } else {
            u64::from(u32::from_le_bytes(*prefix.first_chunk::<4>()?))
        };
        (self.minimum_body..=self.maximum_body)
            .contains(&length)
            .then_some(length)
    }

    /// The length prefix of a `body`-byte frame, or `None` past this log's bounds.
    #[must_use]
    pub fn prefix(&self, body: u64) -> Option<([u8; 8], usize)> {
        if !(self.minimum_body..=self.maximum_body).contains(&body) {
            return None;
        }
        let mut prefix = [0; 8];
        if self.wide {
            prefix = body.to_le_bytes();
        } else {
            prefix[..4].copy_from_slice(&u32::try_from(body).ok()?.to_le_bytes());
        }
        Some((prefix, self.prefix_bytes()))
    }

    /// The checksum of a frame whose prefix and body are given.
    #[must_use]
    pub fn checksum(&self, prefix: &[u8], body: &[u8]) -> [u8; Self::CHECKSUM_BYTES] {
        let mut hasher = Sha256::new();
        hasher.update(self.domain);
        hasher.update(prefix);
        hasher.update(body);
        hasher.finalize().into()
    }

    /// Appends the frame whose body is `parts`, in order, to `out`.
    pub fn encode(&self, parts: &[&[u8]], out: &mut Vec<u8>) -> io::Result<()> {
        let body = parts.iter().map(|part| part.len() as u64).sum::<u64>();
        let (prefix, width) = self.prefix(body).ok_or(io::ErrorKind::InvalidInput)?;
        let start = out.len();
        out.reserve(usize::try_from(self.frame_bytes(body)).map_err(io::Error::other)?);
        out.extend_from_slice(&prefix[..width]);
        for part in parts {
            out.extend_from_slice(part);
        }
        let checksum = self.checksum(&out[start..start + width], &out[start + width..]);
        out.extend_from_slice(&checksum);
        Ok(())
    }

    /// Whether a whole frame with a matching checksum starts `bytes`.
    #[must_use]
    pub fn is_valid(&self, bytes: &[u8]) -> bool {
        let width = self.prefix_bytes();
        let Some(length) = bytes.get(..width).and_then(|prefix| self.body_length(prefix)) else {
            return false;
        };
        let Ok(length) = usize::try_from(length) else {
            return false;
        };
        let Some(body) = bytes.get(width..width + length) else {
            return false;
        };
        bytes
            .get(width + length..width + length + Self::CHECKSUM_BYTES)
            .is_some_and(|checksum| checksum == self.checksum(&bytes[..width], body))
    }

    /// Reads the frame at `reader`'s cursor, `remaining` bytes before the
    /// log's end: its body, or `None` when it is torn or damaged.
    pub fn read(&self, mut reader: impl io::Read, remaining: u64) -> io::Result<Option<Vec<u8>>> {
        let width = self.prefix_bytes();
        if remaining < width as u64 {
            return Ok(None);
        }
        let mut prefix = [0; 8];
        reader.read_exact(&mut prefix[..width])?;
        let Some(length) = self.body_length(&prefix[..width]) else {
            return Ok(None);
        };
        if remaining < self.frame_bytes(length) {
            return Ok(None);
        }
        let mut body = vec![0; usize::try_from(length).map_err(io::Error::other)?];
        reader.read_exact(&mut body)?;
        let mut checksum = [0; Self::CHECKSUM_BYTES];
        reader.read_exact(&mut checksum)?;
        Ok((self.checksum(&prefix[..width], &body) == checksum).then_some(body))
    }
}

#[cfg(test)]
mod tests {
    use super::LogFrame;

    const NARROW: LogFrame = LogFrame {
        domain: b"narrow\0",
        wide: false,
        minimum_body: 1,
        maximum_body: 64,
    };
    const WIDE: LogFrame = LogFrame {
        wide: true,
        maximum_body: u64::MAX - 64,
        ..NARROW
    };

    #[test]
    fn a_frame_is_valid_only_whole_and_unchanged() {
        for format in [NARROW, WIDE] {
            let mut frame = Vec::new();
            format.encode(&[b"ab", b"cde"], &mut frame).unwrap();
            assert_eq!(frame.len() as u64, format.frame_bytes(5));
            assert!(format.is_valid(&frame));
            for cut in 0..frame.len() {
                assert!(!format.is_valid(&frame[..cut]), "cut at {cut}");
            }
            for flipped in 0..frame.len() {
                let mut damaged = frame.clone();
                damaged[flipped] ^= 1;
                assert!(!format.is_valid(&damaged), "flip at {flipped}");
            }
        }
        // A body outside the log's bounds never encodes.
        assert!(NARROW.encode(&[&[0; 65]], &mut Vec::new()).is_err());
        assert!(NARROW.encode(&[], &mut Vec::new()).is_err());
    }
}
