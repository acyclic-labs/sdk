use sha2::{Digest as _, Sha256};
use std::io;

#[path = "frame_layout.rs"]
mod layout;
pub use layout::LogFrame;

impl LogFrame {
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
        self.checksum_parts(prefix, &[body])
    }

    fn checksum_parts(&self, prefix: &[u8], body: &[&[u8]]) -> [u8; Self::CHECKSUM_BYTES] {
        let mut hasher = Sha256::new();
        hasher.update(self.domain);
        hasher.update(prefix);
        for part in body {
            hasher.update(part);
        }
        hasher.finalize().into()
    }

    /// Appends the frame whose body is `parts`, in order, to `out`.
    pub fn encode(&self, parts: &[&[u8]], out: &mut Vec<u8>) -> io::Result<()> {
        let body = parts
            .iter()
            .try_fold(0_u64, |total, part| total.checked_add(part.len() as u64))
            .ok_or(io::ErrorKind::InvalidInput)?;
        let (prefix, width) = self.prefix(body).ok_or(io::ErrorKind::InvalidInput)?;
        let prefix = prefix.get(..width).ok_or(io::ErrorKind::InvalidInput)?;
        let bytes = usize::try_from(self.frame_bytes(body).ok_or(io::ErrorKind::InvalidInput)?)
            .map_err(|_| io::ErrorKind::InvalidInput)?;
        let end = out
            .len()
            .checked_add(bytes)
            .ok_or(io::ErrorKind::InvalidInput)?;
        if end > isize::MAX as usize {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        out.try_reserve(bytes).map_err(io::Error::other)?;
        out.extend_from_slice(prefix);
        for part in parts {
            out.extend_from_slice(part);
        }
        out.extend_from_slice(&self.checksum_parts(prefix, parts));
        Ok(())
    }

    /// Seals a body already encoded in `out` after a reserved length prefix.
    /// Earlier frames in the buffer remain intact.
    pub fn seal(&self, out: &mut Vec<u8>, start: usize) -> io::Result<()> {
        let width = self.prefix_bytes();
        let body_start = start
            .checked_add(width)
            .ok_or(io::ErrorKind::InvalidInput)?;
        let tail = out.get(start..).ok_or(io::ErrorKind::InvalidInput)?;
        let length = tail
            .len()
            .checked_sub(width)
            .ok_or(io::ErrorKind::InvalidInput)?;
        let (prefix, _) = self
            .prefix(length as u64)
            .ok_or(io::ErrorKind::InvalidInput)?;
        let prefix = prefix.get(..width).ok_or(io::ErrorKind::InvalidInput)?;
        let body = out.get(body_start..).ok_or(io::ErrorKind::InvalidInput)?;
        let checksum = self.checksum(prefix, body);
        out.try_reserve(Self::CHECKSUM_BYTES)
            .map_err(io::Error::other)?;
        out.get_mut(start..body_start)
            .ok_or(io::ErrorKind::InvalidInput)?
            .copy_from_slice(prefix);
        out.extend_from_slice(&checksum);
        Ok(())
    }

    /// Splits off one whole checksummed frame, returning its body and the
    /// remaining log bytes. A torn or damaged prefix returns `None`.
    #[must_use]
    pub fn decode_prefix<'a>(&self, bytes: &'a [u8]) -> Option<(&'a [u8], &'a [u8])> {
        let width = self.prefix_bytes();
        let (prefix, tail) = bytes.split_at_checked(width)?;
        let length = self.body_length(prefix)?;
        let extent = usize::try_from(self.frame_bytes(length)?).ok()?;
        let (encoded, rest) = tail.split_at_checked(extent.checked_sub(width)?)?;
        let (body, checksum) = encoded.split_at_checked(usize::try_from(length).ok()?)?;
        (checksum == self.checksum(prefix, body)).then_some((body, rest))
    }

    /// Whether a whole frame with a matching checksum starts `bytes`.
    #[must_use]
    pub fn is_valid(&self, bytes: &[u8]) -> bool {
        self.decode_prefix(bytes).is_some()
    }

    /// Reads the frame at `reader`'s cursor, `remaining` bytes before the
    /// log's end: its body, or `None` when it is torn or damaged.
    pub fn read(&self, mut reader: impl io::Read, remaining: u64) -> io::Result<Option<Vec<u8>>> {
        let width = self.prefix_bytes();
        if remaining < width as u64 {
            return Ok(None);
        }
        let mut prefix = [0; 8];
        let prefix = prefix.get_mut(..width).ok_or(io::ErrorKind::InvalidInput)?;
        reader.read_exact(prefix)?;
        let Some(length) = self.body_length(prefix) else {
            return Ok(None);
        };
        let bytes = self.frame_bytes(length).ok_or(io::ErrorKind::InvalidData)?;
        if remaining < bytes {
            return Ok(None);
        }
        let length = usize::try_from(length).map_err(|_| io::ErrorKind::InvalidData)?;
        if length > isize::MAX as usize {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let mut body = Vec::new();
        body.try_reserve_exact(length).map_err(io::Error::other)?;
        body.resize(length, 0);
        reader.read_exact(&mut body)?;
        let mut checksum = [0; Self::CHECKSUM_BYTES];
        reader.read_exact(&mut checksum)?;
        Ok((self.checksum(prefix, &body) == checksum).then_some(body))
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
            assert_eq!(Some(frame.len() as u64), format.frame_bytes(5));
            assert!(format.is_valid(&frame));
            let mut batch = frame.clone();
            batch.resize(batch.len() + format.prefix_bytes(), 0);
            batch.extend_from_slice(b"next");
            format.seal(&mut batch, frame.len()).unwrap();
            let (body, rest) = format.decode_prefix(&batch).unwrap();
            assert_eq!(body, b"abcde");
            assert_eq!(
                format.decode_prefix(rest),
                Some((b"next".as_slice(), &[][..]))
            );
            assert_eq!(
                format.read(frame.as_slice(), frame.len() as u64).unwrap(),
                Some(b"abcde".to_vec())
            );
            for cut in 0..frame.len() {
                assert!(!format.is_valid(frame.get(..cut).unwrap()), "cut at {cut}");
            }
            for flipped in 0..frame.len() {
                let mut damaged = frame.clone();
                *damaged.get_mut(flipped).unwrap() ^= 1;
                assert!(!format.is_valid(&damaged), "flip at {flipped}");
            }
        }
        // A body outside the log's bounds never encodes.
        assert!(NARROW.encode(&[&[0; 65]], &mut Vec::new()).is_err());
        assert!(NARROW.encode(&[], &mut Vec::new()).is_err());

        // Corrupt the length of a frame from the shipping encoder. Even a
        // declared body that fits usize may overflow when framing is added.
        let format = LogFrame {
            maximum_body: u64::MAX,
            ..WIDE
        };
        let mut frame = Vec::new();
        format.encode(&[b"abcde"], &mut frame).unwrap();
        for length in [u64::from(u32::MAX), usize::MAX as u64, u64::MAX] {
            frame
                .get_mut(..8)
                .unwrap()
                .copy_from_slice(&length.to_le_bytes());
            assert!(!format.is_valid(&frame), "declared {length}");
            let read = format.read(frame.as_slice(), frame.len() as u64);
            if format.frame_bytes(length).is_none() {
                assert_eq!(read.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
            } else {
                assert!(read.unwrap().is_none());
            }
            if length == u64::MAX {
                assert_eq!(
                    format.read(frame.as_slice(), u64::MAX).unwrap_err().kind(),
                    std::io::ErrorKind::InvalidData
                );
            }
        }
    }

    #[test]
    fn impossible_extents_refuse_before_body_io_or_output_allocation() {
        use std::io::{Cursor, Read as _};

        let format = LogFrame {
            maximum_body: u64::MAX,
            ..WIDE
        };
        let overhead = format.prefix_bytes() as u64 + LogFrame::CHECKSUM_BYTES as u64;
        let last = usize::MAX as u64 - overhead;
        assert_eq!(format.frame_bytes(last), Some(usize::MAX as u64));
        assert_eq!(format.frame_bytes(last + 1), None);

        // A real prefix writer can represent a declaration whose full extent
        // cannot fit. Admission must stop before requesting any body bytes.
        let (prefix, width) = format.prefix(u64::MAX).unwrap();
        let mut prefix = Cursor::new(prefix.get(..width).unwrap());
        let mut body = Cursor::new(b"unread body".as_slice());
        let error = format
            .read((&mut prefix).chain(&mut body), u64::MAX)
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(prefix.position(), width as u64);
        assert_eq!(body.position(), 0);
        assert!(!format.is_valid(&u64::MAX.to_le_bytes()));

        let mut output = Vec::from(b"retained".as_slice());
        let capacity = output.capacity();
        assert_eq!(
            NARROW.encode(&[&[0; 65]], &mut output).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
        assert_eq!(output, b"retained");
        assert_eq!(output.capacity(), capacity);
    }
}
