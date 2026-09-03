//! Small fail-closed codec used by every hash-bearing filesystem object.

use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DecodedPageKind {
    Leaf,
    Internal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DecodedPageShape {
    pub(crate) kind: DecodedPageKind,
    pub(crate) items: usize,
    pub(crate) nested_bytes: u64,
}

/// Allocation and collection bounds for one canonical object decode.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DecodeLimits {
    /// Maximum complete canonical object bytes.
    pub maximum_object_bytes: u64,
    /// Maximum bytes in one logical name.
    pub maximum_name_bytes: u32,
    /// Maximum entries or children in one page.
    pub maximum_page_items: u32,
    /// Maximum canonical bytes in one authenticated index/page object.
    pub maximum_page_bytes: u32,
    /// Maximum authenticated page levels followed by one operation.
    pub maximum_page_height: u16,
    /// Maximum distinct authenticated pages retained for cycle/alias checks.
    pub maximum_visited_pages: u32,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            maximum_object_bytes: 64 * 1024 * 1024,
            maximum_name_bytes: 255,
            maximum_page_items: 1_024,
            maximum_page_bytes: 256 * 1024,
            maximum_page_height: 64,
            maximum_visited_pages: 4_096,
        }
    }
}

impl DecodeLimits {
    pub(crate) fn maximum_page_object_bytes(self) -> u64 {
        self.maximum_object_bytes
            .min(u64::from(self.maximum_page_bytes))
    }

    pub(crate) fn page_limits_valid(self, minimum_items: u32) -> bool {
        self.maximum_page_items >= minimum_items
            && self.maximum_page_bytes != 0
            && self.maximum_page_height != 0
            && self.maximum_visited_pages != 0
    }
}

pub(crate) struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    pub(crate) fn new(domain: &[u8], version: u16) -> Self {
        let mut bytes = Vec::with_capacity(domain.len() + 2);
        bytes.extend_from_slice(domain);
        bytes.extend_from_slice(&version.to_le_bytes());
        Self { bytes }
    }

    pub(crate) fn with_exact_capacity(
        domain: &[u8],
        version: u16,
        capacity: usize,
    ) -> Result<Self, CanonicalDecodeError> {
        let minimum = domain
            .len()
            .checked_add(2)
            .ok_or(CanonicalDecodeError::LengthOverflow)?;
        if capacity < minimum {
            return Err(CanonicalDecodeError::LengthOverflow);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(|_| CanonicalDecodeError::AllocationFailed)?;
        bytes.extend_from_slice(domain);
        bytes.extend_from_slice(&version.to_le_bytes());
        Ok(Self { bytes })
    }

    pub(crate) fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub(crate) fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn fixed(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    pub(crate) fn bounded_bytes(&mut self, value: &[u8]) -> Result<(), CanonicalDecodeError> {
        let length =
            u32::try_from(value.len()).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
        self.u32(length);
        self.fixed(value);
        Ok(())
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

pub(crate) struct Decoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Decoder<'a> {
    pub(crate) fn new(
        bytes: &'a [u8],
        domain: &[u8],
        expected_version: u16,
        maximum_object_bytes: u64,
    ) -> Result<Self, CanonicalDecodeError> {
        let observed = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if observed > maximum_object_bytes {
            return Err(CanonicalDecodeError::ObjectTooLarge {
                observed,
                maximum: maximum_object_bytes,
            });
        }
        let header_length = domain
            .len()
            .checked_add(2)
            .ok_or(CanonicalDecodeError::LengthOverflow)?;
        if bytes.len() < header_length || &bytes[..domain.len()] != domain {
            return Err(CanonicalDecodeError::WrongDomain);
        }
        let version_bytes: [u8; 2] = bytes[domain.len()..header_length]
            .try_into()
            .map_err(|_| CanonicalDecodeError::Truncated)?;
        let version = u16::from_le_bytes(version_bytes);
        if version != expected_version {
            return Err(CanonicalDecodeError::UnsupportedVersion(version));
        }
        Ok(Self {
            bytes,
            cursor: header_length,
        })
    }

    pub(crate) fn u8(&mut self) -> Result<u8, CanonicalDecodeError> {
        let value = *self
            .bytes
            .get(self.cursor)
            .ok_or(CanonicalDecodeError::Truncated)?;
        self.cursor = self
            .cursor
            .checked_add(1)
            .ok_or(CanonicalDecodeError::LengthOverflow)?;
        Ok(value)
    }

    pub(crate) fn u32(&mut self) -> Result<u32, CanonicalDecodeError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| CanonicalDecodeError::Truncated)?;
        Ok(u32::from_le_bytes(bytes))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, CanonicalDecodeError> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| CanonicalDecodeError::Truncated)?;
        Ok(u64::from_le_bytes(bytes))
    }

    pub(crate) fn i64(&mut self) -> Result<i64, CanonicalDecodeError> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| CanonicalDecodeError::Truncated)?;
        Ok(i64::from_le_bytes(bytes))
    }

    pub(crate) fn fixed<const N: usize>(&mut self) -> Result<[u8; N], CanonicalDecodeError> {
        self.take(N)?
            .try_into()
            .map_err(|_| CanonicalDecodeError::Truncated)
    }

    pub(crate) fn bounded_bytes(&mut self, maximum: u32) -> Result<Vec<u8>, CanonicalDecodeError> {
        let length = self.u32()?;
        if length > maximum {
            return Err(CanonicalDecodeError::FieldTooLarge {
                observed: length,
                maximum,
            });
        }
        let length = usize::try_from(length).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
        let source = self.take(length)?;
        let mut value = Vec::new();
        value
            .try_reserve_exact(length)
            .map_err(|_| CanonicalDecodeError::AllocationFailed)?;
        value.extend_from_slice(source);
        Ok(value)
    }

    pub(crate) fn skip_bounded_bytes(
        &mut self,
        maximum: u32,
    ) -> Result<usize, CanonicalDecodeError> {
        let length = self.u32()?;
        if length > maximum {
            return Err(CanonicalDecodeError::FieldTooLarge {
                observed: length,
                maximum,
            });
        }
        let length = usize::try_from(length).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
        self.take(length)?;
        Ok(length)
    }

    pub(crate) fn take_exact(&mut self, length: usize) -> Result<&'a [u8], CanonicalDecodeError> {
        self.take(length)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], CanonicalDecodeError> {
        let end = self
            .cursor
            .checked_add(length)
            .ok_or(CanonicalDecodeError::LengthOverflow)?;
        let value = self
            .bytes
            .get(self.cursor..end)
            .ok_or(CanonicalDecodeError::Truncated)?;
        self.cursor = end;
        Ok(value)
    }

    pub(crate) fn finish(self) -> Result<(), CanonicalDecodeError> {
        if self.cursor == self.bytes.len() {
            Ok(())
        } else {
            Err(CanonicalDecodeError::TrailingBytes)
        }
    }
}

/// Fail-closed canonical object decoding failures.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum CanonicalDecodeError {
    /// Object belongs to another canonical domain.
    #[error("canonical object domain is invalid")]
    WrongDomain,
    /// Object format version is unsupported.
    #[error("unsupported canonical object version {0}")]
    UnsupportedVersion(u16),
    /// Object ended before a declared field was complete.
    #[error("canonical object is truncated")]
    Truncated,
    /// Complete object exceeds the admitted allocation/work bound.
    #[error("canonical object has {observed} bytes; maximum is {maximum}")]
    ObjectTooLarge {
        /// Observed bytes.
        observed: u64,
        /// Admitted maximum.
        maximum: u64,
    },
    /// One bounded field exceeds its admitted bytes.
    #[error("canonical field has {observed} bytes; maximum is {maximum}")]
    FieldTooLarge {
        /// Observed bytes or items.
        observed: u32,
        /// Admitted maximum.
        maximum: u32,
    },
    /// Length arithmetic cannot be represented safely.
    #[error("canonical object length overflowed")]
    LengthOverflow,
    /// A bounded canonical encoding or decoding allocation failed.
    #[error("canonical codec allocation failed")]
    AllocationFailed,
    /// Canonical object contains bytes after its final field.
    #[error("canonical object has trailing bytes")]
    TrailingBytes,
    /// A field tag is not recognized by this version.
    #[error("unknown {field} tag {tag}")]
    UnknownTag {
        /// Stable field description.
        field: &'static str,
        /// Unknown tag.
        tag: u8,
    },
    /// Decoded fields violate a semantic page invariant.
    #[error("canonical object invariant failed: {0}")]
    Invariant(String),
}

#[cfg(test)]
#[path = "tests/codec.rs"]
mod tests;
