use crate::{Attribute, OwnedBytes, StreamError};
use std::sync::Arc;

/// Maximum indexed keys in one record.
pub const MAX_RECORD_KEYS: usize = 64;
/// Maximum bytes in one indexed key.
pub const MAX_RECORD_KEY_BYTES: usize = 1024;
/// Maximum single-valued attributes in one record.
pub const MAX_RECORD_ATTRIBUTES: usize = 16;

struct HeaderData {
    keys: Vec<OwnedBytes>,
    attributes: Vec<Attribute>,
    #[cfg(not(target_arch = "wasm32"))]
    _allocation: Option<acyclic_native_runtime::ReadAllocation>,
}

/// Immutable canonical record headers. Clones retain the original allocation
/// owner through reads, replay, forks and retained commit envelopes.
#[derive(Clone, Default)]
pub struct RecordHeaders(Option<Arc<HeaderData>>);

impl RecordHeaders {
    /// Allocation claimed before constructing nonempty header control storage.
    /// Providers additionally fund the supplied vectors, names and byte owners.
    #[must_use]
    pub const fn control_bytes() -> usize {
        std::mem::size_of::<HeaderData>() + 2 * std::mem::size_of::<usize>()
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn from_native_parts(
        keys: Vec<OwnedBytes>,
        attributes: Vec<Attribute>,
        mut allocation: acyclic_native_runtime::ReadAllocation,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<Self> {
        let mut control = acyclic_native_runtime::ReadAllocation::new(
            meter,
            std::mem::size_of::<HeaderData>() + 2 * std::mem::size_of::<usize>(),
        )?;
        allocation.absorb(&mut control)?;
        Ok(Self(Some(Arc::new(HeaderData {
            keys,
            attributes,
            _allocation: Some(allocation),
        }))))
    }

    /// Admit strictly sorted unique keys and attribute names. Producers supply
    /// canonical order; the provider never silently changes retry identities.
    pub fn new(keys: Vec<OwnedBytes>, attributes: Vec<Attribute>) -> Result<Self, StreamError> {
        if keys.len() > MAX_RECORD_KEYS
            || attributes.len() > MAX_RECORD_ATTRIBUTES
            || keys.iter().any(|key| key.len() > MAX_RECORD_KEY_BYTES)
        {
            return Err(StreamError::LimitExceeded);
        }
        if keys
            .windows(2)
            .any(|pair| matches!(pair, [left, right] if left >= right))
            || attributes
                .windows(2)
                .any(|pair| matches!(pair, [left, right] if left.name >= right.name))
            || attributes.iter().any(|attribute| {
                attribute.name.is_empty() || attribute.name.chars().any(char::is_control)
            })
        {
            return Err(StreamError::InvalidArgument);
        }
        if keys.is_empty() && attributes.is_empty() {
            return Ok(Self::default());
        }
        let headers = Self(Some(Arc::new(HeaderData {
            keys,
            attributes,
            #[cfg(not(target_arch = "wasm32"))]
            _allocation: None,
        })));
        if headers.payload_bytes() > crate::MAX_RECORD_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        Ok(headers)
    }

    /// Canonically sorted unique keys.
    pub fn keys(&self) -> &[OwnedBytes] {
        self.0.as_ref().map_or(&[], |data| &data.keys)
    }

    /// Canonically sorted single-valued attributes.
    pub fn attributes(&self) -> &[Attribute] {
        self.0.as_ref().map_or(&[], |data| &data.attributes)
    }

    /// Bytes counted against the record's shared value-and-header bound.
    pub fn payload_bytes(&self) -> usize {
        self.keys()
            .iter()
            .fold(0usize, |total, key| total.saturating_add(key.len()))
            .saturating_add(self.attributes().iter().fold(0usize, |total, attribute| {
                total
                    .saturating_add(attribute.name.len())
                    .saturating_add(attribute.value.len())
            }))
    }

    pub(crate) fn hash_bytes(&self) -> u64 {
        16 + 8 * self.keys().len() as u64
            + 16 * self.attributes().len() as u64
            + self.payload_bytes() as u64
    }

    /// Memory retained by unmetered ingress, excluding shared byte owners.
    pub(crate) fn owned_bytes(&self) -> usize {
        self.0.as_ref().map_or(0, |data| {
            std::mem::size_of::<HeaderData>()
                + 2 * std::mem::size_of::<usize>()
                + data.keys.capacity() * std::mem::size_of::<OwnedBytes>()
                + data.attributes.capacity() * std::mem::size_of::<Attribute>()
                + data
                    .attributes
                    .iter()
                    .map(|attribute| attribute.name.capacity())
                    .sum::<usize>()
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn bind_native(
        &self,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<Self> {
        use acyclic_native_runtime::ReadAllocation;
        meter.check()?;
        let Some(data) = &self.0 else {
            return Ok(self.clone());
        };
        if data._allocation.is_some() {
            return Ok(self.clone());
        }
        let copied = data.keys.len() * std::mem::size_of::<OwnedBytes>()
            + data.attributes.len() * std::mem::size_of::<Attribute>()
            + data
                .attributes
                .iter()
                .map(|attribute| attribute.name.len())
                .sum::<usize>();
        meter.admit_codec(0, copied as u64, 0)?;
        let mut allocation = ReadAllocation::new(
            meter,
            std::mem::size_of::<HeaderData>() + 2 * std::mem::size_of::<usize>(),
        )?;
        let mut keys = crate::native::vector(data.keys.len(), meter, &mut allocation)?;
        keys.extend(data.keys.iter().cloned());
        let mut attributes = crate::native::vector(data.attributes.len(), meter, &mut allocation)?;
        for attribute in &data.attributes {
            let mut name = crate::native::vector(attribute.name.len(), meter, &mut allocation)?;
            name.extend_from_slice(attribute.name.as_bytes());
            attributes.push(Attribute {
                name: String::from_utf8(name).map_err(|_| std::io::ErrorKind::InvalidData)?,
                value: attribute.value.clone(),
            });
        }
        let headers = Self(Some(Arc::new(HeaderData {
            keys,
            attributes,
            _allocation: Some(allocation),
        })));
        meter.record_codec(0, copied as u64, 0)?;
        meter.check()?;
        Ok(headers)
    }
}

impl PartialEq for RecordHeaders {
    fn eq(&self, other: &Self) -> bool {
        self.keys() == other.keys() && self.attributes() == other.attributes()
    }
}
impl Eq for RecordHeaders {}
impl std::fmt::Debug for RecordHeaders {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RecordHeaders")
            .field("keys", &self.keys())
            .field("attributes", &self.attributes())
            .finish()
    }
}

impl crate::AppendRecord {
    /// Value and header bytes counted together against `MAX_RECORD_BYTES`.
    pub fn payload_bytes(&self) -> usize {
        self.value
            .len()
            .saturating_add(self.headers.payload_bytes())
    }

    /// Exact bytes fed into the canonical request digest for this record.
    pub fn hash_bytes(&self) -> u64 {
        8 + self.value.len() as u64 + self.headers.hash_bytes()
    }
}

impl crate::Record {
    /// Shared value and indexed-header payload bound used by every reader.
    pub fn payload_bytes(&self) -> usize {
        self.value
            .len()
            .saturating_add(self.headers.payload_bytes())
    }
}

#[cfg(test)]
#[path = "record_header_tests.rs"]
mod tests;
