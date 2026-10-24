use crate::{Attribute, OwnedBytes, StreamError};
use std::sync::Arc;

mod attributes;
mod validation;
use attributes::AttributeStorage;
pub use attributes::{RecordAttribute, RecordAttributeIter, RecordAttributes};
#[cfg(not(target_arch = "wasm32"))]
mod prepared;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use prepared::{PreparedHeaderControls, PreparedHeaders};

/// Maximum indexed keys in one record.
pub const MAX_RECORD_KEYS: usize = 64;
/// Maximum bytes in one indexed key.
pub const MAX_RECORD_KEY_BYTES: usize = 1024;
/// Maximum single-valued attributes in one record.
pub const MAX_RECORD_ATTRIBUTES: usize = 16;

struct HeaderData {
    keys: Vec<OwnedBytes>,
    attributes: AttributeStorage,
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
        #[cfg(not(target_arch = "wasm32"))]
        {
            acyclic_native_runtime::arc_allocation_bytes::<HeaderData>()
        }
        #[cfg(target_arch = "wasm32")]
        {
            std::mem::size_of::<HeaderData>() + 2 * std::mem::size_of::<usize>()
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn from_native_parts(
        keys: Vec<OwnedBytes>,
        attributes: Vec<Attribute>,
        allocation: acyclic_native_runtime::ReadAllocation,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<Result<Self, StreamError>> {
        // A fallible control-header admission must not release the vector's
        // funding before destroying the values in that vector.
        if let Err(error) = Self::validate_parts(&keys, &attributes) {
            drop(keys);
            drop(attributes);
            return Ok(Err(error));
        }
        let mut data = HeaderData {
            keys,
            attributes: AttributeStorage::Owned(attributes),
            _allocation: Some(allocation),
        };
        let mut control =
            acyclic_native_runtime::ReadAllocation::new(meter, Self::control_bytes())?;
        data._allocation
            .as_mut()
            .ok_or(std::io::ErrorKind::InvalidInput)?
            .absorb(&mut control)?;
        Ok(Ok(Self(Some(Arc::new(data)))))
    }

    /// Admit strictly sorted unique keys and attribute names. Producers supply
    /// canonical order; the provider never silently changes retry identities.
    pub fn new(keys: Vec<OwnedBytes>, attributes: Vec<Attribute>) -> Result<Self, StreamError> {
        Self::validate_parts(&keys, &attributes)?;
        if keys.is_empty() && attributes.is_empty() {
            return Ok(Self::default());
        }
        Ok(Self(Some(Arc::new(HeaderData {
            keys,
            attributes: AttributeStorage::Owned(attributes),
            #[cfg(not(target_arch = "wasm32"))]
            _allocation: None,
        }))))
    }

    fn validate_parts(keys: &[OwnedBytes], attributes: &[Attribute]) -> Result<(), StreamError> {
        let mut validation = validation::Validation::default();
        for key in keys {
            validation.key(key)?;
        }
        for attribute in attributes {
            validation.attribute(&attribute.name, &attribute.value)?;
        }
        validation.finish(crate::MAX_RECORD_BYTES)
    }

    /// Canonically sorted unique keys.
    pub fn keys(&self) -> &[OwnedBytes] {
        self.0.as_ref().map_or(&[], |data| &data.keys)
    }

    /// Canonically sorted single-valued attributes.
    pub fn attributes(&self) -> RecordAttributes<'_> {
        RecordAttributes(self.0.as_ref().map(|data| &data.attributes))
    }

    /// Bytes counted against the record's shared value-and-header bound.
    pub fn payload_bytes(&self) -> usize {
        self.keys().iter().map(OwnedBytes::len).sum::<usize>()
            + self
                .attributes()
                .iter()
                .map(|attribute| attribute.name.len() + attribute.value.len())
                .sum::<usize>()
    }

    pub(crate) fn hash_bytes(&self) -> u64 {
        16 + 8 * self.keys().len() as u64
            + 16 * self.attributes().len() as u64
            + self.payload_bytes() as u64
    }

    /// Memory retained by unmetered ingress, excluding shared byte owners.
    pub(crate) fn owned_bytes(&self) -> usize {
        self.0.as_ref().map_or(0, |data| {
            Self::control_bytes()
                + data.keys.capacity() * std::mem::size_of::<OwnedBytes>()
                + data.attributes.control_bytes()
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn bind_native(
        &self,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<Self> {
        let Some(data) = &self.0 else {
            return Ok(self.clone());
        };
        if data
            ._allocation
            .as_ref()
            .is_some_and(|allocation| Arc::ptr_eq(allocation.meter(), meter))
        {
            return Ok(self.clone());
        }
        let headers = PreparedHeaders::new_shape(
            meter,
            self.payload_bytes(),
            data.keys.len(),
            data.attributes.len(),
        )?
        .finish(self)?;
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod native_tests {
    use super::*;
    use crate::test_meter::Meter;
    use std::sync::atomic::Ordering;

    #[test]
    fn native_header_control_funds_its_complete_shared_allocation()
    -> Result<(), Box<dyn std::error::Error>> {
        let owner = Arc::new(Meter::default());
        let meter = owner.original();
        let keys = vec![OwnedBytes::from_static(b"key")];
        let vector_bytes = keys.capacity() * std::mem::size_of::<OwnedBytes>();
        let allocation = acyclic_native_runtime::ReadAllocation::new(&meter, vector_bytes)?;
        let baseline = owner.held.load(Ordering::SeqCst);
        let mut headers = None;
        let created = acyclic_test_support::measure(|| {
            headers = Some(RecordHeaders::from_native_parts(
                keys,
                vec![],
                allocation,
                &meter,
            ));
        });
        let headers = headers.ok_or("header construction was not called")???;
        assert_eq!(created.count_current, 1);
        assert_eq!(
            owner.held.load(Ordering::SeqCst) - baseline,
            u64::try_from(created.bytes_current)?,
            "native header control omitted Arc counters"
        );
        assert_eq!(
            headers.owned_bytes() as u64,
            owner.held.load(Ordering::SeqCst)
        );
        drop(headers);
        owner.assert_refunded();
        Ok(())
    }

    #[test]
    fn prepared_headers_retain_only_their_original_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        let owner = Arc::new(Meter::default());
        let original = owner.original();
        let headers = RecordHeaders::new(vec![OwnedBytes::from_static(b"key")], vec![])?
            .bind_native(&original)?;
        owner.cancelled.store(true, Ordering::SeqCst);
        owner.deny_allocation.store(true, Ordering::SeqCst);
        assert_eq!(headers.bind_native(&original)?, headers);
        assert_eq!(
            RecordHeaders::default().bind_native(&original)?,
            RecordHeaders::default()
        );
        let foreign = Arc::new(Meter::default());
        foreign.deny_allocation.store(true, Ordering::SeqCst);
        assert!(headers.bind_native(&foreign.original()).is_err());
        Ok(())
    }
}
