//! Immutable bytes with bounded native ownership.
#[cfg(not(target_arch = "wasm32"))]
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation};
use bytes::Bytes;
use std::{
    borrow::Borrow,
    cmp::Ordering,
    hash::{Hash, Hasher},
    ops::{Bound, Deref, Range, RangeBounds},
};
#[cfg(not(target_arch = "wasm32"))]
use std::{io, sync::Arc};
#[cfg(not(target_arch = "wasm32"))]
mod prepared;
#[cfg(not(target_arch = "wasm32"))]
pub use prepared::{PreparedOwnedBytes, PreparedOwnedBytesError, PreparedOwnedBytesFailure};

#[derive(Clone)]
enum Data {
    Plain(Bytes),
    #[cfg(not(target_arch = "wasm32"))]
    Native(Arc<GuardBundle<dyn Send + Sync>>),
}
#[cfg(not(target_arch = "wasm32"))]
// Data and metadata retire before the original owner.
struct GuardBundle<G: ?Sized> {
    data: Vec<u8>,
    metadata: ReadAllocation,
    _original_guard: G,
}
#[cfg(not(target_arch = "wasm32"))]
impl<G: Send + Sync + 'static> GuardBundle<G> {
    fn new(
        data: &mut Vec<u8>,
        guard: &mut Option<G>,
        meter: &Arc<dyn NativeReadMeter>,
    ) -> io::Result<Arc<Self>> {
        if guard.is_none() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let metadata = ReadAllocation::new(meter, std::mem::size_of::<Self>())?;
        Ok(Arc::new(Self {
            data: std::mem::take(data),
            metadata,
            _original_guard: guard.take().expect("checked owner"),
        }))
    }
}

/// Immutable bytes retaining their original allocation.
#[derive(Clone)]
pub struct OwnedBytes {
    data: Data,
    range: Range<usize>,
    retained_capacity: Option<usize>,
}
impl OwnedBytes {
    /// Empty ordinary bytes.
    #[must_use]
    pub const fn new() -> Self {
        Self::from_static(b"")
    }
    /// Borrows static bytes.
    #[must_use]
    pub const fn from_static(bytes: &'static [u8]) -> Self {
        Self {
            data: Data::Plain(Bytes::from_static(bytes)),
            range: 0..bytes.len(),
            retained_capacity: None,
        }
    }
    /// Copies ordinary bytes.
    #[must_use]
    pub fn copy_from_slice(bytes: &[u8]) -> Self {
        Bytes::copy_from_slice(bytes).into()
    }
    /// Transfers a vector and its allocation owner.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_vec_with_guard<G: Send + Sync + 'static>(
        mut data: Vec<u8>,
        guard: G,
        meter: Arc<dyn NativeReadMeter>,
    ) -> Result<Self, PreparedOwnedBytesFailure<G>> {
        let mut guard = Some(guard);
        Self::from_vec_with_guard_in_place(&mut data, &mut guard, meter).map_err(|error| {
            PreparedOwnedBytesFailure {
                data,
                error: PreparedOwnedBytesError::Admission(error),
                original_guard: guard.expect("rejected transfer retains its owner"),
            }
        })
    }
    /// Leaves the vector and its owner untouched if admission fails.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_vec_with_guard_in_place<G: Send + Sync + 'static>(
        data: &mut Vec<u8>,
        guard: &mut Option<G>,
        meter: Arc<dyn NativeReadMeter>,
    ) -> io::Result<Self> {
        let (length, capacity) = (data.len(), data.capacity());
        let guard = GuardBundle::new(data, guard, &meter)?;
        Ok(Self {
            data: Data::Native(guard),
            range: 0..length,
            retained_capacity: Some(capacity),
        })
    }
    /// Borrows the visible bytes.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        let bytes = match &self.data {
            Data::Plain(bytes) => bytes.as_ref(),
            #[cfg(not(target_arch = "wasm32"))]
            Data::Native(guard) => guard.data.as_slice(),
        };
        &bytes[self.range.clone()]
    }
    /// Number of visible bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.range.end - self.range.start
    }
    /// Whether the visible range is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }
    /// Actual original vector or boxed-slice capacity, when known.
    #[must_use]
    pub const fn retained_capacity(&self) -> Option<usize> {
        self.retained_capacity
    }
    /// Recovers unique ordinary vector storage without copying.
    pub fn try_into_ordinary_vec(self) -> Result<Vec<u8>, Self> {
        match self {
            Self {
                data: Data::Plain(bytes),
                range,
                retained_capacity: Some(capacity),
            } if range.start == 0 && range.end == bytes.len() => {
                bytes.try_into_mut().map(Vec::from).map_err(|bytes| Self {
                    data: Data::Plain(bytes),
                    range,
                    retained_capacity: Some(capacity),
                })
            }
            other => Err(other),
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn is_native(&self) -> bool {
        matches!(self.data, Data::Native(_))
    }
    /// Compares the original allocation-owner identity.
    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    pub fn matches_meter(&self, meter: &Arc<dyn NativeReadMeter>) -> bool {
        match &self.data {
            Data::Native(guard) => Arc::ptr_eq(
                guard.metadata.meter(),
                &meter
                    .allocation_owner()
                    .unwrap_or_else(|| Arc::clone(meter)),
            ),
            Data::Plain(_) => false,
        }
    }
    /// Shares a checked range with its original allocation owner.
    #[must_use]
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        if self.retained_capacity.is_none()
            && let Data::Plain(bytes) = &self.data
        {
            return bytes.slice(range).into();
        }
        let start = match range.start_bound() {
            Bound::Included(&start) => start,
            Bound::Excluded(&start) => start.checked_add(1).expect("slice start overflow"),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&end) => end.checked_add(1).expect("slice end overflow"),
            Bound::Excluded(&end) => end,
            Bound::Unbounded => self.len(),
        };
        assert!(
            start <= end && end <= self.len(),
            "slice range exceeds visible bytes"
        );
        Self {
            data: self.data.clone(),
            range: self.range.start + start..self.range.start + end,
            retained_capacity: self.retained_capacity,
        }
    }
}
impl Default for OwnedBytes {
    fn default() -> Self {
        Self::new()
    }
}
impl From<Bytes> for OwnedBytes {
    fn from(bytes: Bytes) -> Self {
        let end = bytes.len();
        Self {
            data: Data::Plain(bytes),
            range: 0..end,
            retained_capacity: None,
        }
    }
}
impl From<Vec<u8>> for OwnedBytes {
    fn from(bytes: Vec<u8>) -> Self {
        let (end, capacity) = (bytes.len(), bytes.capacity());
        Self {
            data: Data::Plain(bytes.into()),
            range: 0..end,
            retained_capacity: Some(capacity),
        }
    }
}
impl From<Box<[u8]>> for OwnedBytes {
    fn from(bytes: Box<[u8]>) -> Self {
        let end = bytes.len();
        Self {
            data: Data::Plain(bytes.into()),
            range: 0..end,
            retained_capacity: Some(end),
        }
    }
}
impl From<String> for OwnedBytes {
    fn from(bytes: String) -> Self {
        bytes.into_bytes().into()
    }
}
impl From<&[u8]> for OwnedBytes {
    fn from(bytes: &[u8]) -> Self {
        Self::copy_from_slice(bytes)
    }
}
impl From<&str> for OwnedBytes {
    fn from(bytes: &str) -> Self {
        Self::copy_from_slice(bytes.as_bytes())
    }
}
impl FromIterator<u8> for OwnedBytes {
    fn from_iter<T: IntoIterator<Item = u8>>(iter: T) -> Self {
        Vec::from_iter(iter).into()
    }
}
impl Deref for OwnedBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}
impl AsRef<[u8]> for OwnedBytes {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
impl Borrow<[u8]> for OwnedBytes {
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}
impl PartialEq for OwnedBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}
impl Eq for OwnedBytes {}
impl PartialOrd for OwnedBytes {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OwnedBytes {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}
impl Hash for OwnedBytes {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}
impl std::fmt::Debug for OwnedBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_slice().fmt(f)
    }
}
#[cfg(feature = "serde")]
impl serde::Serialize for OwnedBytes {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(self.as_slice(), serializer)
    }
}
#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for OwnedBytes {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <Vec<u8> as serde::Deserialize<'de>>::deserialize(deserializer).map(Self::from)
    }
}
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
