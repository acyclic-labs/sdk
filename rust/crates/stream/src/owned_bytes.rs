//! Immutable bytes with original allocation custody.

#[cfg(not(target_arch = "wasm32"))]
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation};
use bytes::Bytes;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
#[cfg(not(target_arch = "wasm32"))]
use std::io;
use std::ops::{Bound, Deref, RangeBounds};
use std::sync::Arc;

trait AllocationCustody: AsRef<[u8]> + Send + Sync {
    #[cfg(not(target_arch = "wasm32"))]
    fn metadata_meter(&self) -> &Arc<dyn NativeReadMeter>;
}

// Original data custody retires last, after the SDK-owned metadata claim.
#[cfg(not(target_arch = "wasm32"))]
struct GuardBundle<G> {
    data: Vec<u8>,
    _metadata: ReadAllocation,
    _original_guard: G,
}
#[cfg(not(target_arch = "wasm32"))]
impl<G: Send + Sync> AllocationCustody for GuardBundle<G> {
    fn metadata_meter(&self) -> &Arc<dyn NativeReadMeter> { self._metadata.meter() }
}

#[cfg(not(target_arch = "wasm32"))]
impl<G> AsRef<[u8]> for GuardBundle<G> {
    fn as_ref(&self) -> &[u8] { &self.data }
}

/// Ordinary or original-allocation-guarded immutable bytes.
#[derive(Clone)]
pub struct OwnedBytes {
    bytes: Bytes,
    start: usize,
    end: usize,
    retained_capacity: Option<usize>,
    guard: Option<Arc<dyn AllocationCustody>>,
}

impl OwnedBytes {
    /// Returns an empty ordinary byte carrier without allocating.
    #[must_use]
    pub const fn new() -> Self { Self { bytes: Bytes::new(), start: 0, end: 0, retained_capacity: None, guard: None } }

    /// Wraps immutable static bytes without allocating.
    #[must_use]
    pub const fn from_static(bytes: &'static [u8]) -> Self {
        Self { bytes: Bytes::from_static(bytes), start: 0, end: bytes.len(), retained_capacity: None, guard: None }
    }

    /// Copies ordinary bytes using their ordinary allocation semantics.
    #[must_use]
    pub fn copy_from_slice(bytes: &[u8]) -> Self { Bytes::copy_from_slice(bytes).into() }

    /// Transfers immutable owned vector storage and its original capacity guard.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_vec_with_guard<G: Send + Sync + 'static>(
        mut data: Vec<u8>, original_guard: G, meter: Arc<dyn NativeReadMeter>,
    ) -> Result<Self, PreparedOwnedBytesFailure<G>> {
        let mut original_guard = Some(original_guard);
        Self::from_vec_with_guard_in_place(&mut data, &mut original_guard, meter)
            .map_err(|error| PreparedOwnedBytesFailure {
                data, error: PreparedOwnedBytesError::Admission(error), original_guard,
            })
    }

    /// Promotes the same vector without moving it or its guard on rejection.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_vec_with_guard_in_place<G: Send + Sync + 'static>(
        data: &mut Vec<u8>, original_guard: &mut Option<G>, meter: Arc<dyn NativeReadMeter>,
    ) -> io::Result<Self> {
        if original_guard.is_none() { return Err(io::ErrorKind::InvalidInput.into()); }
        let end = data.len();
        let capacity = data.capacity();
        let metadata = ReadAllocation::new(&meter, std::mem::size_of::<GuardBundle<G>>())?;
        let guard = Arc::new(GuardBundle {
            data: std::mem::take(data), _metadata: metadata,
            _original_guard: original_guard.take().expect("checked original custody"),
        });
        Ok(Self { bytes: Bytes::new(), start: 0, end, retained_capacity: Some(capacity), guard: Some(guard) })
    }

    /// Borrows immutable bytes while retaining this carrier's original custody.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        let data = self.guard.as_ref().map_or(self.bytes.as_ref(), |guard| guard.as_ref().as_ref());
        &data[self.start..self.end]
    }

    /// Returns the number of visible bytes.
    #[must_use]
    pub fn len(&self) -> usize { self.end - self.start }

    /// Returns whether the visible range is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool { self.start == self.end }

    /// Returns the observed original owned vector or boxed-slice capacity.
    #[must_use]
    pub const fn retained_capacity(&self) -> Option<usize> { self.retained_capacity }

    /// Recovers unique ordinary owned storage without allocating or moving bytes.
    pub fn try_into_ordinary_vec(self) -> Result<Vec<u8>, Self> {
        if self.guard.is_some() || self.retained_capacity.is_none()
            || self.start != 0 || self.end != self.bytes.len()
        {
            return Err(self);
        }
        let Self { bytes, start, end, retained_capacity, guard } = self;
        match bytes.try_into_mut() {
            Ok(bytes) => Ok(bytes.into()),
            Err(bytes) => Err(Self { bytes, start, end, retained_capacity, guard }),
        }
    }

    /// Compares only the SDK metadata payer's original allocation-owner identity.
    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    pub fn matches_meter(&self, meter: &Arc<dyn NativeReadMeter>) -> bool {
        self.guard.as_ref().is_some_and(|guard| {
            let owner = meter.allocation_owner().unwrap_or_else(|| Arc::clone(meter));
            Arc::ptr_eq(guard.metadata_meter(), &owner)
        })
    }

    /// Returns a bounded shared slice with the same original allocation custody.
    #[must_use]
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        if self.guard.is_none() && self.retained_capacity.is_none() { return self.bytes.slice(range).into(); }
        let begin = match range.start_bound() {
            Bound::Included(&begin) => begin,
            Bound::Excluded(&begin) => begin.checked_add(1).expect("slice start overflow"),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&end) => end.checked_add(1).expect("slice end overflow"),
            Bound::Excluded(&end) => end,
            Bound::Unbounded => self.len(),
        };
        assert!(begin <= end && end <= self.len(), "slice range exceeds visible bytes");
        Self {
            bytes: self.bytes.clone(), start: self.start + begin, end: self.start + end,
            retained_capacity: self.retained_capacity,
            guard: self.guard.clone(),
        }
    }

}

impl Default for OwnedBytes { fn default() -> Self { Self::new() } }
impl From<Bytes> for OwnedBytes {
    fn from(bytes: Bytes) -> Self { let end = bytes.len(); Self { bytes, start: 0, end, retained_capacity: None, guard: None } }
}
impl From<Vec<u8>> for OwnedBytes {
    fn from(bytes: Vec<u8>) -> Self {
        let retained_capacity = Some(bytes.capacity());
        let end = bytes.len();
        Self { bytes: Bytes::from(bytes), start: 0, end, retained_capacity, guard: None }
    }
}
impl From<Box<[u8]>> for OwnedBytes {
    fn from(bytes: Box<[u8]>) -> Self {
        let end = bytes.len();
        Self { bytes: Bytes::from(bytes), start: 0, end, retained_capacity: Some(end), guard: None }
    }
}
impl From<String> for OwnedBytes { fn from(bytes: String) -> Self { bytes.into_bytes().into() } }
impl From<&[u8]> for OwnedBytes { fn from(bytes: &[u8]) -> Self { Self::copy_from_slice(bytes) } }
impl From<&str> for OwnedBytes { fn from(bytes: &str) -> Self { Self::copy_from_slice(bytes.as_bytes()) } }
impl FromIterator<u8> for OwnedBytes {
    fn from_iter<T: IntoIterator<Item = u8>>(iter: T) -> Self { Vec::from_iter(iter).into() }
}
impl Deref for OwnedBytes { type Target = [u8]; fn deref(&self) -> &[u8] { self.as_slice() } }
impl AsRef<[u8]> for OwnedBytes { fn as_ref(&self) -> &[u8] { self.as_slice() } }
impl Borrow<[u8]> for OwnedBytes { fn borrow(&self) -> &[u8] { self.as_slice() } }
impl PartialEq for OwnedBytes { fn eq(&self, other: &Self) -> bool { self.as_slice() == other.as_slice() } }
impl Eq for OwnedBytes {}
impl PartialOrd for OwnedBytes { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }
impl Ord for OwnedBytes { fn cmp(&self, other: &Self) -> Ordering { self.as_slice().cmp(other.as_slice()) } }
impl Hash for OwnedBytes { fn hash<H: Hasher>(&self, state: &mut H) { self.as_slice().hash(state); } }
impl std::fmt::Debug for OwnedBytes {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.as_slice().fmt(formatter) }
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

#[cfg(not(target_arch = "wasm32"))]
/// Exclusive fixed-buffer preparation for the unified guarded byte carrier.
#[must_use]
pub struct PreparedOwnedBytes<G: Send + Sync + 'static> {
    maximum_len: usize,
    guard: Arc<GuardBundle<G>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl<G: Send + Sync + 'static> PreparedOwnedBytes<G> {
    /// Transfers a fully initialized vector and its actual original allocation guard.
    pub fn new(
        data: Vec<u8>, original_guard: G, meter: Arc<dyn NativeReadMeter>,
    ) -> Result<Self, PreparedOwnedBytesFailure<G>> {
        let maximum_len = data.len();
        if data.capacity() != maximum_len {
            return Err(PreparedOwnedBytesFailure {
                data, original_guard: Some(original_guard), error: PreparedOwnedBytesError::NotFullyInitialized,
            });
        }
        let metadata = ReadAllocation::new(&meter, std::mem::size_of::<GuardBundle<G>>())
            .map_err(|error| PreparedOwnedBytesError::Admission(error));
        match metadata {
            Ok(metadata) => Ok(Self {
                guard: Arc::new(GuardBundle { data, _metadata: metadata, _original_guard: original_guard }),
                maximum_len,
            }),
            Err(error) => Err(PreparedOwnedBytesFailure { data, original_guard: Some(original_guard), error }),
        }
    }

    /// Returns the initialized fixed writable bound established before the action.
    #[must_use]
    pub const fn maximum_len(&self) -> usize { self.maximum_len }

    /// Borrows only the fixed initialized slice. No vector or mutable owner escapes.
    pub fn writable(&mut self) -> &mut [u8] {
        &mut Arc::get_mut(&mut self.guard).expect("prepared storage has no published alias").data
    }

    /// Freezes the checked prefix into the unified carrier using existing controls.
    pub fn freeze(self, actual_len: usize) -> Result<OwnedBytes, Self> {
        if actual_len > self.maximum_len { return Err(self); }
        Ok(OwnedBytes {
            bytes: Bytes::new(), start: 0, end: actual_len,
            retained_capacity: Some(self.maximum_len), guard: Some(self.guard),
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<G: Send + Sync + 'static> AsMut<[u8]> for PreparedOwnedBytes<G> {
    fn as_mut(&mut self) -> &mut [u8] { self.writable() }
}

#[cfg(not(target_arch = "wasm32"))]
impl<G: Send + Sync + 'static> std::fmt::Debug for PreparedOwnedBytes<G> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("PreparedOwnedBytes").field("maximum_len", &self.maximum_len).finish_non_exhaustive()
    }
}

#[cfg(not(target_arch = "wasm32"))]
/// A typed construction invariant or the exact original meter admission cause.
#[derive(Debug)]
pub enum PreparedOwnedBytesError {
    /// The transferred initialized length does not cover its real capacity.
    NotFullyInitialized,
    /// Admission failed on the original meter before its corresponding allocator call.
    Admission(io::Error),
}

#[cfg(not(target_arch = "wasm32"))]
/// A construction failure retaining actual data before all original allocation custody.
#[must_use]
pub struct PreparedOwnedBytesFailure<G> {
    data: Vec<u8>,
    error: PreparedOwnedBytesError,
    original_guard: Option<G>,
}

#[cfg(not(target_arch = "wasm32"))]
impl<G> PreparedOwnedBytesFailure<G> {
    /// Borrows the typed original cause without formatting or allocating.
    #[must_use]
    pub const fn error(&self) -> &PreparedOwnedBytesError { &self.error }

    /// Physically drops the entire failed payload before returning its original cause.
    #[must_use]
    pub fn into_error(self) -> PreparedOwnedBytesError {
        let Self { data, error, original_guard } = self;
        drop(data);
        drop(original_guard);
        error
    }

    /// Returns the real retained capacity, without estimating private control layout.
    #[must_use]
    pub fn retained_capacity(&self) -> usize {
        self.data.capacity()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<G> std::fmt::Debug for PreparedOwnedBytesFailure<G> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("PreparedOwnedBytesFailure").field("error", &self.error)
            .field("retained_capacity", &self.retained_capacity()).finish_non_exhaustive()
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use acyclic_native_runtime::NativeOperationKind;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};

    struct Meter {
        cancelled: AtomicBool,
        attempts: AtomicU64,
        checks: AtomicU64,
        memory: AtomicU64,
        limit: AtomicU64,
    }

    impl Meter {
        fn new() -> Self {
            Self {
                cancelled: AtomicBool::new(false), attempts: AtomicU64::new(0),
                checks: AtomicU64::new(0), memory: AtomicU64::new(0),
                limit: AtomicU64::new(u64::MAX),
            }
        }

        fn admit_allocation(&self, operation: bool) -> io::Result<()> {
            let operations = self.attempts.load(AtomicOrdering::Relaxed)
                .checked_add(u64::from(operation));
            if operations.is_none_or(|value| value > self.limit.load(AtomicOrdering::Relaxed))
            {
                return Err(io::ErrorKind::OutOfMemory.into());
            }
            Ok(())
        }
    }

    impl NativeReadMeter for Meter {
        fn check(&self) -> io::Result<()> {
            self.checks.fetch_add(1, AtomicOrdering::Relaxed);
            if self.cancelled.load(AtomicOrdering::Relaxed) {
                Err(io::ErrorKind::Interrupted.into())
            } else { Ok(()) }
        }
        fn allocate(&self, bytes: u64) -> io::Result<()> {
            self.check()?;
            self.admit_allocation(true)?;
            self.attempts.fetch_add(1, AtomicOrdering::Relaxed);
            self.memory.fetch_add(bytes, AtomicOrdering::Relaxed);
            Ok(())
        }
        fn reserve_allocation(&self, _: u64) -> io::Result<()> { Err(io::ErrorKind::Unsupported.into()) }
        fn cancel_reserved_allocation(&self, _: u64) {}
        fn record_reserved_allocation(&self, _: u64) -> io::Result<()> { Err(io::ErrorKind::Unsupported.into()) }
        fn allocation_failed(&self, _: std::collections::TryReserveError) -> io::Error {
            io::ErrorKind::OutOfMemory.into()
        }
        fn allocate_refcount_header(&self) -> io::Result<()> { self.allocate(0) }
        fn adopt(&self, bytes: u64) -> io::Result<()> {
            self.check()?;
            self.admit_allocation(false)?;
            self.memory.fetch_add(bytes, AtomicOrdering::Relaxed);
            Ok(())
        }
        fn record_completed_adoption(&self, bytes: u64) -> (u64, io::Result<()>) {
            self.memory.fetch_add(bytes, AtomicOrdering::Relaxed);
            let recorded = self.admit_allocation(false).and_then(|()| self.check());
            (bytes, recorded)
        }
        fn release(&self, bytes: u64) {
            assert!(self.memory.fetch_sub(bytes, AtomicOrdering::Relaxed) >= bytes);
        }
        fn reserve_operations(&self, _: NativeOperationKind, _: u64) -> io::Result<()> { self.check() }
        fn cancel_reserved_operations(&self, _: NativeOperationKind, _: u64) {}
        fn record_reserved_operation(&self, _: NativeOperationKind) -> io::Result<()> { Ok(()) }
        fn admit_write_bytes(&self, _: usize) -> io::Result<()> { self.check() }
        fn reserve_bytes(&self, _: NativeOperationKind, _: u64) -> io::Result<()> { self.check() }
        fn cancel_reserved_bytes(&self, _: NativeOperationKind, _: u64) {}
        fn record_reserved_bytes(&self, _: NativeOperationKind, _: u64, _: u64) -> io::Result<()> { Ok(()) }
        fn examine(&self, _: u64, _: u64) -> io::Result<()> { self.check() }
        fn admit_examination(&self, _: u64, _: u64) -> io::Result<()> { self.check() }
        fn record_examination(&self, _: u64, _: u64) -> io::Result<()> { Ok(()) }
        fn admit_bytes(&self, _: usize) -> io::Result<()> { self.check() }
        fn admit_read(&self, _: usize) -> io::Result<()> { self.check() }
        fn admit_operations(&self, _: u64) -> io::Result<()> { self.check() }
        fn record_operations(&self, _: u64) -> io::Result<()> { Ok(()) }
        fn record_read(&self, _: usize) -> io::Result<()> { Ok(()) }
        fn admit_write(&self, _: usize) -> io::Result<()> { self.check() }
        fn record_write(&self, _: usize) -> io::Result<()> { Ok(()) }
        fn admit_write_operations(&self, _: u64) -> io::Result<()> { self.check() }
        fn record_write_operations(&self, _: u64) -> io::Result<()> { Ok(()) }
        fn admit_durability(&self) -> io::Result<()> { self.check() }
    }

    fn funded_data(meter: &Arc<dyn NativeReadMeter>, maximum: usize) -> (Vec<u8>, ReadAllocation) {
        let mut guard = ReadAllocation::new(meter, maximum).unwrap();
        let mut data = Vec::with_capacity(maximum);
        guard.reconcile_capacity(data.capacity()).unwrap();
        data.resize(data.capacity(), 0);
        (data, guard)
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_keeps_byte_array_wire_without_deserialized_native_custody() {
        let observer = Arc::new(Meter::new());
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let (mut data, guard) = funded_data(&meter, 3);
        data[..3].copy_from_slice(b"abc");
        let data = OwnedBytes::from_vec_with_guard(data, guard, Arc::clone(&meter)).unwrap().slice(..3);
        let attempts = observer.attempts.load(AtomicOrdering::Relaxed);
        let encoded = serde_json::to_string(&data).unwrap();
        assert_eq!(encoded, "[97,98,99]");
        let decoded: OwnedBytes = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.as_slice(), b"abc");
        assert!(!decoded.matches_meter(&meter));
        assert_eq!(observer.attempts.load(AtomicOrdering::Relaxed), attempts);
    }

    #[test]
    fn final_write_freeze_and_aliasing_need_no_late_admission() {
        let observer = Arc::new(Meter::new());
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let (data, guard) = funded_data(&meter, 23);
        let capacity = data.capacity();
        let mut builder = PreparedOwnedBytes::new(data, guard, meter).unwrap();
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed),
            (capacity + std::mem::size_of::<GuardBundle<ReadAllocation>>()) as u64);
        let checks = observer.checks.load(AtomicOrdering::Relaxed);
        let attempts = observer.attempts.load(AtomicOrdering::Relaxed);
        let retained = observer.memory.load(AtomicOrdering::Relaxed);
        observer.limit.store(3, AtomicOrdering::Relaxed);
        observer.cancelled.store(true, AtomicOrdering::Relaxed);
        builder.writable()[..4].copy_from_slice(b"done");
        let bytes = builder.freeze(4).unwrap();
        assert_eq!(bytes.as_slice(), b"done");
        let last_alias = bytes.slice(1..3);
        assert_eq!(last_alias.as_slice(), b"on");
        let empty_alias = bytes.slice(0..0);
        assert_eq!(observer.checks.load(AtomicOrdering::Relaxed), checks);
        assert_eq!(observer.attempts.load(AtomicOrdering::Relaxed), attempts);
        drop(bytes);
        drop(last_alias);
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), retained);
        assert!(empty_alias.is_empty());
        drop(empty_alias);
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), 0);
    }

    #[test]
    fn wrong_length_preserves_the_same_writable_owner() {
        let observer = Arc::new(Meter::new());
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let (data, guard) = funded_data(&meter, 11);
        let mut builder = PreparedOwnedBytes::new(data, guard, meter).unwrap();
        let maximum = builder.maximum_len();
        let retained = observer.memory.load(AtomicOrdering::Relaxed);
        builder = builder.freeze(maximum + 1).unwrap_err();
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), retained);
        builder.writable()[0] = 7;
        let bytes = builder.freeze(1).unwrap();
        assert_eq!(bytes.as_slice(), &[7]);
        drop(bytes);
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), 0);
    }

    #[test]
    fn invalid_initialization_keeps_original_data_claim_until_drop() {
        let observer = Arc::new(Meter::new());
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let (mut data, guard) = funded_data(&meter, 13);
        let capacity = data.capacity();
        data.truncate(capacity - 1);
        let failure = PreparedOwnedBytes::new(data, guard, meter).unwrap_err();
        assert!(matches!(failure.error(), PreparedOwnedBytesError::NotFullyInitialized));
        assert_eq!(failure.retained_capacity(), capacity);
        assert_eq!(observer.attempts.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), capacity as u64);
        drop(failure);
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), 0);
    }


    #[test]
    fn cancellation_before_construction_preserves_original_cause_and_data() {
        let observer = Arc::new(Meter::new());
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let (data, guard) = funded_data(&meter, 9);
        let capacity = data.capacity();
        observer.cancelled.store(true, AtomicOrdering::Relaxed);
        let failure = PreparedOwnedBytes::new(data, guard, meter).unwrap_err();
        match failure.error() {
            PreparedOwnedBytesError::Admission(error) => assert_eq!(error.kind(), io::ErrorKind::Interrupted),
            _ => panic!("original cancellation cause is retained"),
        }
        assert_eq!(observer.attempts.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), capacity as u64);
        drop(failure);
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), 0);
    }

    struct OriginalGuard {
        _allocation: ReadAllocation,
        meter: Arc<Meter>,
        memory_at_drop: Arc<AtomicU64>,
    }
    impl Drop for OriginalGuard {
        fn drop(&mut self) {
            self.memory_at_drop.store(self.meter.memory.load(AtomicOrdering::Relaxed), AtomicOrdering::Relaxed);
        }
    }

    #[test]
    fn original_data_claim_retires_after_owner_metadata() {
        let observer = Arc::new(Meter::new());
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let memory_at_drop = Arc::new(AtomicU64::new(u64::MAX));
        let (data, allocation) = funded_data(&meter, 17);
        let capacity = data.capacity();
        let guard = OriginalGuard { _allocation: allocation, meter: observer.clone(), memory_at_drop: memory_at_drop.clone() };
        let bytes = PreparedOwnedBytes::new(data, guard, meter).unwrap().freeze(3).unwrap();
        let last = bytes.clone();
        drop(bytes);
        assert_eq!(memory_at_drop.load(AtomicOrdering::Relaxed), u64::MAX);
        drop(last);
        assert_eq!(memory_at_drop.load(AtomicOrdering::Relaxed), capacity as u64);
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), 0);
    }


    #[test]
    fn known_ordinary_capacity_survives_shared_ranges() {
        let mut data = Vec::with_capacity(31);
        data.extend_from_slice(b"capacity");
        let capacity = data.capacity();
        let mut bytes = OwnedBytes::from(data);
        assert_eq!(bytes.retained_capacity(), Some(capacity));
        let empty = bytes.slice(0..0);
        let prefix = bytes.slice(..3);
        bytes = bytes.slice(3..);
        let suffix = bytes.slice(2..);
        for alias in [&empty, &prefix, &suffix, &bytes] {
            assert_eq!(alias.retained_capacity(), Some(capacity));
        }
        assert_eq!(prefix.as_slice(), b"cap");
        assert_eq!(suffix.as_slice(), b"ity");
        assert_eq!(OwnedBytes::from(String::from("string")).retained_capacity(), Some(6));
        assert_eq!(OwnedBytes::from(vec![1, 2].into_boxed_slice()).retained_capacity(), Some(2));
        assert_eq!(OwnedBytes::from(Bytes::from_static(b"opaque")).retained_capacity(), None);
    }


    #[test]
    fn ordinary_shared_take_preserves_carrier_until_last_alias_retires() {
        for capacity in [5, 31] {
            let mut data = Vec::with_capacity(capacity);
            data.extend_from_slice(b"alias");
            let pointer = data.as_ptr();
            let capacity = data.capacity();
            let bytes = OwnedBytes::from(data);
            let alias = bytes.clone();
            let recovered = bytes.try_into_ordinary_vec().unwrap_err();
            assert_eq!(recovered.as_slice(), b"alias");
            assert_eq!(recovered.as_slice().as_ptr(), pointer);
            assert_eq!((recovered.start, recovered.end), (0, 5));
            assert_eq!(recovered.retained_capacity(), Some(capacity));
            assert!(recovered.guard.is_none());
            assert_eq!(alias.as_slice().as_ptr(), pointer);
            drop(alias);
            let data = recovered.try_into_ordinary_vec().unwrap();
            assert_eq!(data.as_ptr(), pointer);
            assert_eq!(data.capacity(), capacity);
        }
    }

    #[test]
    fn ordinary_take_rejects_unique_narrowed_and_opaque_backing() {
        let bytes = OwnedBytes::from(vec![1, 2, 3, 4]);
        let narrowed = bytes.slice(1..3);
        drop(bytes);
        let pointer = narrowed.as_slice().as_ptr();
        let narrowed = narrowed.try_into_ordinary_vec().unwrap_err();
        assert_eq!(narrowed.as_slice(), [2, 3]);
        assert_eq!(narrowed.as_slice().as_ptr(), pointer);
        assert_eq!((narrowed.start, narrowed.end), (1, 3));
        assert_eq!(narrowed.retained_capacity(), Some(4));

        for bytes in [
            OwnedBytes::from_static(b"static"),
            OwnedBytes::from(Bytes::from_owner(vec![1, 2, 3])),
            OwnedBytes::from(Bytes::from(vec![4, 5, 6])),
        ] {
            let pointer = bytes.as_slice().as_ptr();
            let length = bytes.len();
            let recovered = bytes.try_into_ordinary_vec().unwrap_err();
            assert_eq!(recovered.as_slice().as_ptr(), pointer);
            assert_eq!(recovered.len(), length);
            assert_eq!(recovered.retained_capacity(), None);
            assert!(recovered.guard.is_none());
        }
    }

    #[test]
    fn guarded_take_rejects_before_touching_original_custody() {
        let observer = Arc::new(Meter::new());
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let (data, guard) = funded_data(&meter, 17);
        let pointer = data.as_ptr();
        let capacity = data.capacity();
        let bytes = OwnedBytes::from_vec_with_guard(data, guard, meter.clone()).unwrap();
        let guard_pointer = Arc::as_ptr(bytes.guard.as_ref().unwrap());
        let checks = observer.checks.load(AtomicOrdering::Relaxed);
        let attempts = observer.attempts.load(AtomicOrdering::Relaxed);
        let retained = observer.memory.load(AtomicOrdering::Relaxed);
        observer.cancelled.store(true, AtomicOrdering::Relaxed);
        observer.limit.store(attempts, AtomicOrdering::Relaxed);
        let recovered = bytes.try_into_ordinary_vec().unwrap_err();
        assert_eq!(recovered.as_slice().as_ptr(), pointer);
        assert_eq!(recovered.as_slice(), vec![0; capacity]);
        assert_eq!((recovered.start, recovered.end), (0, capacity));
        assert_eq!(recovered.retained_capacity(), Some(capacity));
        assert!(std::ptr::eq(Arc::as_ptr(recovered.guard.as_ref().unwrap()), guard_pointer));
        assert!(recovered.matches_meter(&meter));
        assert_eq!(observer.checks.load(AtomicOrdering::Relaxed), checks);
        assert_eq!(observer.attempts.load(AtomicOrdering::Relaxed), attempts);
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), retained);
        drop(recovered);
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), 0);
    }

    #[test]
    fn denied_in_place_transfer_keeps_original_vector_and_guard() {
        let observer = Arc::new(Meter::new());
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let memory_at_drop = Arc::new(AtomicU64::new(u64::MAX));
        let (mut data, allocation) = funded_data(&meter, 23);
        data[..4].copy_from_slice(b"keep");
        data.truncate(4);
        let pointer = data.as_ptr();
        let capacity = data.capacity();
        let mut guard = Some(OriginalGuard {
            _allocation: allocation, meter: observer.clone(), memory_at_drop: memory_at_drop.clone(),
        });
        let attempts = observer.attempts.load(AtomicOrdering::Relaxed);
        let retained = observer.memory.load(AtomicOrdering::Relaxed);
        observer.limit.store(attempts, AtomicOrdering::Relaxed);
        let error = OwnedBytes::from_vec_with_guard_in_place(&mut data, &mut guard, meter.clone())
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
        assert_eq!(data.as_ptr(), pointer);
        assert_eq!(data.capacity(), capacity);
        assert_eq!(data, b"keep");
        let original = guard.as_ref().unwrap();
        assert!(Arc::ptr_eq(&original.meter, &observer));
        assert!(Arc::ptr_eq(&original.memory_at_drop, &memory_at_drop));
        assert!(Arc::ptr_eq(original._allocation.meter(), &meter));
        assert_eq!(original._allocation.bytes(), capacity as u64);
        assert_eq!(memory_at_drop.load(AtomicOrdering::Relaxed), u64::MAX);
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), retained);

        observer.limit.store(u64::MAX, AtomicOrdering::Relaxed);
        let bytes = OwnedBytes::from_vec_with_guard_in_place(&mut data, &mut guard, meter.clone())
            .unwrap();
        assert!(data.is_empty());
        assert!(guard.is_none());
        assert_eq!(bytes.as_slice().as_ptr(), pointer);
        assert_eq!(bytes.as_slice(), b"keep");
        assert_eq!(bytes.retained_capacity(), Some(capacity));
        drop(bytes);
        assert_eq!(memory_at_drop.load(AtomicOrdering::Relaxed), retained);
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), 0);
    }

    #[test]
    fn immutable_transfer_promotes_controls_before_late_cancellation() {
        let observer = Arc::new(Meter::new());
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let (mut data, guard) = funded_data(&meter, 21);
        let capacity = data.capacity();
        data.truncate(4);
        let bytes = OwnedBytes::from_vec_with_guard(data, guard, meter.clone()).unwrap();
        assert_eq!(bytes.retained_capacity(), Some(capacity));
        assert!(bytes.matches_meter(&meter));
        let checks = observer.checks.load(AtomicOrdering::Relaxed);
        let attempts = observer.attempts.load(AtomicOrdering::Relaxed);
        let retained = observer.memory.load(AtomicOrdering::Relaxed);
        observer.cancelled.store(true, AtomicOrdering::Relaxed);
        observer.limit.store(3, AtomicOrdering::Relaxed);
        let alias = bytes.slice(0..0);
        drop(bytes);
        assert!(alias.is_empty());
        assert_eq!(alias.retained_capacity(), Some(capacity));
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), retained);
        assert_eq!(observer.checks.load(AtomicOrdering::Relaxed), checks);
        assert_eq!(observer.attempts.load(AtomicOrdering::Relaxed), attempts);
        drop(alias);
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), 0);
    }

    #[test]
    fn consuming_failure_returns_original_cause_after_payload_retirement() {
        let observer = Arc::new(Meter::new());
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let (data, guard) = funded_data(&meter, 7);
        observer.limit.store(observer.attempts.load(AtomicOrdering::Relaxed), AtomicOrdering::Relaxed);
        let failure = OwnedBytes::from_vec_with_guard(data, guard, meter).unwrap_err();
        assert!(observer.memory.load(AtomicOrdering::Relaxed) > 0);
        let cause = failure.into_error();
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), 0);
        match cause {
            PreparedOwnedBytesError::Admission(error) => assert_eq!(error.kind(), io::ErrorKind::OutOfMemory),
            _ => panic!("original IO cause moves without boxing"),
        }
    }
}
