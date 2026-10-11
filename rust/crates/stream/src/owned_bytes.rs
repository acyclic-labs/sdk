//! A single byte carrier preserves original allocation custody through every
//! alias. Native preparation uses only safe bytes APIs and creates all shared
//! controls before the source action; its final freeze cannot allocate.

#[cfg(not(target_arch = "wasm32"))]
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation};
use bytes::{Buf, Bytes};
#[cfg(not(target_arch = "wasm32"))]
use bytes::BytesMut;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
#[cfg(not(target_arch = "wasm32"))]
use std::io;
use std::ops::{Bound, Deref, RangeBounds};
use std::sync::Arc;

trait AllocationCustody: Send + Sync {
    #[cfg(not(target_arch = "wasm32"))]
    fn metadata_meter(&self) -> &Arc<dyn NativeReadMeter>;
}

// Original data custody retires last, after the SDK-owned metadata claim.
#[cfg(not(target_arch = "wasm32"))]
struct GuardBundle<G> {
    _metadata: ReadAllocation,
    _original_guard: G,
}
#[cfg(not(target_arch = "wasm32"))]
impl<G: Send + Sync> AllocationCustody for GuardBundle<G> {
    fn metadata_meter(&self) -> &Arc<dyn NativeReadMeter> { self._metadata.meter() }
}

/// Ordinary or original-allocation-guarded immutable bytes.
///
/// All clones and slices keep allocation custody inseparable from the bytes.
/// Only borrowed byte slices are exposed: no owned raw `Bytes` alias can escape.
/// Fields retire in declaration order, so each physical byte alias is dropped
/// before its guard alias, including the last carrier.
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
    ///
    /// The original meter funds the real SDK-owned guard Arc and any required
    /// byte-control promotion before their allocator calls. The vector is never
    /// copied or adopted again. All subsequent guarded clones and slices reuse
    /// these shared controls, including fully initialized and empty input.
    /// Private control layout and allocator heap/RSS remain excluded.
    ///
    /// # Errors
    /// Retains the actual whole vector and every created guard alongside the
    /// original admission cause until failure destruction, data first.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_vec_with_guard<G: Send + Sync + 'static>(
        data: Vec<u8>, original_guard: G, meter: Arc<dyn NativeReadMeter>,
    ) -> Result<Self, PreparedOwnedBytesFailure<G>> {
        let end = data.len();
        let capacity = data.capacity();
        let metadata = match ReadAllocation::new(&meter, std::mem::size_of::<GuardBundle<G>>()) {
            Ok(metadata) => metadata,
            Err(error) => return Err(PreparedOwnedBytesFailure {
                data: FailureData::Vector(data), error: PreparedOwnedBytesError::Admission(error),
                shared_guard: None, original_guard: Some(original_guard),
            }),
        };
        let guard = Arc::new(GuardBundle { _metadata: metadata, _original_guard: original_guard });
        if capacity != end || end != 0 {
            if let Err(error) = meter.allocate_refcount_header() {
                return Err(PreparedOwnedBytesFailure {
                    data: FailureData::Vector(data), error: PreparedOwnedBytesError::Admission(error),
                    shared_guard: Some(guard), original_guard: None,
                });
            }
        }
        let bytes = Bytes::from(data);
        // A spare-capacity vector creates Shared in Bytes::from already. An
        // exact-capacity nonempty vector initially uses promotable Box backing;
        // its first clone creates Shared now, never during a later native clone.
        if capacity == end && end != 0 { drop(bytes.clone()); }
        Ok(Self { bytes, start: 0, end, retained_capacity: Some(capacity), guard: Some(guard) })
    }

    /// Borrows immutable bytes while retaining this carrier's original custody.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] { &self.bytes[self.start..self.end] }

    /// Returns the number of visible bytes.
    #[must_use]
    pub fn len(&self) -> usize { self.end - self.start }

    /// Returns whether the visible range is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool { self.start == self.end }

    /// Returns the observed original owned vector or boxed-slice capacity.
    /// Opaque Bytes backing returns `None`, never a guessed capacity.
    #[must_use]
    pub const fn retained_capacity(&self) -> Option<usize> { self.retained_capacity }

    /// Compares only the SDK metadata payer's original allocation-owner identity.
    /// This pure query performs no admission and grants no namespace permission.
    /// It does not prove data origin or authority. Cached bytes can legitimately
    /// retain a prior invocation's metadata payer; a new reader must not reject
    /// them solely because that payer differs from its own current query.
    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    pub fn matches_meter(&self, meter: &Arc<dyn NativeReadMeter>) -> bool {
        self.guard.as_ref().is_some_and(|guard| {
            let owner = meter.allocation_owner().unwrap_or_else(|| Arc::clone(meter));
            Arc::ptr_eq(guard.metadata_meter(), &owner)
        })
    }

    /// Returns a bounded shared slice with the same original allocation custody.
    ///
    /// # Panics
    /// Panics for an invalid or out-of-range slice, like the underlying bytes API.
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
        // Preserve the full backing even for an empty view: the physical data
        // and its original claim must retire together at the final carrier.
        Self {
            bytes: self.bytes.clone(), start: self.start + begin, end: self.start + end,
            retained_capacity: self.retained_capacity,
            guard: self.guard.clone(),
        }
    }

    /// Shares an existing subslice while retaining original allocation custody.
    ///
    /// # Panics
    /// Panics unless `subset` is a subslice of this carrier's visible bytes.
    #[must_use]
    pub fn slice_ref(&self, subset: &[u8]) -> Self {
        if self.guard.is_none() && self.retained_capacity.is_none() { return self.bytes.slice_ref(subset).into(); }
        if subset.is_empty() { return self.slice(0..0); }
        let begin = (subset.as_ptr() as usize).checked_sub(self.as_slice().as_ptr() as usize)
            .expect("subset precedes visible bytes");
        let end = begin.checked_add(subset.len()).expect("subset length overflow");
        self.slice(begin..end)
    }

    /// Splits off the suffix while both carriers retain original custody.
    ///
    /// # Panics
    /// Panics if `at` exceeds the visible length.
    #[must_use]
    pub fn split_off(&mut self, at: usize) -> Self {
        if self.guard.is_none() && self.retained_capacity.is_none() {
            let suffix = self.bytes.split_off(at).into();
            self.end = self.bytes.len();
            return suffix;
        }
        let suffix = self.slice(at..);
        self.truncate(at);
        suffix
    }

    /// Splits off the prefix while both carriers retain original custody.
    ///
    /// # Panics
    /// Panics if `at` exceeds the visible length.
    #[must_use]
    pub fn split_to(&mut self, at: usize) -> Self {
        if self.guard.is_none() && self.retained_capacity.is_none() {
            let prefix = self.bytes.split_to(at).into();
            self.end = self.bytes.len();
            return prefix;
        }
        let prefix = self.slice(..at);
        self.advance(at);
        prefix
    }

    /// Shortens the visible range with the underlying ordinary bytes semantics.
    /// Original custody remains retained until this carrier is destroyed.
    pub fn truncate(&mut self, len: usize) {
        if self.guard.is_none() && self.retained_capacity.is_none() {
            self.bytes.truncate(len);
            self.end = self.bytes.len();
        } else if len < self.len() {
            self.end = self.start + len;
        }
    }

    /// Clears the visible range while retaining this carrier's original custody.
    pub fn clear(&mut self) { self.truncate(0); }

    /// Advances the visible prefix while retaining original allocation custody.
    ///
    /// # Panics
    /// Panics if `count` exceeds the visible length.
    pub fn advance(&mut self, count: usize) {
        assert!(count <= self.len(), "advance exceeds visible bytes");
        if self.guard.is_none() && self.retained_capacity.is_none() {
            self.bytes.advance(count);
            self.end = self.bytes.len();
        } else {
            self.start += count;
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

#[cfg(all(target_arch = "wasm32", feature = "wasm"))]
impl serde::Serialize for OwnedBytes {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(self.as_slice(), serializer)
    }
}

#[cfg(all(target_arch = "wasm32", feature = "wasm"))]
impl<'de> serde::Deserialize<'de> for OwnedBytes {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <Vec<u8> as serde::Deserialize<'de>>::deserialize(deserializer).map(Self::from)
    }
}

#[cfg(not(target_arch = "wasm32"))]
/// Exclusive fixed-buffer preparation for the unified guarded byte carrier.
///
/// The actual initialized vector and original guard are transferred, not rebilled.
/// Safe unique bytes conversion recovers the mutable buffer before any immutable
/// alias escapes. `split_to(0)` then unconditionally promotes its Vec-backed
/// BytesMut control before the action. The private empty sibling keeps that
/// shared control alive until the final allocation-free freeze.
#[must_use]
pub struct PreparedOwnedBytes<G: Send + Sync + 'static> {
    data: BytesMut,
    promotion_sibling: BytesMut,
    maximum_len: usize,
    guard: Arc<GuardBundle<G>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl<G: Send + Sync + 'static> PreparedOwnedBytes<G> {
    /// Transfers a fully initialized vector and its actual original allocation guard.
    ///
    /// Requires `len() == capacity()` and retains the entire real capacity. The
    /// caller's guard must own that capacity; no adoption, replacement meter, or
    /// namespace permission is created. The original meter funds the compiled
    /// SDK-owned `GuardBundle<G>` immediately before its actual Arc allocation,
    /// and admits the actual private BytesMut promotion before `split_to(0)`.
    /// Private Arc/BytesMut backing layout and heap/RSS are not estimated.
    /// Metadata claims use the original allocation-owner projection.
    ///
    /// # Errors
    /// Every typed invariant or original admission cause retains all real storage
    /// and guards until failure destruction, with data retired before custody.
    /// Infallible Arc/bytes allocator calls retain their normal failure behavior.
    pub fn new(
        data: Vec<u8>, original_guard: G, meter: Arc<dyn NativeReadMeter>,
    ) -> Result<Self, PreparedOwnedBytesFailure<G>> {
        let maximum_len = data.len();
        let mut failure = PreparedOwnedBytesFailure {
            data: FailureData::Vector(data),
            error: PreparedOwnedBytesError::NotFullyInitialized,
            original_guard: Some(original_guard),
            shared_guard: None,
        };
        if failure.retained_capacity() != maximum_len { return Err(failure); }
        let metadata = match ReadAllocation::new(&meter, std::mem::size_of::<GuardBundle<G>>()) {
            Ok(metadata) => metadata,
            Err(error) => { failure.error = PreparedOwnedBytesError::Admission(error); return Err(failure); }
        };
        let original_guard = failure.original_guard.take().expect("original custody is transferred once");
        let guard = Arc::new(GuardBundle { _metadata: metadata, _original_guard: original_guard });
        failure.shared_guard = Some(Arc::clone(&guard));
        if let Err(error) = meter.allocate_refcount_header() {
            failure.error = PreparedOwnedBytesError::Admission(error);
            return Err(failure);
        }
        let FailureData::Vector(data) = failure.data else { unreachable!() };
        // For len == capacity, Bytes::from(Vec) uses into_boxed_slice without a
        // control allocation. Its unique try_into_mut reconstructs the same Vec
        // as BytesMut without allocation. The zero-capacity case owns no data.
        let mut data = if maximum_len == 0 {
            drop(data);
            BytesMut::new()
        } else {
            match Bytes::from(data).try_into_mut() {
                Ok(data) => data,
                Err(data) => {
                    return Err(PreparedOwnedBytesFailure {
                        data: FailureData::Immutable(data), error: PreparedOwnedBytesError::NotUnique,
                        original_guard: None, shared_guard: failure.shared_guard,
                    });
                }
            }
        };
        // bytes 1.12.1 split_to(0) calls shallow_clone unconditionally; Vec-backed
        // shallow_clone allocates/promotes Shared even for this empty sibling.
        let promotion_sibling = data.split_to(0);
        Ok(Self { data, promotion_sibling, maximum_len, guard })
    }

    /// Returns the initialized fixed writable bound established before the action.
    #[must_use]
    pub const fn maximum_len(&self) -> usize { self.maximum_len }

    /// Borrows only the fixed initialized slice. No vector or mutable owner escapes.
    pub fn writable(&mut self) -> &mut [u8] { self.data.as_mut() }

    /// Freezes the checked prefix into the unified carrier using existing controls.
    /// No allocation, copying, cancellation check, admission, or receipt change
    /// occurs. Consuming this builder structurally ends writable access.
    ///
    /// # Errors
    /// Returns the unchanged owning builder when the prefix exceeds its fixed bound.
    pub fn freeze(mut self, actual_len: usize) -> Result<OwnedBytes, Self> {
        if actual_len > self.maximum_len { return Err(self); }
        self.data.truncate(actual_len);
        let Self { data, promotion_sibling, guard, maximum_len } = self;
        let bytes = data.freeze();
        drop(promotion_sibling);
        Ok(OwnedBytes { bytes, start: 0, end: actual_len, retained_capacity: Some(maximum_len), guard: Some(guard) })
    }
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
    /// The safe unique conversion unexpectedly could not recover mutable ownership.
    NotUnique,
    /// Admission failed on the original meter before its corresponding allocator call.
    Admission(io::Error),
}

#[cfg(not(target_arch = "wasm32"))]
enum FailureData { Vector(Vec<u8>), Immutable(Bytes) }

#[cfg(not(target_arch = "wasm32"))]
/// A construction failure retaining actual data before all original allocation custody.
#[must_use]
pub struct PreparedOwnedBytesFailure<G> {
    data: FailureData,
    error: PreparedOwnedBytesError,
    shared_guard: Option<Arc<GuardBundle<G>>>,
    original_guard: Option<G>,
}

#[cfg(not(target_arch = "wasm32"))]
impl<G> PreparedOwnedBytesFailure<G> {
    /// Borrows the typed original cause without formatting or allocating.
    #[must_use]
    pub const fn error(&self) -> &PreparedOwnedBytesError { &self.error }

    /// Physically drops the entire failed payload before returning its original
    /// cause. Data retires first, then created metadata and original data custody.
    /// This deliberate consuming destruction performs no admission or allocation.
    #[must_use]
    pub fn into_error(self) -> PreparedOwnedBytesError {
        let Self { data, error, shared_guard, original_guard } = self;
        drop(data);
        drop(shared_guard);
        drop(original_guard);
        error
    }

    /// Returns the real retained capacity, without estimating private control layout.
    #[must_use]
    pub fn retained_capacity(&self) -> usize {
        match &self.data { FailureData::Vector(data) => data.capacity(), FailureData::Immutable(data) => data.len() }
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
            let next = self.attempts.load(AtomicOrdering::Relaxed) + 1;
            if next > self.limit.load(AtomicOrdering::Relaxed) {
                return Err(io::ErrorKind::OutOfMemory.into());
            }
            self.attempts.store(next, AtomicOrdering::Relaxed);
            self.memory.fetch_add(bytes, AtomicOrdering::Relaxed);
            Ok(())
        }
        fn allocation_failed(&self, _: std::collections::TryReserveError) -> io::Error {
            io::ErrorKind::OutOfMemory.into()
        }
        fn allocate_refcount_header(&self) -> io::Result<()> { self.allocate(0) }
        fn adopt(&self, bytes: u64) -> io::Result<()> {
            self.check()?;
            self.memory.fetch_add(bytes, AtomicOrdering::Relaxed);
            Ok(())
        }
        fn record_completed_adoption(&self, bytes: u64) -> (u64, io::Result<()>) {
            self.memory.fetch_add(bytes, AtomicOrdering::Relaxed);
            (bytes, self.check())
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
        fn admit_durability(&self) -> io::Result<()> { self.check() }
    }

    fn funded_data(meter: &Arc<dyn NativeReadMeter>, maximum: usize) -> (Vec<u8>, ReadAllocation) {
        let mut guard = ReadAllocation::new(meter, maximum).unwrap();
        let mut data = Vec::with_capacity(maximum);
        guard.reconcile_capacity(data.capacity()).unwrap();
        data.resize(data.capacity(), 0);
        (data, guard)
    }

    #[test]
    fn final_write_freeze_and_aliasing_need_no_late_admission() {
        let observer = Arc::new(Meter::new());
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let (data, guard) = funded_data(&meter, 23);
        let capacity = data.capacity();
        let mut builder = PreparedOwnedBytes::new(data, guard, meter).unwrap();
        assert_eq!(observer.attempts.load(AtomicOrdering::Relaxed), 3);
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed),
            (capacity + std::mem::size_of::<GuardBundle<ReadAllocation>>()) as u64);
        let checks = observer.checks.load(AtomicOrdering::Relaxed);
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
        assert_eq!(observer.attempts.load(AtomicOrdering::Relaxed), 3);
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
    fn denied_header_keeps_real_data_and_created_arc_until_failure_drop() {
        let observer = Arc::new(Meter::new());
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let (data, guard) = funded_data(&meter, 19);
        let capacity = data.capacity();
        observer.limit.store(2, AtomicOrdering::Relaxed);
        let failure = PreparedOwnedBytes::new(data, guard, meter).unwrap_err();
        match failure.error() {
            PreparedOwnedBytesError::Admission(error) => assert_eq!(error.kind(), io::ErrorKind::OutOfMemory),
            _ => panic!("original admission cause is retained"),
        }
        assert_eq!(observer.attempts.load(AtomicOrdering::Relaxed), 2);
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed),
            (capacity + std::mem::size_of::<GuardBundle<ReadAllocation>>()) as u64);
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

    #[test]
    fn abort_and_empty_freeze_retain_no_leaked_claims() {
        let observer = Arc::new(Meter::new());
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let (data, guard) = funded_data(&meter, 5);
        drop(PreparedOwnedBytes::new(data, guard, meter.clone()).unwrap());
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), 0);
        let (data, guard) = funded_data(&meter, 5);
        let mut bytes = PreparedOwnedBytes::new(data, guard, meter).unwrap().freeze(0).unwrap();
        let retained = observer.memory.load(AtomicOrdering::Relaxed);
        assert!(retained > 0);
        let last_alias = bytes.clone();
        bytes.clear();
        drop(bytes);
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), retained);
        drop(last_alias);
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
    fn ordinary_carrier_preserves_visible_bytes_and_splitting() {
        let mut bytes = OwnedBytes::from_static(b"ordinary");
        let prefix = bytes.split_to(3);
        assert_eq!(prefix.as_slice(), b"ord");
        assert_eq!(bytes.as_slice(), b"inary");
        let suffix = bytes.split_off(2);
        assert_eq!(bytes.as_slice(), b"in");
        assert_eq!(suffix.as_slice(), b"ary");
        bytes.advance(1);
        assert_eq!(bytes.as_slice(), b"n");
        bytes.clear();
        assert!(bytes.is_empty());
    }

    #[test]
    fn known_ordinary_capacity_survives_shared_ranges() {
        let mut data = Vec::with_capacity(31);
        data.extend_from_slice(b"capacity");
        let capacity = data.capacity();
        let mut bytes = OwnedBytes::from(data);
        assert_eq!(bytes.retained_capacity(), Some(capacity));
        let empty = bytes.slice(0..0);
        let prefix = bytes.split_to(3);
        let suffix = bytes.split_off(2);
        bytes.clear();
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
    fn immutable_transfer_promotes_controls_before_late_cancellation() {
        let observer = Arc::new(Meter::new());
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let (mut data, guard) = funded_data(&meter, 21);
        let capacity = data.capacity();
        data.truncate(4);
        let bytes = OwnedBytes::from_vec_with_guard(data, guard, meter.clone()).unwrap();
        assert_eq!(bytes.retained_capacity(), Some(capacity));
        assert!(bytes.matches_meter(&meter));
        assert_eq!(observer.attempts.load(AtomicOrdering::Relaxed), 3);
        let checks = observer.checks.load(AtomicOrdering::Relaxed);
        let retained = observer.memory.load(AtomicOrdering::Relaxed);
        observer.cancelled.store(true, AtomicOrdering::Relaxed);
        observer.limit.store(3, AtomicOrdering::Relaxed);
        let mut alias = bytes.clone();
        alias.clear();
        drop(bytes);
        assert!(alias.is_empty());
        assert_eq!(alias.retained_capacity(), Some(capacity));
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), retained);
        assert_eq!(observer.checks.load(AtomicOrdering::Relaxed), checks);
        assert_eq!(observer.attempts.load(AtomicOrdering::Relaxed), 3);
        drop(alias);
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), 0);
    }

    #[test]
    fn immutable_exact_capacity_transfer_precreates_clone_control() {
        let observer = Arc::new(Meter::new());
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let (data, guard) = funded_data(&meter, 15);
        let bytes = OwnedBytes::from_vec_with_guard(data, guard, meter).unwrap();
        assert_eq!(observer.attempts.load(AtomicOrdering::Relaxed), 3);
        observer.cancelled.store(true, AtomicOrdering::Relaxed);
        let alias = bytes.slice(1..2);
        assert_eq!(alias.len(), 1);
        drop(bytes);
        assert!(observer.memory.load(AtomicOrdering::Relaxed) > 0);
        drop(alias);
        assert_eq!(observer.memory.load(AtomicOrdering::Relaxed), 0);
    }

    #[test]
    fn consuming_failure_returns_original_cause_after_payload_retirement() {
        let observer = Arc::new(Meter::new());
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let (data, guard) = funded_data(&meter, 7);
        observer.limit.store(2, AtomicOrdering::Relaxed);
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
