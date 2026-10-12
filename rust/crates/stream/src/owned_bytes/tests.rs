use super::*;
use crate::test_meter::Meter;
use std::sync::atomic::{AtomicU64, Ordering as Order};

fn funded(meter: &Arc<dyn NativeReadMeter>, size: usize) -> (Vec<u8>, ReadAllocation) {
    let mut claim = ReadAllocation::new(meter, size).unwrap();
    let mut data = Vec::with_capacity(size);
    claim.reconcile_capacity(data.capacity()).unwrap();
    data.resize(data.capacity(), 0);
    (data, claim)
}
struct Owner {
    _allocation: ReadAllocation,
    meter: Arc<Meter>,
    retired: Arc<AtomicU64>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.retired
            .store(self.meter.held.load(Order::Relaxed), Order::Relaxed);
    }
}

#[test]
fn ordinary_storage_preserves_capacity_and_rejects_unsafe_recovery() {
    for requested in [5, 31] {
        let mut data = Vec::with_capacity(requested);
        data.extend_from_slice(b"alias");
        let (pointer, capacity) = (data.as_ptr(), data.capacity());
        let bytes = OwnedBytes::from(data);
        let alias = bytes.clone();
        let bytes = bytes.try_into_ordinary_vec().unwrap_err();
        assert_eq!(bytes.as_slice(), b"alias");
        assert_eq!(bytes.as_slice().as_ptr(), pointer);
        assert_eq!(bytes.retained_capacity(), Some(capacity));
        drop(alias);
        let data = bytes.try_into_ordinary_vec().unwrap();
        assert_eq!((data.as_ptr(), data.capacity()), (pointer, capacity));
    }
    let bytes = OwnedBytes::from(vec![1, 2, 3, 4]);
    let range = bytes.slice(1..3);
    drop(bytes);
    let range = range.try_into_ordinary_vec().unwrap_err();
    assert_eq!(range.as_slice(), [2, 3]);
    assert_eq!(range.retained_capacity(), Some(4));
    for bytes in [
        OwnedBytes::from_static(b"static"),
        Bytes::from_owner(vec![1, 2, 3]).into(),
        Bytes::from(vec![4, 5, 6]).into(),
    ] {
        let pointer = bytes.as_slice().as_ptr();
        let length = bytes.len();
        let bytes = bytes.try_into_ordinary_vec().unwrap_err();
        assert_eq!((bytes.as_slice().as_ptr(), bytes.len()), (pointer, length));
        assert_eq!(bytes.retained_capacity(), None);
    }
    let bytes = OwnedBytes::from(String::from("capacity"));
    for alias in [bytes.slice(..3), bytes.slice(3..), bytes.slice(0..0)] {
        assert_eq!(alias.retained_capacity(), bytes.retained_capacity());
    }
    assert_eq!(bytes.slice(..3).as_slice(), b"cap");
    assert_eq!(bytes.slice(3..).slice(2..).as_slice(), b"ity");
    assert_eq!(
        OwnedBytes::from(vec![1, 2].into_boxed_slice()).retained_capacity(),
        Some(2)
    );
}

#[test]
fn prepared_storage_freezes_after_cancellation_and_keeps_empty_aliases() {
    let observed = Arc::new(Meter::default());
    let meter = observed.original();
    let (data, claim) = funded(&meter, 23);
    let mut prepared = PreparedOwnedBytes::new(data, claim, meter.clone()).unwrap();
    let held = observed.held.load(Order::Relaxed);
    let maximum = prepared.maximum_len();
    prepared = prepared.freeze(maximum + 1).unwrap_err();
    assert_eq!(observed.held.load(Order::Relaxed), held);
    observed.limit.store(0, Order::Relaxed);
    observed.cancelled.store(true, Order::Relaxed);
    prepared.writable()[..4].copy_from_slice(b"done");
    let bytes = prepared.freeze(4).unwrap();
    assert!(bytes.matches_meter(&meter));
    let range = bytes.slice(1..3);
    let empty = bytes.slice(0..0);
    assert_eq!(bytes.as_slice(), b"done");
    assert_eq!(range.as_slice(), b"on");
    drop(bytes);
    drop(range);
    assert_eq!(observed.held.load(Order::Relaxed), held);
    drop(empty);
    assert_eq!(observed.held.load(Order::Relaxed), 0);
}

#[test]
fn rejected_preparation_preserves_data_and_original_cause() {
    for cancelled in [false, true] {
        let observed = Arc::new(Meter::default());
        let meter = observed.original();
        let (data, claim) = funded(&meter, 9);
        let capacity = data.capacity();
        if cancelled {
            observed.cancelled.store(true, Order::Relaxed);
        } else {
            observed
                .limit
                .store(observed.allocations.load(Order::Relaxed), Order::Relaxed);
        }
        let failure = PreparedOwnedBytes::new(data, claim, meter).unwrap_err();
        assert_eq!(failure.retained_capacity(), capacity);
        assert_eq!(observed.held.load(Order::Relaxed), capacity as u64);
        match failure.into_error() {
            PreparedOwnedBytesError::Admission(error) => assert_eq!(
                error.kind(),
                if cancelled {
                    io::ErrorKind::Interrupted
                } else {
                    io::ErrorKind::OutOfMemory
                }
            ),
            _ => panic!("original admission cause"),
        }
        assert_eq!(observed.held.load(Order::Relaxed), 0);
    }
    let observed = Arc::new(Meter::default());
    let meter = observed.original();
    let (mut data, claim) = funded(&meter, 13);
    data.truncate(data.capacity() - 1);
    let failure = PreparedOwnedBytes::new(data, claim, meter).unwrap_err();
    assert!(matches!(
        failure.error(),
        PreparedOwnedBytesError::NotFullyInitialized
    ));
    assert!(observed.held.load(Order::Relaxed) > 0);
    drop(failure);
    assert_eq!(observed.held.load(Order::Relaxed), 0);
}

#[test]
fn in_place_transfer_keeps_owner_on_denial_and_retires_metadata_first() {
    let observed = Arc::new(Meter::default());
    let meter = observed.original();
    let retired = Arc::new(AtomicU64::new(u64::MAX));
    let (mut data, allocation) = funded(&meter, 23);
    data[..4].copy_from_slice(b"keep");
    data.truncate(4);
    let (pointer, capacity) = (data.as_ptr(), data.capacity());
    let mut owner = Some(Owner {
        _allocation: allocation,
        meter: observed.clone(),
        retired: retired.clone(),
    });
    let held = observed.held.load(Order::Relaxed);
    observed
        .limit
        .store(observed.allocations.load(Order::Relaxed), Order::Relaxed);
    let error =
        OwnedBytes::from_vec_with_guard_in_place(&mut data, &mut owner, meter.clone()).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
    assert_eq!((data.as_ptr(), data.capacity()), (pointer, capacity));
    assert_eq!(data, b"keep");
    assert!(owner.is_some());
    assert_eq!(retired.load(Order::Relaxed), u64::MAX);
    assert_eq!(observed.held.load(Order::Relaxed), held);
    observed.limit.store(u64::MAX, Order::Relaxed);
    let bytes =
        OwnedBytes::from_vec_with_guard_in_place(&mut data, &mut owner, meter.clone()).unwrap();
    assert!(data.is_empty() && owner.is_none());
    assert_eq!(bytes.as_slice().as_ptr(), pointer);
    let held = observed.held.load(Order::Relaxed);
    observed.cancelled.store(true, Order::Relaxed);
    let bytes = bytes.try_into_ordinary_vec().unwrap_err();
    assert!(bytes.matches_meter(&meter));
    assert_eq!(observed.held.load(Order::Relaxed), held);
    let alias = bytes.slice(0..0);
    drop(bytes);
    assert_eq!(retired.load(Order::Relaxed), u64::MAX);
    drop(alias);
    assert_eq!(retired.load(Order::Relaxed), capacity as u64);
    assert_eq!(observed.held.load(Order::Relaxed), 0);
}

#[test]
fn consuming_transfer_failure_retires_payload_before_returning_the_cause() {
    let observed = Arc::new(Meter::default());
    let meter = observed.original();
    let (data, claim) = funded(&meter, 7);
    observed
        .limit
        .store(observed.allocations.load(Order::Relaxed), Order::Relaxed);
    let failure = OwnedBytes::from_vec_with_guard(data, claim, meter).unwrap_err();
    assert!(observed.held.load(Order::Relaxed) > 0);
    assert!(
        matches!(failure.into_error(), PreparedOwnedBytesError::Admission(error) if error.kind() == io::ErrorKind::OutOfMemory)
    );
    assert_eq!(observed.held.load(Order::Relaxed), 0);
}

#[cfg(feature = "serde")]
#[test]
fn serde_preserves_wire_bytes_without_native_ownership() {
    let observed = Arc::new(Meter::default());
    let meter = observed.original();
    let (mut data, claim) = funded(&meter, 3);
    data[..3].copy_from_slice(b"abc");
    let bytes = OwnedBytes::from_vec_with_guard(data, claim, meter.clone())
        .unwrap()
        .slice(..3);
    let encoded = serde_json::to_string(&bytes).unwrap();
    assert_eq!(encoded, "[97,98,99]");
    let decoded: OwnedBytes = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded.as_slice(), b"abc");
    assert!(!decoded.matches_meter(&meter));
}
