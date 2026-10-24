use super::*;
use crate::OwnedBytes;
use crate::test_meter::Meter;
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

struct Witness {
    meter: Arc<Meter>,
    observed: Arc<AtomicU64>,
}
impl Drop for Witness {
    fn drop(&mut self) {
        self.observed
            .store(self.meter.held.load(Ordering::SeqCst), Ordering::SeqCst);
    }
}

struct KeyOwner {
    _allocation: ReadAllocation,
    _witness: Witness,
}

struct Fixture {
    keys: Vec<OwnedBytes>,
    allocation: ReadAllocation,
    meter: Arc<Meter>,
    observed: Arc<AtomicU64>,
}

fn fixture() -> Result<Fixture, Box<dyn std::error::Error>> {
    let meter = Arc::new(Meter::default());
    let original = meter.original();
    let observed = Arc::new(AtomicU64::new(u64::MAX));
    let mut allocation = ReadAllocation::empty(&original);
    let mut keys = crate::native::vector(1, &original, &mut allocation)?;
    let mut payload = ReadAllocation::empty(&original);
    let mut bytes = crate::native::vector(1, &original, &mut payload)?;
    bytes.push(1);
    let key = OwnedBytes::from_vec_with_guard(
        bytes,
        KeyOwner {
            _allocation: payload,
            _witness: Witness {
                meter: meter.clone(),
                observed: observed.clone(),
            },
        },
        original,
    )
    .map_err(|_| std::io::Error::from(std::io::ErrorKind::Other))?;
    keys.push(key);
    Ok(Fixture {
        keys,
        allocation,
        meter,
        observed,
    })
}

fn previous(
    keys: Vec<OwnedBytes>,
    allocation: ReadAllocation,
    meter: &Arc<dyn NativeReadMeter>,
) -> std::io::Result<()> {
    let _control = ReadAllocation::new(meter, RecordHeaders::control_bytes())?;
    drop(keys);
    drop(allocation);
    Ok(())
}

#[test]
fn native_header_failure_destroys_values_before_releasing_vector_custody()
-> Result<(), Box<dyn std::error::Error>> {
    for old in [false, true] {
        let Fixture {
            keys,
            allocation,
            meter,
            observed,
        } = fixture()?;
        let vector_bytes = allocation.bytes();
        meter.deny_allocation.store(true, Ordering::SeqCst);
        let result = if old {
            previous(keys, allocation, &meter.original())
        } else {
            RecordHeaders::from_native_parts(keys, Vec::new(), allocation, &meter.original())
                .map(|_| ())
        };
        assert!(result.is_err());
        assert_eq!(
            observed.load(Ordering::SeqCst),
            if old { 0 } else { vector_bytes }
        );
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
    }
    Ok(())
}

#[test]
fn native_header_parts_apply_the_same_canonical_key_checks_before_control_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    for (values, valid) in [
        ([b"a", b"b"], true),
        ([b"b", b"a"], false),
        ([b"a", b"a"], false),
    ] {
        let meter = Arc::new(Meter::default());
        let original = meter.original();
        let mut allocation = ReadAllocation::empty(&original);
        let mut keys = crate::native::vector(values.len(), &original, &mut allocation)?;
        keys.extend(values.map(|value| OwnedBytes::from_static(value)));
        let before = meter.allocations.load(Ordering::SeqCst);
        let headers = RecordHeaders::from_native_parts(keys, Vec::new(), allocation, &original);
        assert_eq!(headers.as_ref().is_ok_and(Result::is_ok), valid);
        if !valid {
            assert_eq!(meter.allocations.load(Ordering::SeqCst), before);
        }
        drop(headers);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
    }
    Ok(())
}
