use super::*;
use crate::test_meter::Meter;
use std::sync::atomic::Ordering;

fn native(meter: &Arc<Meter>, capacity: usize) -> Result<OwnedBytes, Box<dyn std::error::Error>> {
    let original = meter.original();
    let mut allocation = ReadAllocation::new(&original, capacity)?;
    let mut data = Vec::with_capacity(capacity);
    allocation.reconcile_capacity(data.capacity())?;
    data.extend_from_slice(b"prefix,old");
    OwnedBytes::from_vec_with_guard(data, allocation, original)
        .map_err(|failure| format!("{:?}", failure.into_error()).into())
}

#[test]
fn exclusive_edits_preserve_pointers_and_original_custody_without_allocating()
-> Result<(), Box<dyn std::error::Error>> {
    let meter = Arc::new(Meter::default());
    for bytes in [
        OwnedBytes::from({
            let mut data = Vec::with_capacity(64);
            data.extend_from_slice(b"prefix,old");
            data
        }),
        native(&meter, 64)?,
    ] {
        let pointer = bytes.as_slice().as_ptr();
        let held = meter.held.load(Ordering::SeqCst);
        let allocations = meter.allocations.load(Ordering::SeqCst);
        let mut unique = bytes
            .try_into_unique()
            .map_err(|_| "fixture storage must be exclusive")?;
        unique.reserve_capacity(64)?;
        unique.replace_suffix(7, b"new")?;
        unique.replace_suffix(10, b",more")?;
        let bytes = unique.freeze();
        assert_eq!(bytes.as_slice(), b"prefix,new,more");
        assert_eq!(
            bytes.as_slice().as_ptr(),
            pointer,
            "exclusive continuation copied its prefix"
        );
        assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
        assert_eq!(meter.held.load(Ordering::SeqCst), held);
        drop(bytes);
    }
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn aliases_slices_and_borrowed_bytes_cannot_obtain_exclusive_editing()
-> Result<(), Box<dyn std::error::Error>> {
    let meter = Arc::new(Meter::default());
    for bytes in [
        OwnedBytes::from(b"prefix,old".to_vec()),
        native(&meter, 10)?,
    ] {
        let alias = bytes.clone();
        let result = bytes.try_into_unique();
        assert!(result.is_err(), "shared storage became mutable");
        let bytes = result.err().ok_or("shared storage became mutable")?;
        assert_eq!(alias.as_slice(), b"prefix,old");
        drop(alias);
        let slice = bytes.slice(1..);
        drop(bytes);
        let result = slice.try_into_unique();
        assert!(result.is_err(), "sliced storage became mutable");
        let slice = result.err().ok_or("sliced storage became mutable")?;
        assert_eq!(slice.as_slice(), b"refix,old");
    }
    assert!(
        OwnedBytes::from_static(b"borrowed")
            .try_into_unique()
            .is_err()
    );
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn growth_owns_the_full_relocation_peak_and_retires_all_capacity()
-> Result<(), Box<dyn std::error::Error>> {
    let meter = Arc::new(Meter::default());
    let mut bytes = native(&meter, 10)?
        .try_into_unique()
        .map_err(|_| "fixture storage must be exclusive")?;
    let initial = meter.held.load(Ordering::SeqCst);
    let old_capacity = bytes.capacity();
    for requested in [32, 64, 128, 256, 512] {
        let allocations = meter.allocations.load(Ordering::SeqCst);
        let before = bytes.as_slice().to_vec();
        bytes.reserve_capacity(requested)?;
        assert_eq!(bytes.as_slice(), before);
        assert!(bytes.capacity() >= requested);
        assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations + 1);
        assert_eq!(
            meter.held.load(Ordering::SeqCst),
            initial + (bytes.capacity() - old_capacity) as u64
        );
    }
    bytes.replace_suffix(7, &[b'x'; 400])?;
    let bytes = bytes.freeze();
    assert!(bytes.matches_meter(&meter.original()));
    assert_eq!(bytes.len(), 407);
    let alias = bytes.slice(0..0);
    drop(bytes);
    assert!(meter.held.load(Ordering::SeqCst) > 0);
    drop(alias);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn a_final_capacity_allowance_does_not_authorize_the_relocation_peak()
-> Result<(), Box<dyn std::error::Error>> {
    let maximum = OwnedBytes::vec_guard_header_bytes::<ReadAllocation>() + 64;
    let meter = Arc::new(Meter {
        memory_limit: maximum,
        ..Meter::default()
    });
    let mut bytes = native(&meter, 10)?
        .try_into_unique()
        .map_err(|_| "fixture storage must be exclusive")?;
    let held = meter.held.load(Ordering::SeqCst);
    let result = bytes.reserve_capacity(64);
    assert!(result.is_err(), "growth omitted its relocation peak");
    assert_eq!(
        result.err().ok_or("growth should be refused")?.kind(),
        io::ErrorKind::OutOfMemory,
        "growth admitted only the capacity delta and missed its relocation peak"
    );
    assert_eq!(bytes.as_slice(), b"prefix,old");
    assert_eq!(meter.held.load(Ordering::SeqCst), held);
    drop(bytes.freeze());
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn suffix_edits_copy_only_new_bytes_at_one_to_one_thousand_prefix_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    for length in [1, 10, 100, 1_000] {
        let meter = Arc::new(Meter::default());
        let original = meter.original();
        let mut owner = ReadAllocation::new(&original, length + 32)?;
        let mut data = Vec::with_capacity(length + 32);
        owner.reconcile_capacity(data.capacity())?;
        data.resize(length, b'p');
        data.extend_from_slice(b"old");
        let pointer = data.as_ptr();
        let bytes = OwnedBytes::from_vec_with_guard(data, owner, original)
            .map_err(|failure| format!("{:?}", failure.into_error()))?;
        let before = meter.copied.load(Ordering::SeqCst);
        let allocations = meter.allocations.load(Ordering::SeqCst);
        let mut bytes = bytes
            .try_into_unique()
            .map_err(|_| "fixture storage must be exclusive")?;
        bytes.replace_suffix(length, b"new suffix")?;
        assert_eq!(
            bytes.as_slice().as_ptr(),
            pointer,
            "suffix edit copied historical bytes at {length}"
        );
        assert_eq!(meter.copied.load(Ordering::SeqCst) - before, 10);
        assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
        assert_eq!(
            bytes.as_slice().get(..length),
            Some(vec![b'p'; length].as_slice())
        );
        assert_eq!(
            bytes.as_slice().get(length..),
            Some(b"new suffix".as_slice())
        );
        drop(bytes.freeze());
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[test]
fn denied_growth_and_edits_preserve_initialized_bytes_and_their_owner()
-> Result<(), Box<dyn std::error::Error>> {
    for cancelled in [false, true] {
        let meter = Arc::new(Meter::default());
        let mut bytes = native(&meter, 32)?
            .try_into_unique()
            .map_err(|_| "fixture storage must be exclusive")?;
        let pointer = bytes.as_slice().as_ptr();
        let held = meter.held.load(Ordering::SeqCst);
        if cancelled {
            meter.cancelled.store(true, Ordering::SeqCst);
        } else {
            meter
                .limit
                .store(meter.allocations.load(Ordering::SeqCst), Ordering::SeqCst);
        }
        let error = bytes
            .reserve_capacity(64)
            .err()
            .ok_or("growth should be refused")?;
        assert_eq!(
            error.kind(),
            if cancelled {
                io::ErrorKind::Interrupted
            } else {
                io::ErrorKind::OutOfMemory
            }
        );
        assert_eq!(bytes.as_slice(), b"prefix,old");
        assert_eq!(bytes.as_slice().as_ptr(), pointer);
        assert_eq!(meter.held.load(Ordering::SeqCst), held);
        assert!(bytes.replace_suffix(11, b"bad").is_err());
        assert!(bytes.replace_suffix(7, &[0; 33]).is_err());
        if cancelled {
            assert!(bytes.replace_suffix(7, b"new").is_err());
        }
        // Freezing and retirement never need fresh admission after cancellation.
        drop(bytes.freeze());
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[test]
fn a_late_copy_failure_keeps_completed_bytes_funded_until_retirement()
-> Result<(), Box<dyn std::error::Error>> {
    let meter = Arc::new(Meter {
        check_on_record: true,
        ..Meter::default()
    });
    let mut bytes = native(&meter, 32)?
        .try_into_unique()
        .map_err(|_| "fixture storage must be exclusive")?;
    let held = meter.held.load(Ordering::SeqCst);
    meter.cancel_on_copy.store(true, Ordering::SeqCst);
    assert_eq!(
        bytes
            .replace_suffix(7, b"completed")
            .err()
            .ok_or("copy cancellation was lost")?
            .kind(),
        io::ErrorKind::Interrupted
    );
    assert_eq!(bytes.as_slice(), b"prefix,completed");
    assert_eq!(meter.copied.load(Ordering::SeqCst), 9);
    assert_eq!(meter.held.load(Ordering::SeqCst), held);
    let bytes = bytes.freeze();
    assert!(bytes.matches_meter(&meter.original()));
    drop(bytes);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}
