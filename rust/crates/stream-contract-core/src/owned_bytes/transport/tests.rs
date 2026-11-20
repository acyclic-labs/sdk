use super::*;
use crate::test_meter::Meter;
use std::sync::atomic::Ordering;

#[test]
fn transport_transfer_preserves_original_backing_credit_and_refusal()
-> Result<(), Box<dyn std::error::Error>> {
    for length in [1024, 1024 * 1024] {
        let ordinary = OwnedBytes::from(vec![23; length]);
        let pointer = ordinary.as_ptr();
        let mut transferred = None;
        let physical = acyclic_test_support::measure(|| {
            transferred = Some(ordinary.try_into_bytes().unwrap());
        });
        assert_eq!(physical.count_total, 0);
        assert_eq!(transferred.unwrap().as_ptr(), pointer);

        for native in [false, true] {
            let observed = Arc::new(Meter::default());
            let meter = observed.original();
            let data = vec![42; length];
            let bytes = if native {
                let allocation = ReadAllocation::new(&meter, data.capacity())?;
                OwnedBytes::from_vec_with_guard(data, allocation, meter.clone())
                    .map_err(|failure| format!("{:?}", failure.into_error()))?
            } else {
                data.into()
            };
            // Shadowing would keep the unsliced original (and its custody)
            // alive to the end of the iteration; the block retires it here.
            let bytes = {
                let original = bytes;
                original.slice(1..length - 1).slice(1..)
            };
            let pointer = bytes.as_ptr();
            let held = observed.held.load(Ordering::SeqCst);
            let header = if native {
                acyclic_auth::operation::bytes_owner_allocation_bytes::<NativeTransport>() as u64
            } else {
                0
            };
            let mut transferred = None;
            let physical = acyclic_test_support::measure(|| {
                transferred = Some(bytes.try_into_bytes().unwrap());
            });
            assert_eq!(physical.count_total, u64::from(native));
            assert_eq!(physical.bytes_total, header);
            assert_eq!(observed.held.load(Ordering::SeqCst), held + header);
            let body = transferred.unwrap();
            assert_eq!(body.as_ptr(), pointer);
            assert_eq!(body.len(), length - 3);

            // Restore the former transport copy as an independent cost control.
            let mut copied = None;
            let old = acyclic_test_support::measure(|| {
                copied = Some(Bytes::copy_from_slice(&body));
            });
            let copied = copied.unwrap();
            assert_eq!(copied, body);
            assert_ne!(copied.as_ptr(), body.as_ptr());
            assert!(old.bytes_total >= body.len() as u64);
            drop(copied);

            observed.cancelled.store(true, Ordering::SeqCst);
            let mut shared = None;
            let physical = acyclic_test_support::measure(|| {
                let clone = body.clone();
                shared = Some(clone.slice(1..clone.len() - 1));
            });
            assert_eq!(physical.count_total, 0);
            let part = shared.unwrap();
            assert_eq!(part.as_ptr(), body.as_ptr().wrapping_add(1));
            drop(body);
            assert_eq!(observed.held.load(Ordering::SeqCst), held + header);
            assert!(part.iter().all(|byte| *byte == 42));
            drop(part);
            assert_eq!(observed.held.load(Ordering::SeqCst), 0);
            observed.assert_refunded();
        }
    }

    for cancelled in [false, true] {
        let observed = Arc::new(Meter::default());
        let meter = observed.original();
        let data = vec![7; 1024];
        let allocation = ReadAllocation::new(&meter, data.capacity())?;
        let bytes = OwnedBytes::from_vec_with_guard(data, allocation, meter)
            .map_err(|failure| format!("{:?}", failure.into_error()))?;
        let original = bytes.clone();
        let held = observed.held.load(Ordering::SeqCst);
        let admissions = observed.allocations.load(Ordering::SeqCst);
        if cancelled {
            observed.cancelled.store(true, Ordering::SeqCst);
        } else {
            observed.limit.store(admissions, Ordering::SeqCst);
        }
        let error = bytes.try_into_bytes().unwrap_err();
        assert_eq!(
            error.kind(),
            if cancelled {
                io::ErrorKind::Interrupted
            } else {
                io::ErrorKind::OutOfMemory
            }
        );
        assert_eq!(observed.allocations.load(Ordering::SeqCst), admissions);
        assert_eq!(observed.held.load(Ordering::SeqCst), held);
        assert_eq!(original.as_slice(), &[7; 1024]);
        drop(original);
        assert_eq!(observed.held.load(Ordering::SeqCst), 0);
        observed.assert_refunded();
    }
    Ok(())
}
