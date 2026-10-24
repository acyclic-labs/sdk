use super::*;
use crate::test_meter::Meter;
use std::sync::atomic::{AtomicUsize, Ordering};

#[repr(align(64))]
struct Aligned {
    _allocation: ReadAllocation,
}

fn check<G: Send + Sync + 'static>(
    wrap: impl Fn(ReadAllocation) -> G,
) -> Result<(), Box<dyn std::error::Error>> {
    for mode in 0..3 {
        let observed = Arc::new(Meter::default());
        let meter = observed.original();
        let data = vec![42; 8];
        let capacity = data.capacity();
        let owner = wrap(ReadAllocation::new(&meter, capacity)?);
        let bytes = match mode {
            0 => OwnedBytes::from_vec_with_guard(data, owner, meter)
                .map_err(|failure| format!("{:?}", failure.into_error()))?,
            1 => {
                let mut data = data;
                OwnedBytes::from_vec_with_guard_in_place(&mut data, &mut Some(owner), &meter)?
            }
            _ => PreparedOwnedBytes::new(data, owner, &meter)
                .map_err(|failure| format!("{:?}", failure.into_error()))?
                .freeze(8)
                .map_err(|_| "invalid initialized fixture")?,
        };
        let header = observed.held.load(Ordering::SeqCst) - capacity as u64;
        let minimum =
            (std::mem::size_of::<GuardBundle<G>>() + 2 * std::mem::size_of::<AtomicUsize>()) as u64;
        let alignment = std::mem::align_of::<GuardBundle<G>>() as u64;
        assert!(header >= minimum, "native byte header omitted Arc counters");
        assert_eq!(
            header % alignment,
            0,
            "native byte header omitted alignment padding"
        );
        assert!(header < minimum + alignment);
        assert_eq!(bytes.as_slice(), &[42; 8]);
        drop(bytes);
        assert_eq!(observed.held.load(Ordering::SeqCst), 0);
        observed.assert_refunded();
    }
    Ok(())
}

#[test]
fn every_byte_transfer_funds_arc_counters_and_owner_alignment()
-> Result<(), Box<dyn std::error::Error>> {
    check(std::convert::identity)?;
    check(|allocation| Aligned {
        _allocation: allocation,
    })
}
