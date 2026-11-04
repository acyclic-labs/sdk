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
    for mode in 0..6 {
        let observed = Arc::new(Meter::default());
        let meter = observed.original();
        let data = vec![42; 8];
        let pointer = data.as_ptr();
        let capacity = data.capacity();
        let owner = wrap(ReadAllocation::new(&meter, capacity)?);
        let mut alias = None;
        let bytes = match mode {
            0 => OwnedBytes::from_vec_with_guard(data, owner, meter)
                .map_err(|failure| format!("{:?}", failure.into_error()))?,
            1 => {
                let mut data = data;
                OwnedBytes::from_vec_with_guard_in_place(&mut data, &mut Some(owner), &meter)?
            }
            2 => PreparedOwnedBytes::new(data, owner, &meter)
                .map_err(|failure| format!("{:?}", failure.into_error()))?
                .freeze(8)
                .map_err(|_| "invalid initialized fixture")?,
            3 => OwnedBytes::from(data).with_guard(owner, meter.clone())?,
            4 => {
                let bytes = OwnedBytes::from(data);
                alias = Some(bytes.clone());
                bytes.with_guard(owner, meter.clone())?
            }
            _ => OwnedBytes::from(Bytes::from_owner(data)).with_guard(owner, meter.clone())?,
        };
        if let Some(alias) = alias {
            assert_eq!(bytes.backing(), alias.backing());
        }
        let header = observed.held.load(Ordering::SeqCst) - capacity as u64;
        let (size, alignment) = if mode < 4 {
            (
                std::mem::size_of::<GuardBundle<G>>(),
                std::mem::align_of::<GuardBundle<G>>(),
            )
        } else {
            (
                std::mem::size_of::<GuardBundle<G, RetainedBytes>>(),
                std::mem::align_of::<GuardBundle<G, RetainedBytes>>(),
            )
        };
        let minimum = (size + 2 * std::mem::size_of::<AtomicUsize>()) as u64;
        let alignment = alignment as u64;
        assert!(header >= minimum, "native byte header omitted Arc counters");
        assert_eq!(
            header % alignment,
            0,
            "native byte header omitted alignment padding"
        );
        assert!(header < minimum + alignment);
        assert_eq!(bytes.as_slice(), &[42; 8]);
        assert_eq!(bytes.as_ptr(), pointer);
        assert!(bytes.matches_meter(&observed.original()));
        let nested = bytes.slice(1..).with_guard((), observed.original())?;
        let nested = nested.with_guard((), observed.original())?;
        assert_eq!(nested.as_slice(), &[42; 7]);
        assert_eq!(nested.as_ptr(), pointer.wrapping_add(1));
        assert_eq!(nested.backing(), bytes.backing());
        assert!(nested.matches_meter(&observed.original()));
        drop(nested);
        assert_eq!(
            observed.held.load(Ordering::SeqCst),
            capacity as u64 + header
        );
        let selected = bytes.slice(1..3).slice(1..);
        let empty = bytes.slice(..0);
        drop(bytes);
        assert_eq!(selected.as_slice(), &[42]);
        assert_eq!(selected.as_ptr(), pointer.wrapping_add(2));
        drop(selected);
        assert_eq!(
            observed.held.load(Ordering::SeqCst),
            capacity as u64 + header
        );
        drop(empty);
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
