use super::storage::{adopt_backing, charge, map_bytes, release_backing, scan_work};
use super::{MAXIMUM_TRACKED_KEYS, MountSourceError, ViewStamp};
use crate::demand::NativeMountAllocationCustody;
use crate::native_mount::adapter::NativeDemandCarrier;
use loom::sync::{Arc as ModelArc, RwLock};
use std::collections::HashMap;
use std::sync::{Arc, PoisonError};

#[path = "positions.rs"]
mod positions;

fn work() -> crate::demand::DemandWork {
    crate::demand::DemandWork::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
}

#[test]
fn concurrent_growth_preserves_both_keys_and_newest_publication() {
    crate::initialize_test_native_io();
    loom::model(|| {
        let map = ModelArc::new(RwLock::new(positions::ChangePositions::default()));
        let other = ModelArc::clone(&map);
        let writer = loom::thread::spawn(move || {
            let mut work = work();
            let mut carrier = NativeDemandCarrier::Borrowed(&mut work);
            assert!(positions::record(&other, 1, ViewStamp(2), &mut carrier).is_ok());
            assert!(positions::record(&other, 2, ViewStamp(2), &mut carrier).is_ok());
        });
        let mut work = work();
        let mut carrier = NativeDemandCarrier::Borrowed(&mut work);
        assert!(positions::record(&map, 1, ViewStamp(1), &mut carrier).is_ok());
        assert!(writer.join().is_ok());
        assert!(!positions::unchanged_since(&map, ViewStamp(1), [1]));
        assert!(!positions::unchanged_since(&map, ViewStamp(1), [2]));
        assert!(positions::unchanged_since(&map, ViewStamp(2), [1, 2]));
        assert!(positions::unchanged_since(&map, ViewStamp(0), [3]));
    });
}
