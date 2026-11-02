use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn count_body_credit_covers_the_actual_owner_allocation_and_last_slice(
        length in 1usize..4096,
    ) {
        for complete_control in [true, false] {
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(100_000, 4_000_000),
            &acyclic_fs::CancellationToken::new(),
        ).unwrap();
        let baseline = work.live_allocation_bytes();
        // Generated SDK payload; the allocator observes Bytes' real owner,
        // independently of the ledger's retained-capacity calculation.
        let bytes = vec![b'c'; length];
        let capacity = bytes.capacity() as u64;
        let control = crate::http::owned_bytes::control_bytes::<Body>().unwrap();
        // The negative restores the original payload-plus-owner claim, which
        // omits Bytes' refcount. Both paths construct the same real allocation.
        let credited = capacity + if complete_control { control } else { std::mem::size_of::<Body>() as u64 };
        let mut custody = work.claim(credited, 1,
            acyclic_fs::WorkCounters::UNCHARGED).unwrap();
        crate::native::retain(&mut custody, credited).unwrap();
        let owner = Body { bytes, _custody: Some(custody) };
        let mut body = None;
        let allocated = acyclic_test_support::measure(|| {
            body = Some(bytes::Bytes::from_owner(owner));
        });
        prop_assert_eq!(allocated.count_total, 1);
        prop_assert_eq!(allocated.bytes_total, control);
        prop_assert_eq!(work.live_allocation_bytes() - baseline, credited);
        prop_assert_eq!(credited == capacity + allocated.bytes_total, complete_control);
        let body = body.unwrap();
        let retained = body.slice(length - 1..);
        drop(body);
        prop_assert_eq!(retained.as_ref(), b"c");
        prop_assert_eq!(work.live_allocation_bytes() - baseline, credited);
        drop(retained);
        prop_assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }
}
