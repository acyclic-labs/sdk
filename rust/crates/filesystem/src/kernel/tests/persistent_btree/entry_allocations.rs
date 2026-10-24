use super::*;
use crate::{CancellationToken, WorkBudget};
use proptest::prelude::*;

#[repr(align(64))]
struct Aligned([u8; 65]);

fn denied<const OLD: bool, T>(value: T, shortfall: u64) {
    let cancellation = CancellationToken::new();
    let exact = arc_control_bytes::<Entry<T>>().unwrap();
    let mut accounting = KernelAccounting::standalone(
        WorkBudget {
            peak_allocation_bytes: exact - shortfall.min(exact),
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    let input = Owned::new(value, KernelOwnedAllocation::Standalone, 0);
    let mut result = None;
    let physical = acyclic_test_support::measure(|| {
        result = Some(if OLD {
            share_with_control(
                input,
                &mut accounting,
                Ok(usize_to_u64(size_of::<Entry<T>>())),
            )
        } else {
            share(input, &mut accounting)
        });
    });
    let result = result.unwrap();
    assert!(result.is_err(), "entry admission omitted Arc control bytes");
    assert_eq!(physical.count_total, 0, "denied entry admission allocated");
    assert_eq!(physical.bytes_total, 0);
    assert_eq!(accounting.standalone_parts().unwrap().0.live_bytes(), 0);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn denied_entry_admission_allocates_nothing(shortfall in 1_u64..65) {
        denied::<false, _>(0_u8, shortfall);
        denied::<false, _>(Aligned([0; 65]), shortfall);
    }
}

#[test]
#[should_panic(expected = "entry admission omitted Arc control bytes")]
fn omitted_arc_header_is_detected_by_same_admission_oracle() {
    denied::<true, _>(0_u8, 1);
}

fn exact<T>(value: T) {
    let cancellation = CancellationToken::new();
    let bytes = arc_control_bytes::<Entry<T>>().unwrap();
    let mut accounting = KernelAccounting::standalone(
        WorkBudget {
            peak_allocation_bytes: bytes,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    let input = Owned::new(value, KernelOwnedAllocation::Standalone, 0);
    let mut shared = None;
    let physical = acyclic_test_support::measure(|| {
        shared = Some(share(input, &mut accounting).unwrap());
    });
    assert_eq!(physical.count_total, 1);
    assert_eq!(physical.bytes_total, bytes);
    assert_eq!(accounting.standalone_parts().unwrap().0.live_bytes(), bytes);
    shared.unwrap().retire(&mut accounting).unwrap();
    assert_eq!(accounting.standalone_parts().unwrap().0.live_bytes(), 0);
}

#[test]
fn entry_control_claim_matches_physical_layout_including_alignment() {
    exact(0_u8);
    let value = Aligned([0; 65]);
    assert_eq!(value.0.len(), 65);
    exact(value);
}
