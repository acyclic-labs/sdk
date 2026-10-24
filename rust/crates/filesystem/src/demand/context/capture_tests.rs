use super::*;
use acyclic_test_support::{AllocationInfo, measure};
use proptest::prelude::*;
use std::cell::Cell;

thread_local! {
    static PAYLOAD_ONLY: Cell<bool> = const { Cell::new(false) };
}

pub(super) fn claimed_bytes() -> u64 {
    if PAYLOAD_ONLY.get() {
        std::mem::size_of::<CaptureTransientCustody>() as u64
    } else {
        CaptureTransientCustody::ALLOCATION_BYTES
    }
}

fn exact_backing() {
    let work = crate::NativeMaterializationWork::new(
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .unwrap();
    let before = work.live_allocation_bytes();
    let mut capture = None;
    let heap = measure(|| {
        capture = Some(CaptureTransientCustody::new_native(&work).unwrap());
    });
    let bytes = CaptureTransientCustody::ALLOCATION_BYTES;
    assert_eq!(
        heap,
        AllocationInfo {
            count_total: 1,
            count_current: 1,
            count_max: 1,
            bytes_total: bytes,
            bytes_current: bytes as i64,
            bytes_max: bytes,
        }
    );
    assert_eq!(
        work.live_allocation_bytes(),
        before + bytes,
        "full capture backing funded"
    );
    drop(capture);
    assert_eq!(work.live_allocation_bytes(), before);
}

#[test]
fn capture_backing_is_one_fully_funded_allocation_and_payload_only_is_detected() {
    exact_backing();
    PAYLOAD_ONLY.set(true);
    let error = std::panic::catch_unwind(exact_backing);
    PAYLOAD_ONLY.set(false);
    let error = error.expect_err("payload-only funding must fail the same bound");
    let text = error.downcast_ref::<String>().expect("assertion message");
    assert!(text.contains("full capture backing funded"), "{text}");
}

fn header_limit() {
    let stop = crate::CancellationToken::new();
    let budget = WorkBudget {
        peak_allocation_bytes: crate::NativeMaterializationWork::accounting_state_bytes()
            + CaptureTransientCustody::ALLOCATION_BYTES
            - 1,
        ..WorkBudget::UNBOUNDED
    };
    let work = crate::NativeMaterializationWork::new(budget, &stop).unwrap();
    let before = work.receipt();
    let mut result = None;
    let heap = measure(|| result = Some(CaptureTransientCustody::new_native(&work)));
    assert_eq!(
        matches!(result.unwrap(), Err(crate::NativeTreeMaterializationError::Work(
        crate::WorkError::BudgetExceeded { counter: "peak_allocation_bytes", observed, maximum }
    )) if observed == budget.peak_allocation_bytes + 1 && maximum == budget.peak_allocation_bytes),
        true,
        "capture header refused before physical allocation"
    );
    assert_eq!(work.receipt(), before);
    assert_eq!(
        work.live_allocation_bytes(),
        crate::NativeMaterializationWork::accounting_state_bytes()
    );
    assert_eq!(heap, AllocationInfo::default());
}

#[test]
fn capture_header_limit_is_checked_before_the_physical_allocation() {
    header_limit();
    PAYLOAD_ONLY.set(true);
    let error = std::panic::catch_unwind(header_limit);
    PAYLOAD_ONLY.set(false);
    let error = error.expect_err("payload-only funding must admit an oversized backing");
    let text = error.downcast_ref::<String>().expect("assertion message");
    assert!(
        text.contains("capture header refused before physical allocation"),
        "{text}"
    );
}

fn native_scope(
    mode: u8,
) -> (
    crate::NativeMaterializationWork,
    crate::NativeMaterializationWork,
    crate::CancellationToken,
) {
    let stop = crate::CancellationToken::new();
    let root = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &stop).unwrap();
    let view = match mode {
        1 => root.native_volume_work(WorkBudget::UNBOUNDED).unwrap(),
        2 => root
            .native_volume_work(WorkBudget::UNBOUNDED)
            .unwrap()
            .native_volume_work(WorkBudget::UNBOUNDED)
            .unwrap(),
        3 => root
            .invocation_view()
            .unwrap()
            .0
            .native_volume_work(WorkBudget::UNBOUNDED)
            .unwrap()
            .native_volume_work(WorkBudget::UNBOUNDED)
            .unwrap(),
        _ => root.clone(),
    };
    (root, view, stop)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Property: any surviving allocation meter funds the complete origin after
    // close, and its final destruction retires both claims exactly once.
    #[test]
    fn closed_capture_meters_retain_funding_without_delaying_scope_shutdown(
        bytes in 0_u64..4096, cancelled in any::<bool>(),
    ) {
        for mode in 0..4 {
            let (work, view, stop) = native_scope(mode);
            let (reference, _reference_view, reference_stop) = native_scope(mode);
            let before = work.live_allocation_bytes();
            let mut expected = work.receipt();
            let capture = CaptureTransientCustody::new_native(&view).unwrap();
            expected.allocation_operations += 1;
            expected.peak_allocation_bytes = expected.peak_allocation_bytes.max(before + CaptureTransientCustody::ALLOCATION_BYTES);
            prop_assert_eq!(work.receipt(), expected);
            let bound = view.with_capture_custody(&capture).unwrap();
            let owner = bound.claim(bytes, u64::from(bytes != 0), WorkCounters::UNCHARGED).unwrap();
            if cancelled {
                stop.cancel();
                reference_stop.cancel();
            }
            // Cancellation may retire its own linked-child metadata. The
            // independently initialized reference isolates that standing cost.
            let standing = reference.live_allocation_bytes();
            capture.close().unwrap();
            prop_assert!(bound.with_original_leaf(|state| state.check()).is_err());
            drop(capture);
            drop(bound);
            prop_assert_eq!(work.live_allocation_bytes(), standing + CaptureTransientCustody::ALLOCATION_BYTES + bytes);
            drop(owner);
            prop_assert_eq!(work.live_allocation_bytes(), standing);
        }
    }
}

fn origin_backing(legacy_weak: bool) {
    let work = crate::NativeMaterializationWork::new(
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .unwrap();
    let before = work.live_allocation_bytes();
    let (mut strong, mut weak) = (None, None);
    let heap = measure(|| {
        let capture = CaptureTransientCustody::new_native(&work).unwrap();
        if legacy_weak {
            // Deliberately break the private handle boundary. Ordinary callers
            // cannot obtain this Arc or downgrade it.
            weak = Some(Arc::downgrade(&capture.0));
        } else {
            strong = Some(capture.clone());
        }
        drop(capture);
    });
    let bytes = CaptureTransientCustody::ALLOCATION_BYTES;
    assert_eq!(
        heap,
        AllocationInfo {
            count_total: 1,
            count_current: 1,
            count_max: 1,
            bytes_total: bytes,
            bytes_current: bytes as i64,
            bytes_max: bytes,
        }
    );
    assert_eq!(
        work.live_allocation_bytes(),
        before + bytes,
        "capture origin funds live backing"
    );
    drop((strong, weak));
    assert_eq!(work.live_allocation_bytes(), before);
}

#[test]
fn opaque_origins_fund_the_backing_and_weak_origins_fail_the_same_bound() {
    origin_backing(false);
    let error = std::panic::catch_unwind(|| origin_backing(true))
        .expect_err("a weak origin must leave physical backing unfunded");
    let text = error.downcast_ref::<String>().expect("assertion message");
    assert!(text.contains("capture origin funds live backing"), "{text}");
}
