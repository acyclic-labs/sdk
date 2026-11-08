use super::*;
use acyclic_test_support::{AllocationInfo, measure};
use std::cell::Cell;

thread_local! {
    pub(super) static PAYLOAD_ONLY: Cell<bool> = const { Cell::new(false) };
}

fn exact_backing_mode(mode: u8) {
    let original = NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .unwrap();
    let mut view = match mode {
        1 => original
            .native_volume_work(crate::WorkBudget::UNBOUNDED)
            .unwrap(),
        2 => original
            .native_volume_work(crate::WorkBudget::UNBOUNDED)
            .unwrap()
            .native_volume_work(crate::WorkBudget::UNBOUNDED)
            .unwrap(),
        3 => original
            .invocation_view()
            .unwrap()
            .0
            .native_volume_work(crate::WorkBudget::UNBOUNDED)
            .unwrap()
            .native_volume_work(crate::WorkBudget::UNBOUNDED)
            .unwrap()
            .recheck_admitted(),
        _ => original.clone(),
    };
    let baseline = original.live_allocation_bytes();
    let mut expected = original.receipt();
    let mut custody = None;
    let heap = measure(|| {
        custody = Some(NativeMountAllocationCustody::new(&mut view).unwrap());
    });
    let bytes = NativeMountAllocationCustody::ALLOCATION_BYTES
        + NativeAllocationAccounting::ALLOCATION_BYTES;
    assert_eq!(
        heap,
        AllocationInfo {
            count_total: 2,
            count_current: 2,
            count_max: 2,
            bytes_total: bytes,
            bytes_current: bytes as i64,
            bytes_max: bytes,
        }
    );
    assert_eq!(
        original.live_allocation_bytes(),
        baseline + bytes,
        "full mount backing funded"
    );
    expected.allocation_operations += 2;
    expected.peak_allocation_bytes = expected.peak_allocation_bytes.max(baseline + bytes);
    assert_eq!(original.receipt(), expected);
    let spent = original.receipt();
    let mut clone = None;
    assert_eq!(
        measure(|| clone = Some(custody.as_ref().unwrap().clone())),
        AllocationInfo::default()
    );
    drop(custody);
    assert_eq!(original.live_allocation_bytes(), baseline + bytes);
    drop(clone);
    original.charge(WorkCounters::UNCHARGED).unwrap();
    assert_eq!(original.live_allocation_bytes(), baseline);
    assert_eq!(original.receipt(), spent);
}

fn exact_backing() {
    exact_backing_mode(0);
}

#[test]
fn mount_backing_is_fully_funded_and_cloning_is_free() {
    for mode in 0..4 {
        exact_backing_mode(mode);
    }
    PAYLOAD_ONLY.set(true);
    let error = std::panic::catch_unwind(exact_backing);
    PAYLOAD_ONLY.set(false);
    let error = error.expect_err("payload-only funding must fail the same bound");
    let text = error.downcast_ref::<String>().unwrap();
    assert!(text.contains("full mount backing funded"), "{text}");
}

fn final_backing(expose_weak: bool) {
    let mut original = NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .unwrap();
    let baseline = original.live_allocation_bytes();
    let mut weak = None;
    let heap = measure(|| {
        let custody = NativeMountAllocationCustody::new(&mut original).unwrap();
        if expose_weak {
            weak = Some(crate::sync::Arc::downgrade(&custody.0));
        }
        drop(custody);
        original.charge(WorkCounters::UNCHARGED).unwrap();
    });
    assert_eq!(
        original.live_allocation_bytes(),
        baseline + heap.bytes_current as u64,
        "all surviving mount backing remains funded"
    );
    drop(weak);
}

#[test]
fn opaque_custody_retires_backing_with_its_last_accounting_owner() {
    final_backing(false);
    let error = std::panic::catch_unwind(|| final_backing(true))
        .expect_err("exposing the private Arc must fail the same lifetime bound");
    let text = error.downcast_ref::<String>().unwrap();
    assert!(
        text.contains("all surviving mount backing remains funded"),
        "{text}"
    );
}

fn header_limit() {
    let budget = crate::WorkBudget {
        peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes()
            + NativeMountAllocationCustody::ALLOCATION_BYTES
            - 1,
        ..crate::WorkBudget::UNBOUNDED
    };
    let mut original =
        NativeMaterializationWork::new(budget, &crate::CancellationToken::new()).unwrap();
    let before = original.receipt();
    let mut result = None;
    assert_eq!(
        measure(|| result = Some(NativeMountAllocationCustody::new(&mut original))),
        AllocationInfo::default(),
        "mount header refused before physical allocation"
    );
    assert!(matches!(
        result.unwrap(),
        Err(DemandError::Work(crate::WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            ..
        }))
    ));
    assert_eq!(original.receipt(), before);
}

#[test]
fn custody_header_limit_precedes_any_physical_allocation() {
    header_limit();
    PAYLOAD_ONLY.set(true);
    let error = std::panic::catch_unwind(header_limit);
    PAYLOAD_ONLY.set(false);
    let error = error.expect_err("payload-only header must admit an unfunded allocation");
    let text = error.downcast_ref::<String>().unwrap();
    assert!(
        text.contains("mount header refused before physical allocation"),
        "{text}"
    );
}
