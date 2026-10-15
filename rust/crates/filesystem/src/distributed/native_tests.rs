use super::*;
use crate::NativeMaterializationWork;

type TestResult = Result<(), Box<dyn std::error::Error>>;
const BUDGET: WorkBudget = crate::performance::BOUNDARY_BUDGET;

#[test]
fn meter_metadata_is_admitted_once_and_released_with_operation() -> TestResult {
    let cancellation = CancellationToken::new();
    let original = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
    let baseline = original.live_allocation_bytes();
    let before = original.receipt();
    let mut work = AuthorityWork::native(&original, &cancellation);
    let first = work.native_meter()?;
    let admitted = original.receipt();
    assert_eq!(
        admitted.allocation_operations,
        before.allocation_operations + 1
    );
    assert!(original.live_allocation_bytes() > baseline);
    for _ in 0..16 {
        let reused = work.native_meter()?;
        assert!(Arc::ptr_eq(&first, &reused));
        assert_eq!(original.receipt(), admitted);
    }
    drop(first);
    drop(work);
    assert_eq!(original.live_allocation_bytes(), baseline);
    assert_eq!(original.receipt(), admitted);
    Ok(())
}

#[test]
fn rebinding_original_cannot_reuse_another_ledger_or_resource() -> TestResult {
    let cancellation = CancellationToken::new();
    let first = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
    let second = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
    let denied = second.native_volume_work(WorkBudget {
        allocation_operations: 0,
        ..BUDGET
    })?;
    let mut work = AuthorityWork::native(&first, &cancellation);
    let initial = work.native_meter()?;
    work.original = Some(&second);
    let rebound = work.native_meter()?;
    assert!(!Arc::ptr_eq(&initial, &rebound));
    let first_spent = first.receipt();
    rebound.record_read(3)?;
    assert_eq!(first.receipt(), first_spent);
    assert_eq!(second.receipt().authority_bytes_read, 3);
    work.original = Some(&denied);
    let before = second.receipt();
    assert!(work.native_meter().is_err());
    assert!(work.native_read_meter.is_none());
    assert_eq!(second.receipt(), before);
    work.original = Some(&second);
    let retry = work.native_meter()?;
    assert!(!Arc::ptr_eq(&rebound, &retry));
    Ok(())
}

#[test]
fn reused_meter_preserves_authority_bytes_and_resource_checks() -> TestResult {
    let cancellation = CancellationToken::new();
    let original = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
    let resource = original.native_volume_work(WorkBudget {
        authority_bytes_read: 4,
        ..BUDGET
    })?;
    let mut work = AuthorityWork::native(&resource, &cancellation);
    let first = work.native_meter()?;
    first.record_read(3)?;
    let reused = work.native_meter()?;
    assert!(Arc::ptr_eq(&first, &reused));
    let before = original.receipt();
    assert!(reused.admit_read(2).is_err());
    assert_eq!(original.receipt(), before);
    reused.record_read(1)?;
    let mut expected = before;
    expected.authority_bytes_read += 1;
    assert_eq!(original.receipt(), expected);
    assert_eq!(resource.remaining()?.authority_bytes_read, 0);
    assert_eq!(original.receipt().source_bytes_read, 0);
    Ok(())
}

#[test]
fn reused_meter_checks_current_original_and_invocation_cancellation() -> TestResult {
    for cancel_original in [false, true] {
        let cancellation = CancellationToken::new();
        let original = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
        let (invocation, invocation_cancel) = original.invocation_view()?;
        let mut work = AuthorityWork::native(&invocation, &cancellation);
        let meter = work.native_meter()?;
        if cancel_original {
            cancellation.cancel();
        } else {
            invocation_cancel.cancel();
        }
        let before = original.receipt();
        assert!(work.native_meter().is_err());
        assert!(meter.check().is_err());
        assert_eq!(original.receipt(), before);
    }
    Ok(())
}

#[test]
fn failed_metadata_admission_keeps_the_operation_retryable() -> TestResult {
    let cancellation = CancellationToken::new();
    let denied = NativeMaterializationWork::new(
        WorkBudget {
            allocation_operations: 1,
            ..BUDGET
        },
        &cancellation,
    )
    .map_err(|(e, _)| e)?;
    let allowed = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
    let mut work = AuthorityWork::native(&denied, &cancellation);
    let before = denied.receipt();
    for _ in 0..2 {
        assert!(work.native_meter().is_err());
        assert!(work.native_read_meter.is_none());
        assert_eq!(denied.receipt(), before);
        assert_eq!(
            denied.live_allocation_bytes(),
            NativeMaterializationWork::accounting_state_bytes()
        );
    }
    work.original = Some(&allowed);
    assert!(work.native_meter().is_ok());
    Ok(())
}
