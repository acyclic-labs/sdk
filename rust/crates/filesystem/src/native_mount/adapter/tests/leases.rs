use super::*;
use proptest::prelude::*;

#[test]
fn binding_lease_only_allocates_its_waker_and_owned_payload()
-> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    let owner = ViewGate::callback_owner()?;
    let (reference, retained) = {
        let work = capture_meter(WorkBudget::UNBOUNDED, source.cancellation())?;
        let mut carrier = NativeDemandCarrier::Native(&work);
        let lease = source.runtime.wait(|| {
            source.checkout.view_gate.read_for_callback_with_carrier(
                owner,
                None,
                &mut carrier,
                work.cancellation(),
                Some(source.cancellation()),
            )
        })?;
        drop(lease);
        (work.receipt(), work.live_allocation_bytes())
    };
    let work = capture_meter(WorkBudget::UNBOUNDED, source.cancellation())?;
    let lease = source.binding_lease(None, &work)?;
    drop(lease);
    let mut actual = work.receipt();
    assert_eq!(
        actual.allocation_operations,
        reference.allocation_operations + 2
    );
    assert_eq!(work.live_allocation_bytes(), retained);
    // The gate is the work oracle: only temporary allocation ownership differs.
    actual.allocation_operations = reference.allocation_operations;
    actual.peak_allocation_bytes = reference.peak_allocation_bytes;
    assert_eq!(actual, reference);
    Ok(())
}

#[test]
fn binding_lease_respects_allocation_ceilings_and_releases_on_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    let owner = ViewGate::callback_owner()?;
    let required = {
        let work = capture_meter(WorkBudget::UNBOUNDED, source.cancellation())?;
        drop(source.binding_lease(None, &work)?);
        work.receipt()
    };
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 64,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    runner.run(
        &(
            0..=required.allocation_operations + 1,
            prop_oneof![
                0..=required.peak_allocation_bytes + 1,
                Just(required.peak_allocation_bytes),
            ],
        ),
        |(operations, bytes)| {
            let budget = WorkBudget {
                allocation_operations: operations,
                peak_allocation_bytes: bytes,
                ..WorkBudget::UNBOUNDED
            };
            let succeeds = operations >= required.allocation_operations
                && bytes >= required.peak_allocation_bytes;
            let work = match crate::NativeMaterializationWork::new(budget, source.cancellation()) {
                Ok(work) => work,
                Err((_, receipt)) => {
                    prop_assert!(!succeeds);
                    prop_assert!(receipt.verify(budget).is_ok());
                    return Ok(());
                }
            };
            let lease = source.binding_lease(None, &work);
            prop_assert_eq!(lease.is_ok(), succeeds);
            drop(lease);
            prop_assert!(work.receipt().verify(budget).is_ok());
            // Failure must not leave a read lease behind on this callback owner.
            prop_assert!(!source.checkout.view_gate.reads_for_callback(owner));
            Ok(())
        },
    )?;
    Ok(())
}
