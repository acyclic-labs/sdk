use super::*;
use proptest::prelude::*;

fn cancel_binding_wait(
    wrapped: bool,
    lifetime: bool,
) -> Result<(crate::WorkCounters, u64), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    let writer = source
        .runtime
        .wait(|| async { Ok(source.checkout.view_gate.write().await) })?;
    let cancellation = CancellationToken::new();
    let work = capture_meter(WorkBudget::UNBOUNDED, &cancellation)?;
    let initial = work.live_allocation_bytes();
    let cancelled = if lifetime {
        source.cancellation().clone()
    } else {
        cancellation.clone()
    };
    let pending = std::rc::Rc::new(std::cell::Cell::new(false));
    let observed = std::rc::Rc::clone(&pending);
    let waiting_work = work.clone();
    super::super::runtime::BEFORE_PARK.with_borrow_mut(|hook| {
        assert!(hook.is_none());
        *hook = Some(Box::new(move || {
            // A held writer forces the real gate future to register its waits
            // before returning Pending. Cancel at that exact poll boundary.
            assert!(waiting_work.live_allocation_bytes() > initial);
            observed.set(true);
            cancelled.cancel();
        }));
    });
    let owner = ViewGate::callback_owner()?;
    let result = if wrapped {
        source.binding_lease(None, &work).map(drop)
    } else {
        let mut carrier = NativeDemandCarrier::Native(&work);
        source.runtime.wait(|| async {
            source
                .checkout
                .view_gate
                .read_for_callback_with_carrier(
                    owner,
                    None,
                    &mut carrier,
                    work.cancellation(),
                    Some(source.cancellation()),
                )
                .await
                .map(drop)
        })
    };
    assert!(pending.get());
    assert!(if lifetime {
        matches!(result, Err(MountSourceError::Stale))
    } else {
        matches!(result, Err(MountSourceError::Cancelled))
    });
    assert!(!source.checkout.view_gate.reads_for_callback(owner));
    assert_eq!(work.live_allocation_bytes(), initial);
    drop(writer);
    Ok((work.receipt(), work.live_allocation_bytes()))
}

#[test]
fn cancelled_binding_wait_releases_its_waker_and_registrations()
-> Result<(), Box<dyn std::error::Error>> {
    for lifetime in [false, true] {
        let (reference, retained) = cancel_binding_wait(false, lifetime)?;
        let (mut actual, live) = cancel_binding_wait(true, lifetime)?;
        // Cancellation happens before a returned lease can be allocated.
        assert_eq!(
            actual.allocation_operations,
            reference.allocation_operations + 1
        );
        assert_eq!(live, retained);
        actual.allocation_operations = reference.allocation_operations;
        actual.peak_allocation_bytes = reference.peak_allocation_bytes;
        assert_eq!(actual, reference);
    }
    Ok(())
}

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
