use super::*;
use crate::{CancellationToken, source_watch::CallbackWork};

#[test]
fn shared_demand_claims_and_cleanup_keep_one_leaf_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let root = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let resource = root.native_volume_work(WorkBudget {
        backend_read_operations: 0,
        ..WorkBudget::UNBOUNDED
    })?;
    resource.with_original_leaf(|work| {
        // The generic ordinary path must enforce the same resource ceiling.
        assert!(matches!(
            NativeHostWork::admit(work, READ),
            Err(DemandError::Work(_))
        ));
    });

    fn exercise<W: DemandWorkContext>(
        work: &mut W,
        owner: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), DemandError> {
        let (allocation, locks) = owner.state.count_locks(|| work.claim(8));
        let allocation = allocation?;
        assert_eq!(locks, 1);
        assert!(matches!(work.claim(1), Err(DemandError::Work(_))));
        work.release(allocation)?;
        let reservation = work.reserve_cleanup_write()?;
        cancellation.cancel();
        let (claim, locks) = owner.state.count_locks(|| work.claim(1));
        assert!(matches!(claim, Err(DemandError::Cancelled)));
        assert_eq!(locks, 1);
        owner.with_original_leaf(|_| assert!(owner.state.0.try_lock().is_err()));
        let (unlocked, recorded) = work.attempt_cleanup_write(reservation, || {
            // A regression fails immediately instead of deadlocking on re-entry.
            owner.state.0.try_lock().is_ok()
        });
        assert!(unlocked);
        recorded?;
        assert_eq!(owner.receipt().backend_write_operations, 1);
        Ok(())
    }

    for callback in [false, true] {
        let cancellation = CancellationToken::new();
        let budget = WorkBudget {
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + 8,
            backend_write_operations: 1,
            ..WorkBudget::UNBOUNDED
        };
        let mut work =
            NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
        let owner = work.clone();
        if callback {
            exercise(&mut CallbackWork::Original(work), &owner, &cancellation)?;
        } else {
            exercise(&mut work, &owner, &cancellation)?;
        }
    }
    Ok(())
}
