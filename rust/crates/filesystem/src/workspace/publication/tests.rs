use super::*;
use crate::facade::FsWork;
use crate::kernel::allocation::retained::Retained;
use crate::{Fs, WorkCounters};
use std::sync::Mutex;

struct Prefunded {
    budget: WorkBudget,
    held: Mutex<Option<(Retained<Vec<u8>>, u64)>>,
}

impl FsWork for &Prefunded {
    type Output<T, E> = (Result<T, E>, WorkCounters, u64, u64);
    type OwnedOutput<T> = Result<(T, KernelOwnedAllocation), FsError>;

    fn finish_owned<T>(
        &self,
        result: Result<(T, KernelOwnedAllocation), FsError>,
        _: &KernelAccounting<'_>,
    ) -> Self::OwnedOutput<T> {
        result
    }

    fn accounting<'a>(&'a self, cancellation: &'a CancellationToken) -> KernelAccounting<'a> {
        let mut accounting = KernelAccounting::standalone(self.budget, cancellation);
        let buffer = crate::kernel::Owned::<Vec<u8>>::allocate(4096, &mut accounting).unwrap();
        let held = Retained::from_owned(buffer, &mut accounting).unwrap();
        accounting.charge_items(1).unwrap();
        let baseline = accounting.standalone_parts().unwrap().0.live_bytes();
        *self.held.lock().unwrap() = Some((held, baseline));
        accounting
    }

    fn finish<T, E>(
        &self,
        result: Result<T, E>,
        accounting: &KernelAccounting<'_>,
    ) -> Self::Output<T, E> {
        let KernelAccounting::Standalone { allocations, .. } = accounting else {
            unreachable!("ordinary fixture")
        };
        let baseline = self.held.lock().unwrap().as_ref().unwrap().1;
        (
            result,
            accounting.spent(),
            allocations.live_bytes(),
            baseline,
        )
    }
}

#[tokio::test]
async fn reservation_preserves_original_live_peak_and_refuses_before_publication()
-> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    for writes in [0, 4] {
        let fs = Fs::memory();
        let workspace = fs
            .create_workspace(
                "original-reservation",
                crate::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await?
            .value;
        let source = Prefunded {
            budget: WorkBudget {
                backend_write_operations: writes,
                ..crate::performance::BOUNDARY_BUDGET
            },
            held: Mutex::new(None),
        };
        let operation = OperationId::new();
        let (result, spent, live, baseline) = workspace
            .reserve_publication(operation, &source, &cancellation)
            .await;
        spent.verify(source.budget)?;
        if writes == 0 {
            assert!(matches!(
                result,
                Err(WorkspaceError::Filesystem(FsError::Authority(
                    crate::AuthorityStoreError::Work(crate::WorkError::BudgetExceeded {
                        counter: "backend_write_operations",
                        ..
                    })
                )))
            ));
            assert_eq!(live, baseline);
            assert_eq!(spent.backend_write_operations, 0);
            // A refused operation must leave the original reservation available.
            let reserved = workspace
                .reserve_publication(
                    OperationId::new(),
                    crate::performance::BOUNDARY_BUDGET,
                    &cancellation,
                )
                .await?
                .value;
            workspace
                .release_publication(reserved, crate::performance::BOUNDARY_BUDGET, &cancellation)
                .await?;
        } else {
            let reservation = result?;
            assert_eq!(reservation.operation_id, operation);
            assert_eq!(spent.items_examined, 1);
            assert_eq!(spent.backend_read_operations, 2);
            assert_eq!(spent.backend_write_operations, 1);
            assert!(
                live > baseline,
                "reservation backing left the original ledger"
            );
            assert!(spent.peak_allocation_bytes > baseline);
            let (result, released, release_live, release_baseline) = workspace
                .release_publication(reservation, &source, &cancellation)
                .await;
            result?;
            released.verify(source.budget)?;
            assert_eq!(released.items_examined, 1);
            assert_eq!(released.backend_read_operations, 1);
            assert_eq!(released.backend_write_operations, 1);
            assert_eq!(release_live, release_baseline);
        }
    }
    Ok(())
}
