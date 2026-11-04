use super::*;
use crate::Fs;
use crate::workspace::accounting_tests::Prefunded;
use std::sync::Mutex;

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
            // The authority call spends the caller's original frame (#2111),
            // so its refusal is the caller's own work refusal.
            assert!(matches!(
                result,
                Err(WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                    counter: "backend_write_operations",
                    ..
                }))
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
            // The original frame records exactly the provider work an
            // identical fresh workspace reports, plus the caller's one
            // prefunded item: nothing is detached, lost or counted twice.
            let twin_fs = Fs::memory();
            let twin = twin_fs
                .create_workspace(
                    "original-reservation",
                    crate::performance::BOUNDARY_BUDGET,
                    &cancellation,
                )
                .await?
                .value;
            let plain = twin
                .reserve_publication(
                    OperationId::new(),
                    crate::performance::BOUNDARY_BUDGET,
                    &cancellation,
                )
                .await?;
            assert_eq!(spent.items_examined, plain.work.items_examined + 1);
            assert_eq!(
                spent.backend_read_operations,
                plain.work.backend_read_operations
            );
            assert_eq!(spent.backend_write_operations, 1);
            assert_eq!(plain.work.backend_write_operations, 1);
            let plain_release = twin
                .release_publication(
                    plain.value,
                    crate::performance::BOUNDARY_BUDGET,
                    &cancellation,
                )
                .await?
                .work;
            // The actual provider retains committed keys and record bodies.
            // Their original funding must survive the request, then retire
            // with that physical provider state rather than as scratch.
            let (reserved_backing, reserved_baseline) = source.held.lock().unwrap().take().unwrap();
            assert_eq!(reserved_baseline, baseline);
            assert_eq!(
                reserved_backing.allocation.standalone_live_bytes(),
                Some(live)
            );
            assert!(
                live > baseline,
                "resident reservation backing lost its funding"
            );
            assert!(spent.peak_allocation_bytes > baseline);
            let (result, released, release_live, release_baseline) = workspace
                .release_publication(reservation, &source, &cancellation)
                .await;
            result?;
            released.verify(source.budget)?;
            assert_eq!(released.items_examined, plain_release.items_examined + 1);
            assert_eq!(
                released.backend_read_operations,
                plain_release.backend_read_operations
            );
            assert_eq!(released.backend_write_operations, 1);
            assert_eq!(plain_release.backend_write_operations, 1);
            let (release_backing, released_baseline) = source.held.lock().unwrap().take().unwrap();
            assert_eq!(released_baseline, release_baseline);
            assert_eq!(
                release_backing.allocation.standalone_live_bytes(),
                Some(release_live)
            );
            assert!(
                release_live > release_baseline,
                "resident release backing lost its funding"
            );
            drop(workspace);
            drop(fs);
            assert_eq!(
                reserved_backing.allocation.standalone_live_bytes(),
                Some(baseline),
                "reservation backing survived its physical provider"
            );
            assert_eq!(
                release_backing.allocation.standalone_live_bytes(),
                Some(release_baseline),
                "release backing survived its physical provider"
            );
        }
    }
    Ok(())
}
