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
