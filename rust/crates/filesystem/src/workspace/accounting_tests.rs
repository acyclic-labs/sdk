use super::*;
use crate::facade::FsWork;
use crate::kernel::allocation::retained::Retained;
use crate::{Fs, WorkCounters};
use std::sync::Mutex;

pub(super) struct Prefunded {
    pub(super) budget: WorkBudget,
    pub(super) held: Mutex<Option<(Retained<Vec<u8>>, u64)>>,
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
async fn generation_diff_keeps_original_live_backing_and_read_refusal()
-> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("original-diff", budget, &cancellation)
        .await?
        .value;
    let before = workspace.head(budget, &cancellation).await?.value;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), budget, &cancellation)
        .await?
        .value;
    transaction
        .write(
            "/changed",
            acyclic_stream::OwnedBytes::from_static(b"original bytes"),
            budget,
            &cancellation,
        )
        .await?;
    let committed = transaction
        .commit::<WorkspaceError, _, _>(
            None,
            crate::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    let TransactionCommit::Committed(after) = committed else {
        return Err("original diff fixture did not commit".into());
    };
    for reads in [0, budget.backend_read_operations] {
        let source = Prefunded {
            budget: WorkBudget {
                backend_read_operations: reads,
                ..budget
            },
            held: Mutex::new(None),
        };
        let (result, spent, live, baseline) =
            before.diff_to(&after, 64, &source, &cancellation).await;
        spent.verify(source.budget)?;
        if reads == 0 {
            assert!(matches!(
                result,
                Err(WorkspaceError::Filesystem(FsError::Work(
                    crate::WorkError::BudgetExceeded {
                        counter: "backend_read_operations",
                        ..
                    }
                )))
            ));
            assert_eq!(spent.items_examined, 1);
            assert_eq!(spent.backend_read_operations, 0);
            assert_eq!(live, baseline);
        } else {
            let (changes, _allocation) = result?;
            assert_eq!(changes.work(), spent);
            assert!(!changes.changes().files.is_empty());
            assert!(!changes.changes().bindings.is_empty());
            assert!(live > baseline, "diff backing left the original ledger");
            assert!(spent.peak_allocation_bytes > baseline);
            let paths = changes
                .changed_paths(64, budget, &cancellation)
                .await?
                .value
                .0;
            assert_eq!(paths.len(), 1);
            assert_eq!(namespace_path_text(&paths[0].path)?, "/changed");
        }
    }
    Ok(())
}
