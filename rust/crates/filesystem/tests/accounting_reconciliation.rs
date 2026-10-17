//! Original-ledger charge and concurrent retirement regression coverage.
#![cfg(all(feature = "memory", not(target_arch = "wasm32")))]

use acyclic_fs::model::{CheckoutMode, GenerationSelector};
use acyclic_fs::performance::BOUNDARY_BUDGET;
use acyclic_fs::{CancellationToken, Fs, NativeMaterializationWork, WorkCounters};

#[tokio::test]
async fn unchanged_checkout_charges_and_cross_thread_retirement_remain_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("reconcile", BOUNDARY_BUDGET, &cancellation)
        .await?
        .value;
    let work = NativeMaterializationWork::new(BOUNDARY_BUDGET, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut checkouts = Vec::new();
    for _ in 0..4 {
        checkouts.push(
            workspace
                .checkout(
                    GenerationSelector::Head,
                    CheckoutMode::read_only_pinned(),
                    &work,
                    &cancellation,
                )
                .await?,
        );
    }
    let delta = WorkCounters {
        items_examined: 1,
        ..WorkCounters::UNCHARGED
    };
    let mut expected = work.receipt();
    for _ in 0..64 {
        work.charge(delta)?;
        expected.items_examined += 1;
        assert_eq!(work.receipt(), expected);
    }
    let retained = work.live_allocation_bytes();
    let checkout = checkouts.pop().ok_or("missing retained checkout")?;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let retiring = std::sync::Arc::clone(&barrier);
    let retirement = std::thread::spawn(move || {
        retiring.wait();
        drop(checkout);
    });
    barrier.wait();
    for _ in 0..64 {
        work.charge(delta)?;
        expected.items_examined += 1;
    }
    retirement
        .join()
        .map_err(|_| std::io::Error::other("checkout retirement panicked"))?;
    work.charge(WorkCounters::UNCHARGED)?;
    assert!(work.live_allocation_bytes() < retained);
    assert_eq!(work.receipt(), expected);
    drop(checkouts);
    work.charge(WorkCounters::UNCHARGED)?;
    assert_eq!(
        work.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
    assert_eq!(work.receipt(), expected);
    let mut budget = BOUNDARY_BUDGET;
    budget.items_examined = 0;
    let denied =
        NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
    assert!(denied.charge(delta).is_err());
    assert_eq!(denied.receipt().items_examined, 0);
    cancellation.cancel();
    assert!(work.charge(delta).is_err());
    Ok(())
}
