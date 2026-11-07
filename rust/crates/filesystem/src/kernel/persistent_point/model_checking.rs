use super::*;
use crate::kernel::allocation::effects::OMIT_RESERVATION;

fn push<const BROKEN: bool>(
    mut values: Owned<Vec<u8>>,
    work: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> usize {
    OMIT_RESERVATION.with(|flag| flag.set(BROKEN));
    let mut accounting = KernelAccounting::native(work, cancellation);
    if let Err(error) = values.push(7, &mut accounting) {
        assert!(matches!(
            error,
            crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(
                crate::WorkError::BudgetExceeded { .. }
            ))
        ));
    }
    let completed = values.value.len();
    values.retire(&mut accounting).unwrap();
    completed
}

fn race<const BROKEN: bool>(maximum: u64, resource: bool) {
    loom::model(move || {
        let cancellation = CancellationToken::new();
        let budget = WorkBudget {
            bytes_copied: maximum,
            ..WorkBudget::UNBOUNDED
        };
        let original = crate::NativeMaterializationWork::new(
            if resource {
                WorkBudget::UNBOUNDED
            } else {
                budget
            },
            &cancellation,
        )
        .unwrap();
        let work = if resource {
            original.native_volume_work(budget).unwrap()
        } else {
            original.clone()
        };
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&work, &cancellation);
        let first = Owned::<Vec<u8>>::allocate(1, &mut accounting).unwrap();
        let second = Owned::<Vec<u8>>::allocate(1, &mut accounting).unwrap();
        let worker_work = work.clone();
        let worker_cancel = cancellation.clone();
        let worker =
            loom::thread::spawn(move || push::<BROKEN>(first, &worker_work, &worker_cancel));
        let completed = push::<BROKEN>(second, &work, &cancellation) + worker.join().unwrap();
        assert!(
            completed as u64 <= maximum,
            "pushes exceeded original credit"
        );
        assert_eq!(completed as u64, maximum);
        assert_eq!(original.receipt().bytes_copied, completed as u64);
        assert_eq!(original.live_allocation_bytes(), baseline);
    });
}

#[test]
fn concurrent_pushes_reserve_the_original_copy_allowance() {
    for maximum in [0, 1, 2] {
        race::<false>(maximum, false);
    }
    race::<false>(1, true);
    let panic = std::panic::catch_unwind(|| race::<true>(1, false))
        .expect_err("check-only admission must exceed the original allowance");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("pushes exceeded original credit"),
        "{message}"
    );
}
