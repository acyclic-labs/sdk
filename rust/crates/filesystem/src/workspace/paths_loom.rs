use super::*;
use std::cell::Cell;

loom::thread_local! {
    static COPIED: Cell<u64> = Cell::new(0);
}
pub(super) fn completed(bytes: u64) {
    COPIED.with(|copied| copied.set(copied.get() + bytes));
    loom::thread::yield_now();
}

fn copy(work: &crate::NativeMaterializationWork) -> u64 {
    let mut accounting = KernelAccounting::native(work, work.cancellation());
    let result = restore_absolute_path("abc", &mut accounting, work.cancellation());
    if let Ok(value) = result {
        assert_eq!(value.value, "/abc");
        value.retire(&mut accounting).unwrap();
    }
    COPIED.with(Cell::get)
}

#[test]
fn concurrent_path_copies_share_one_allowance() {
    loom::model(|| {
        let cancellation = CancellationToken::new();
        let root = crate::NativeMaterializationWork::new(
            crate::WorkBudget {
                bytes_copied: 4,
                ..crate::WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .unwrap();
        let work = root.clone();
        let worker = loom::thread::spawn(move || copy(&work));
        let copied = copy(&root) + worker.join().unwrap();
        assert_eq!(copied, 4, "concurrent path copies overspent shared credit");
        assert_eq!(root.receipt().bytes_copied, copied);
    });
}
