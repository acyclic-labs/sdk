use super::*;
use crate::memory::{MemoryAuthorityStore, MemoryObjectStore};
use std::cell::Cell;

type Push = Transaction<MemoryAuthorityStore, MemoryObjectStore>;
const VALUE: u64 = std::mem::size_of::<u64>() as u64;

loom::thread_local! {
    static SKIP_FUNDING: Cell<bool> = Cell::new(false);
}

// Broken control: pushes the value but drops its funding.
pub(super) fn skip_funding_transfer() -> bool {
    SKIP_FUNDING.with(Cell::get)
}

fn funded(accounting: &mut KernelAccounting<'_>, value: u64) -> crate::kernel::Owned<u64> {
    let allocation = accounting.claim_owned(VALUE, 1).unwrap();
    crate::kernel::Owned::new(value, allocation, VALUE)
}

fn held(owned: &crate::kernel::Owned<Vec<u64>>) -> u64 {
    match &owned.allocation {
        KernelOwnedAllocation::NativeOriginal(allocation) => allocation.bytes(),
        KernelOwnedAllocation::Standalone => unreachable!("native model"),
    }
}

// A worker pushes two funded owners through the real push_mutation while the
// caller is cancelled, then optionally unwinds holding the list. Every
// pushed value keeps its funding, every rejected owner releases its own,
// copy work is charged exactly once per pushed value, and an unwind releases
// everything.
fn model<const SKIP: bool>(depth: u8, scoped: bool, unwind: bool) {
    loom::model(move || {
        let root_cancel = CancellationToken::new();
        let caller_cancel = CancellationToken::new();
        let root =
            crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &root_cancel)
                .unwrap();
        let mut work = root.clone();
        for _ in 0..depth {
            work = work
                .native_volume_work(crate::WorkBudget::UNBOUNDED)
                .unwrap();
        }
        let setup = root.live_allocation_bytes();
        let funding = CancellationToken::new();
        let mut accounting = KernelAccounting::native(&work, &funding);
        let list = crate::kernel::Owned::<Vec<u64>>::allocate(2, &mut accounting).unwrap();
        let owners = [funded(&mut accounting, 1), funded(&mut accounting, 2)];
        let (base_bytes, base_held) = (list.bytes, held(&list));
        let receipt = root.receipt().bytes_copied;
        let (worker_work, worker_cancel) = (work.clone(), caller_cancel.clone());
        let worker = loom::thread::spawn(move || {
            SKIP_FUNDING.with(|flag| flag.set(SKIP));
            let mut list = list;
            let mut accounting = KernelAccounting::native(&worker_work, &worker_cancel);
            for owner in owners {
                if Push::push_mutation(&mut list, owner, &mut accounting).is_err() {
                    break;
                }
                loom::thread::yield_now();
            }
            SKIP_FUNDING.with(|flag| flag.set(false));
            let pushed = (list.value.len() as u64, list.bytes, held(&list));
            if unwind {
                let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    let _held = list;
                    std::panic::resume_unwind(Box::new("unwind holding pushed mutations"));
                }));
                assert!(unwound.is_err());
                return (pushed, None);
            }
            (pushed, Some(list))
        });
        if scoped {
            root_cancel.cancel();
        } else {
            caller_cancel.cancel();
        }
        let ((pushed, bytes, funded), list) = worker.join().unwrap();
        assert_eq!(
            funded,
            base_held + pushed * VALUE,
            "pushed mutations stay funded"
        );
        assert_eq!(bytes, base_bytes + pushed * VALUE);
        assert_eq!(root.receipt().bytes_copied - receipt, pushed * VALUE);
        if let Some(list) = list {
            assert_eq!(
                root.live_allocation_bytes(),
                setup + base_held + pushed * VALUE,
                "rejected owners release their own funding"
            );
            list.retire(&mut KernelAccounting::native(&work, &funding))
                .unwrap();
        }
        assert_eq!(root.live_allocation_bytes(), setup);
    });
}

#[test]
fn pushed_mutations_keep_funding_through_cancellation_and_unwind() {
    model::<false>(0, false, false);
    model::<false>(2, false, false);
    model::<false>(2, true, false);
    model::<false>(1, false, true);
}

#[test]
fn dropping_the_funding_transfer_fails_the_funding_assertion() {
    let panic = std::panic::catch_unwind(|| model::<true>(0, false, false))
        .expect_err("a pushed value without its funding must be caught");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("pushed mutations stay funded"),
        "{message}"
    );
}
