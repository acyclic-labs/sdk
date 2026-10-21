#[cfg(not(target_arch = "wasm32"))]
use super::*;

std::thread_local! {
    static SKIP: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

// Negative control: charges before discovering the transfer cannot happen.
pub(super) fn skip_precheck() -> bool {
    SKIP.with(std::cell::Cell::get)
}

#[cfg(not(target_arch = "wasm32"))]
type Push = Transaction<crate::memory::MemoryAuthorityStore, crate::memory::MemoryObjectStore>;

#[cfg(not(target_arch = "wasm32"))]
// Funding that cannot move into the list (another ledger) is rejected before
// the copy is charged, so a failed push records no work and changes nothing.
fn foreign_push(skip: bool) -> (bool, u64, usize) {
    let cancellation = CancellationToken::new();
    let home =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let foreign =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let mut accounting = KernelAccounting::native(&home, &cancellation);
    let mut list = crate::kernel::Owned::<Vec<u64>>::allocate(1, &mut accounting).unwrap();
    let mut other = KernelAccounting::native(&foreign, &cancellation);
    let allocation = other.claim_owned(8, 1).unwrap();
    let before = home.receipt().bytes_copied;
    SKIP.with(|flag| flag.set(skip));
    let pushed = Push::push_mutation(
        &mut list,
        crate::kernel::Owned::new(7_u64, allocation, 8),
        &mut accounting,
    );
    SKIP.with(|flag| flag.set(false));
    let charged = home.receipt().bytes_copied - before;
    let len = list.value.len();
    list.retire(&mut accounting).unwrap();
    (pushed.is_err(), charged, len)
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn untransferable_funding_fails_before_the_copy_is_charged() {
    assert_eq!(foreign_push(false), (true, 0, 0));
    // Without the precheck the same push fails after charging the copy.
    assert_eq!(foreign_push(true), (true, 8, 0));
}
