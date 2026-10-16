// Synchronous callback lock order: operation -> view state -> open file ->
// observer registry -> source cursor -> original demand leaf -> view gate counters.
// View gate counters never acquire the leaf. Resource allowances follow the leaf:
// ancestor resource -> descendant resource (materializer/allowance.rs).
pub(crate) const OPERATION: u16 = 10;
pub(crate) const VIEW: u16 = 20;
pub(crate) const FILE: u16 = 30;
pub(crate) const OBSERVERS: u16 = 35;
pub(crate) const CURSOR: u16 = 40;
pub(crate) const LEAF: u16 = 50;
pub(crate) const GATE: u16 = 60;

#[cfg(any(debug_assertions, test))]
mod checked;
#[cfg(any(debug_assertions, test))]
pub(crate) use checked::{Condvar, Mutex, MutexGuard, lock};

#[cfg(not(any(debug_assertions, test)))]
pub(crate) type Mutex<T, const RANK: u16> = crate::sync::Mutex<T>;
#[cfg(not(any(debug_assertions, test)))]
pub(crate) use crate::sync::{Condvar, MutexGuard};

#[cfg(not(any(debug_assertions, test)))]
#[inline(always)]
pub(crate) fn lock<T, const RANK: u16>(
    mutex: &crate::sync::Mutex<T>,
) -> std::sync::LockResult<MutexGuard<'_, T>> {
    mutex.lock()
}

#[cfg(all(test, not(acyclic_fs_loom)))]
mod tests;
