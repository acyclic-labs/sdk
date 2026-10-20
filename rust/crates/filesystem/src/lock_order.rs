// Synchronous callback lock order: operation -> view state -> open file ->
// observer registry -> source cursor -> operation read cache -> original demand
// leaf -> view gate counters. Native views read their checkout source while
// holding view state, so the per-operation object cache ranks below the view.
// View gate counters never acquire the leaf. Resource allowances follow the leaf:
// ancestor resource -> descendant resource (materializer/allowance.rs).
#[cfg(any(
    test,
    all(
        any(feature = "local", feature = "native-mount"),
        not(target_arch = "wasm32")
    )
))]
pub(crate) const OPERATION: u16 = 10;
pub(crate) const VIEW: u16 = 20;
#[cfg(all(
    feature = "native-mount",
    any(target_os = "linux", target_os = "macos")
))]
pub(crate) const FILE: u16 = 30;
#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
pub(crate) const OBSERVERS: u16 = 35;
#[cfg(any(test, all(feature = "native-watch", not(target_arch = "wasm32"))))]
pub(crate) const CURSOR: u16 = 40;
pub(crate) const OPERATION_CACHE: u16 = 45;
#[cfg(any(test, not(target_arch = "wasm32")))]
pub(crate) const LEAF: u16 = 50;
#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
pub(crate) const GATE: u16 = 60;

#[cfg(any(debug_assertions, test))]
mod checked;
#[cfg(all(
    any(debug_assertions, test),
    any(
        all(test, not(target_arch = "wasm32")),
        all(feature = "native-mount", target_os = "linux")
    )
))]
pub(crate) use checked::Condvar;
#[cfg(all(
    any(debug_assertions, test),
    any(
        test,
        all(
            any(feature = "local", feature = "native-watch"),
            not(target_arch = "wasm32")
        )
    )
))]
pub(crate) use checked::Mutex;
#[cfg(any(debug_assertions, test))]
pub(crate) use checked::{MutexGuard, lock};

#[cfg(all(
    not(any(debug_assertions, test)),
    any(feature = "local", feature = "native-watch"),
    not(target_arch = "wasm32")
))]
pub(crate) type Mutex<T, const RANK: u16> = crate::sync::Mutex<T>;
#[cfg(all(
    not(any(debug_assertions, test)),
    feature = "native-mount",
    target_os = "linux"
))]
pub(crate) use crate::sync::Condvar;
#[cfg(not(any(debug_assertions, test)))]
pub(crate) use crate::sync::MutexGuard;

#[cfg(not(any(debug_assertions, test)))]
#[inline(always)]
pub(crate) fn lock<T, const RANK: u16>(
    mutex: &crate::sync::Mutex<T>,
) -> std::sync::LockResult<MutexGuard<'_, T>> {
    mutex.lock()
}

#[cfg(all(test, not(acyclic_fs_loom)))]
mod tests;
