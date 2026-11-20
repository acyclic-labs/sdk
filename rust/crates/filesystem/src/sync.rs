#[cfg(loom)]
pub(crate) use loom::sync::*;
#[cfg(not(loom))]
pub(crate) use std::sync::*;

// Every retained native owner includes its lock storage in its admitted layout.
// The pthread-backed standard mutex allocates separately on first acquisition.
#[cfg(all(not(loom), not(target_arch = "wasm32")))]
pub(crate) use self::{InlineMutex as Mutex, InlineMutexGuard as MutexGuard};
// The ranked condition variable's raw half; every other native condition
// variable names its own type, so this export exists exactly where the ranked
// `lock_order::Condvar` does.
#[cfg(all(not(loom), not(target_arch = "wasm32"), target_os = "linux"))]
pub(crate) use acyclic_native_runtime::InlineCondvar as Condvar;

#[cfg(all(not(loom), not(target_arch = "wasm32")))]
pub(crate) use acyclic_native_runtime::{InlineMutex, InlineMutexGuard};
#[cfg(loom)]
pub(crate) use loom::sync::Mutex as InlineMutex;
#[cfg(all(loom, not(target_arch = "wasm32")))]
pub(crate) use loom::sync::MutexGuard as InlineMutexGuard;
#[cfg(all(not(loom), target_arch = "wasm32"))]
pub(crate) use std::sync::Mutex as InlineMutex;

// Type identity checks compile in ordinary builds, including --all-features.
#[cfg(all(not(loom), not(target_arch = "wasm32")))]
const _: fn(Mutex<()>) -> acyclic_native_runtime::InlineMutex<()> = |mutex| mutex;
#[cfg(all(not(loom), target_arch = "wasm32"))]
const _: fn(Mutex<()>) -> std::sync::Mutex<()> = |mutex| mutex;
#[cfg(loom)]
const _: fn(Mutex<()>) -> loom::sync::Mutex<()> = |mutex| mutex;
