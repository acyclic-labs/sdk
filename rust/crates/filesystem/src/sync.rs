#[cfg(acyclic_fs_loom)]
pub(crate) use loom::{
    sync::{Condvar, Mutex, MutexGuard, WaitTimeoutResult, atomic},
    thread_local,
};
#[cfg(not(acyclic_fs_loom))]
pub(crate) use std::{
    sync::{Condvar, Mutex, MutexGuard, WaitTimeoutResult, atomic},
    thread_local,
};
