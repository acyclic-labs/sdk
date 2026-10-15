#[cfg(loom)]
pub(crate) use loom::{
    sync::{Condvar, Mutex, MutexGuard, WaitTimeoutResult, atomic},
    thread_local,
};
#[cfg(not(loom))]
pub(crate) use std::{
    sync::{Condvar, Mutex, MutexGuard, WaitTimeoutResult, atomic},
    thread_local,
};
