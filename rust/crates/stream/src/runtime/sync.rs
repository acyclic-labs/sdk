pub(crate) use crate::query_sync::SyncMutex as Mutex;
#[cfg(loom)]
pub(crate) use loom::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
#[cfg(not(loom))]
pub(crate) use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
