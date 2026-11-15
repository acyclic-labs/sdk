#[cfg(loom)]
use loom::sync;
#[cfg(not(loom))]
use std::sync;

pub(crate) use crate::query_sync::SyncMutex as Mutex;

#[cfg(host_runtime)]
pub(crate) use sync::Arc;
pub(crate) use sync::atomic::{AtomicU64, Ordering};
