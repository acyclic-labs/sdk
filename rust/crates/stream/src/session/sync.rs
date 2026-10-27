#[cfg(loom)]
pub(super) use loom::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
#[cfg(not(loom))]
pub(super) use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
