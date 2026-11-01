#[cfg(loom)]
pub(crate) use loom::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};
#[cfg(not(loom))]
pub(crate) use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};
