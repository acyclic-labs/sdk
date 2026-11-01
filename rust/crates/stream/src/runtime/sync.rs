#[cfg(loom)]
pub(crate) use loom::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
#[cfg(not(loom))]
pub(crate) use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
