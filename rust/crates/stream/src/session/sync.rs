#[cfg(acyclic_stream_loom)]
pub(super) use loom::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
#[cfg(not(acyclic_stream_loom))]
pub(super) use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
