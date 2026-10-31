#[cfg(loom)]
#[path = "sync/loom.rs"]
mod implementation;

#[cfg(not(loom))]
mod implementation {
    pub(crate) use std::sync::*;
}

pub(crate) use implementation::*;
