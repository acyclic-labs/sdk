#[cfg(loom)]
#[path = "sync/loom.rs"]
mod implementation;

#[cfg(not(loom))]
mod implementation {
    pub use std::sync::*;
}

pub use implementation::*;
