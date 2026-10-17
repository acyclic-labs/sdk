#[cfg(loom)]
#[path = "sync/loom.rs"]
mod implementation;

#[cfg(not(loom))]
mod implementation {
    pub(crate) use std::sync::*;
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) use tokio::sync::Mutex as AsyncMutex;
}

pub(crate) use implementation::*;
