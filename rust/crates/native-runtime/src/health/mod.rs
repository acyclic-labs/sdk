use crate::{NativeFilePayload, OperationFence};
use std::sync::{Arc, Mutex, atomic::AtomicBool};

#[derive(Default)]
pub(crate) struct FileHealth {
    pub(crate) tail: Mutex<Option<Arc<OperationFence>>>,
    pub(crate) uncertain: AtomicBool,
    pub(crate) poison_anchor: Mutex<Option<Arc<NativeFilePayload>>>,
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
mod registry;
#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
pub(crate) use registry::{FILE_HEALTH, FileHealthRegistry, poison_file_health, shared_file_health};
