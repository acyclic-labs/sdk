use crate::{NativeFilePayload, OperationFence};
use std::sync::{Arc, Mutex, atomic::AtomicBool};

#[derive(Default)]
pub(crate) struct FileHealth {
    pub(crate) tail: Mutex<Option<Arc<OperationFence>>>,
    pub(crate) uncertain: AtomicBool,
    pub(crate) poison_anchor: Mutex<Option<Arc<NativeFilePayload>>>,
}

/// One owner of a file's health: a file capability, a queued or running job,
/// or an admission scope. Every owner is this type, so whichever one drops
/// last unregisters the health and frees it on its own thread, whatever the
/// drop order; a bare `Arc<FileHealth>` owner would skip that cleanup.
#[derive(Clone)]
pub(crate) struct HealthOwner {
    health: Arc<FileHealth>,
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    identity: crate::FileIdentity,
}

impl std::ops::Deref for HealthOwner {
    type Target = Arc<FileHealth>;
    fn deref(&self) -> &Arc<FileHealth> {
        &self.health
    }
}

#[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
impl HealthOwner {
    /// Health for a platform without a registry: nothing else can share it.
    pub(crate) fn unregistered() -> Self {
        Self {
            health: Arc::default(),
        }
    }
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
impl Drop for HealthOwner {
    fn drop(&mut self) {
        registry::release_file_health(self.identity, &self.health);
    }
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
mod registry;
#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
pub(crate) use registry::{
    FILE_HEALTH, FileHealthRegistry, poison_file_health, shared_file_health,
};
