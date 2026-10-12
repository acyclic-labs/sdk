//! Native source change notifications.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(windows)]
mod directory_changes;
#[cfg(target_os = "macos")]
mod fsevents;
#[cfg(target_os = "linux")]
mod inotify;

#[cfg(windows)]
pub(crate) use directory_changes::NativeSourceWatch;
#[cfg(target_os = "macos")]
pub(crate) use fsevents::NativeSourceWatch;
#[cfg(target_os = "linux")]
pub(crate) use inotify::NativeSourceWatch;

/// One change the host reported beneath a watched root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum HostChange {
    /// This root-relative path may bind another entry.
    Rebound(PathBuf),
    /// This root-relative entry may have changed in place.
    Altered(PathBuf),
    /// This volume node may have changed through any name.
    #[cfg(windows)]
    File(u64),
    /// Notifications were lost: anything may have changed.
    Everything,
    /// Delivery may be incomplete without known notification loss.
    #[cfg(any(target_os = "macos", windows))]
    Unconfirmed,
}

/// Receives each batch of changes, in the order the host reported them.
pub(crate) trait HostChangeSink: Send + Sync {
    /// Called with no watch lock held that a lookup could wait on.
    fn host_changed(&self, changes: &[HostChange]);
}

/// Where a watch delivers and whether it still covers every change.
pub(crate) struct Delivery {
    sink: Arc<dyn HostChangeSink>,
    exact: AtomicBool,
}

impl Delivery {
    fn new(sink: Arc<dyn HostChangeSink>) -> Self {
        Self {
            sink,
            exact: AtomicBool::new(true),
        }
    }

    fn deliver(&self, changes: &[HostChange]) {
        if !changes.is_empty() {
            self.sink.host_changed(changes);
        }
    }

    /// Reports everything before permanently marking delivery inexact.
    fn abandon(&self) {
        if self.exact.swap(false, Ordering::AcqRel) {
            self.sink.host_changed(&[HostChange::Everything]);
        }
    }

    fn is_exact(&self) -> bool {
        self.exact.load(Ordering::Acquire)
    }
}

impl NativeSourceWatch {
    pub(crate) fn start(
        directory: &crate::native_host::HostRoot,
        sink: Arc<dyn HostChangeSink>,
    ) -> std::io::Result<Self> {
        Self::start_driver(directory, Arc::new(Delivery::new(sink)))
    }

    pub(crate) fn is_exact(&self) -> bool {
        self.delivery().is_exact()
    }
}

#[cfg(test)]
mod tests;
