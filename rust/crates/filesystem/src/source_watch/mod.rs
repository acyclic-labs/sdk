//! Host notifications of changes made to a native source directory outside
//! every view of it.
//!
//! A view remembers what it read from a source: resolved names, absences,
//! attributes, listings, and the kernel caches built from them. The source
//! changes outside the view, so each remembered fact holds only until the
//! host reports a change to what it depends on. One watch per source root
//! turns the host's own notifications into exactly those reports: a name
//! rebound (created, removed, or renamed), which covers everything reached
//! through it; an entry altered in place (its content or attributes), which
//! covers the entry and the listing that shows it; and, where the host can
//! still identify it, the node, which covers every other name bound to it.
//!
//! # Backends
//!
//! - Linux: one inotify instance, with a watch on every directory the
//!   source reads before it reads it (see [`NativeSourceWatch::admit`]).
//!   The kernel queues each event before the system call that caused it
//!   returns.
//! - macOS: one `FSEvents` stream over the root and a private fence
//!   directory; the kernel appends each event to one ordered queue as the
//!   operation completes.
//! - Windows: `ReadDirectoryChangesExW` over the whole root, which the file
//!   system reports into before the operation that caused it completes.
//!
//! A root whose changes do not all pass through this kernel (a network or
//! user-space file system) has no watch: every view of it reads it afresh.
//!
//! # Exactness
//!
//! A fact is read after the view samples its ledger stamp, and is reused
//! only while nothing it depends on recorded a later position. A reported
//! change is recorded after the host made it. So a read that races an
//! unreported change either read the source before the change, and its fact
//! is invalidated once the report is recorded, or read it after, and its
//! fact is already current. What a lookup may still return is the answer
//! from before a change whose report has not been recorded yet: the change
//! takes effect in the view when its report is recorded rather than when
//! the host operation returned. Between processes that do not otherwise
//! communicate, that is indistinguishable from the change completing a
//! little later, which is how every native file system orders concurrent
//! operations. A process that learns of a change by another channel (a
//! build step waiting for a writer to exit) and must see it through the
//! mount calls [`NativeSourceWatch::fence`] first, which the mount's
//! revalidation does: once it returns, every change that completed before
//! it was called has been recorded. Hosts report a file's content and size
//! at different points: Linux at every write, macOS and Windows once the
//! writer closes (or flushes) the file, which is NFS close-to-open
//! consistency.
//!
//! Where the host names only the path it changed (Linux and macOS), the
//! node is the one the path binds when the report is handled: a change
//! through one hard link reaches every name of the node, but removing one
//! of several links leaves the others' link count as the view last read it
//! until something reports a change to them.
//!
//! A host that loses notifications says so (a queue overflow, dropped
//! events, a lost buffer); the watch then reports that everything may have
//! changed, which invalidates every remembered fact. A host that stops
//! reporting altogether (a replaced root, an exhausted watch limit) makes
//! the watch inexact for good: it reports everything once more and the view
//! reads the source afresh from then on.

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
use directory_changes::PlatformWatch;
#[cfg(target_os = "macos")]
use fsevents::PlatformWatch;
#[cfg(target_os = "linux")]
use inotify::PlatformWatch;

/// One change the host reported beneath a watched root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum HostChange {
    /// This root-relative path (empty: the root itself) may bind another
    /// entry, or none, and so may everything reached through it.
    Rebound(PathBuf),
    /// The entry at this root-relative path may have changed in place: its
    /// content or attributes. The path still binds it.
    Altered(PathBuf),
    /// The file with this host file index, on the root's volume, may have
    /// changed under every name bound to it.
    #[cfg(windows)]
    File(u64),
    /// Notifications were lost: anything may have changed.
    Everything,
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

    /// The host stopped reporting changes exactly: report everything once,
    /// after which the watch is inexact for good.
    fn abandon(&self) {
        if self.exact.swap(false, Ordering::AcqRel) {
            self.sink.host_changed(&[HostChange::Everything]);
        }
    }

    fn is_exact(&self) -> bool {
        self.exact.load(Ordering::Acquire)
    }
}

/// The live watch of one native source root.
pub(crate) struct NativeSourceWatch {
    platform: PlatformWatch,
}

impl NativeSourceWatch {
    /// Starts reporting changes beneath `root`, whose held directory is
    /// `directory`, to `sink`.
    ///
    /// # Errors
    ///
    /// Fails when the host refuses the watch; the caller then treats the
    /// root as unobserved.
    pub(crate) fn start(
        directory: &crate::native_host::HostRoot,
        sink: Arc<dyn HostChangeSink>,
    ) -> std::io::Result<Self> {
        Ok(Self {
            platform: PlatformWatch::start(directory, Arc::new(Delivery::new(sink)))?,
        })
    }

    /// Watches the root-relative directory `directory`, and every directory
    /// on the way to it, before the source reads beneath it. Returns whether
    /// `directory` itself became watched by this call, after which a fact
    /// about it read before the call must be read again.
    ///
    /// Only Linux watches directory by directory; elsewhere one watch covers
    /// the whole root.
    #[cfg(target_os = "linux")]
    pub(crate) fn admit(&self, directory: &std::path::Path) -> bool {
        self.platform.admit(directory)
    }

    /// Returns once every change that completed before the call has been
    /// delivered.
    ///
    /// # Errors
    ///
    /// Fails only when the host cannot be read; the watch is then inexact.
    pub(crate) fn fence(&self) -> std::io::Result<()> {
        self.platform.fence()
    }

    /// Whether every change still reaches the sink.
    pub(crate) fn is_exact(&self) -> bool {
        self.platform.delivery().is_exact()
    }
}

#[cfg(test)]
mod tests;
