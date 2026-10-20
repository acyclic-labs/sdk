//! Native source change notifications.

use crate::demand::{DemandError, DemandWork, SourceDemandReadWork, SourcePayloadOwnership};
use crate::native_host::NativeHostWork;
use crate::performance::WorkCounters;
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

/// Known SDK control storage: Arc's two counters, alignment, and Rust payload.
pub(super) const fn arc_control_bytes<T>() -> u64 {
    let header = std::mem::size_of::<[usize; 2]>();
    let alignment = std::mem::align_of::<T>();
    let payload_offset = (header + alignment - 1) & !(alignment - 1);
    (payload_offset + std::mem::size_of::<T>()) as u64
}

fn cleanup_credits(delta: WorkCounters) -> [(crate::materializer::NativeResourceCredit, u64); 3] {
    use crate::materializer::NativeResourceCredit::{Items, ReadOperations, WriteOperations};
    [
        (ReadOperations, delta.backend_read_operations),
        (WriteOperations, delta.backend_write_operations),
        (Items, delta.items_examined),
    ]
}
fn io_failure(work: &mut DemandWork, error: DemandError) -> DemandError {
    let io = work.record_native_io_failure(error);
    work.take_native_io_failure().unwrap_or(DemandError::Io(io))
}
pub(super) fn reserve_cleanup_credits(
    work: &mut DemandWork,
    delta: WorkCounters,
) -> Result<(), DemandError> {
    let credits = cleanup_credits(delta);
    for (index, (credit, count)) in credits.iter().enumerate() {
        if let Err(error) = work.reserve(credit.delta(*count)) {
            let error = if index < 2 {
                io_failure(work, error)
            } else {
                error
            };
            for (credit, count) in credits.iter().take(index) {
                work.refund(credit.delta(*count))?;
            }
            return Err(error);
        }
    }
    Ok(())
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[allow(
    clippy::expect_used,
    reason = "Cancellation refunds only credits reserved by the paired reservation function."
)]
pub(super) fn cancel_cleanup_credits(work: &mut DemandWork, delta: WorkCounters) {
    for (credit, count) in cleanup_credits(delta) {
        work.refund(credit.delta(count))
            .expect("balanced cleanup credit");
    }
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) fn record_cleanup_credits(
    work: &mut DemandWork,
    delta: WorkCounters,
) -> Result<(), DemandError> {
    let mut result = Ok(());
    for (credit, count) in &cleanup_credits(delta)[..2] {
        for _ in 0..*count {
            let recorded = work
                .complete(credit.delta(1), credit.delta(1))
                .map_err(|error| io_failure(work, error));
            result = result.and(recorded);
        }
    }
    let items = crate::materializer::NativeResourceCredit::Items.delta(delta.items_examined);
    result.and(work.complete(items, items))
}

/// One change the host reported beneath a watched root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum HostChange {
    /// This root-relative path (empty: the root itself) may bind another entry, or none, and so may everything reached through it.
    Rebound(PathBuf),
    /// The entry at this root-relative path may have changed in place: its content or attributes.
    Altered(PathBuf),
    /// The file with this host file index, on the root's volume, may have changed under every name bound to it.
    #[cfg(windows)]
    File(u64),
    /// Notifications were lost: anything may have changed.
    Everything,
    /// Changes may not all have been delivered yet, though none is known to have been lost: a fence that cannot prove delivery (see the macOS and Windows backends) reports this instead.
    #[cfg(any(target_os = "macos", windows))]
    Unconfirmed,
}

#[cfg(windows)]
pub(crate) enum HostChangeWide<'a> {
    Path { action: u32, path: &'a [u16] },
    File(u64),
    Everything,
    Unconfirmed,
}

/// Receives each batch of changes, in the order the host reported them.
pub(crate) trait HostChangeSink: Send + Sync {
    /// SDK payload owned by this watch's concrete sink wrapper, excluding borrowed caller controls and provider-private allocator metadata.
    fn logical_owned_bytes(&self) -> u64;
    /// Called with no watch lock held that a lookup could wait on.
    fn host_changed(
        &self,
        changes: &[HostChange],
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError>;

    fn host_changed_with_native_work(
        &self,
        _changes: &[HostChange],
        _work: &mut crate::NativeMaterializationWork,
    ) -> Result<(), DemandError> {
        Err(DemandError::NativeWorkUnavailable)
    }

    #[cfg(windows)]
    fn host_changed_wide(
        &self,
        changes: &[HostChangeWide<'_>],
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError>;

    #[cfg(windows)]
    fn host_changed_wide_with_native_work(
        &self,
        _changes: &[HostChangeWide<'_>],
        _work: &mut crate::NativeMaterializationWork,
    ) -> Result<(), DemandError> {
        Err(DemandError::NativeWorkUnavailable)
    }
}

#[cfg(target_os = "linux")]
pub(crate) trait NativeEventSink: Send + Sync {
    fn logical_owned_bytes(&self) -> u64;
    fn event_measured(
        &self,
        relative: &std::path::Path,
        mask: u32,
        cookie: usize,
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError>;
    fn invalidate_measured(
        &self,
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError>;
    fn event_with_native_work(
        &self,
        _relative: &std::path::Path,
        _mask: u32,
        _cookie: usize,
        _work: &mut crate::NativeMaterializationWork,
    ) -> Result<(), DemandError> {
        Err(DemandError::NativeWorkUnavailable)
    }
    fn invalidate_with_native_work(
        &self,
        _work: &mut crate::NativeMaterializationWork,
    ) -> Result<(), DemandError> {
        Err(DemandError::NativeWorkUnavailable)
    }
}

#[allow(
    clippy::large_enum_variant,
    reason = "Callback work stays on the stack; boxing would add an unadmitted allocation."
)]
pub(crate) enum CallbackWork {
    Ordinary(DemandWork),
    Original(crate::NativeMaterializationWork),
}

pub(crate) struct Delivery {
    #[cfg(windows)]
    read_authority: Option<Arc<Delivery>>,
    sink: Arc<dyn HostChangeSink>,
    exact: AtomicBool,
    budget: crate::performance::WorkBudget,
    cancellation: crate::cancellation::CancellationToken,
    failure: std::sync::Mutex<Option<(DemandError, Option<WorkCounters>)>>,
    #[cfg(target_os = "linux")]
    events: Option<Arc<dyn NativeEventSink>>,
    ownership: SourcePayloadOwnership,
    original: Option<crate::NativeMaterializationWork>,
}

/// A failed startup retaining its actual native owner.
pub struct SourceWatchStartupFailure {
    cause: std::sync::OnceLock<DemandError>,
    watch: std::sync::Mutex<Option<NativeSourceWatch>>,
    stopped: AtomicBool,
    _ownership: SourcePayloadOwnership,
}

mod api;
mod callback;
mod delivery;
mod startup_failure;
#[cfg(test)]
mod tests;
