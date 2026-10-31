//! One native full flush attempt of a held descriptor, with no internal retry.
use std::io;
#[cfg(unix)]
use std::os::fd::{AsRawFd as _, BorrowedFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawHandle as _, BorrowedHandle};

/// Observe the actual attempt through the caller's existing operation collector.
/// Neither helper nesting nor a barrier without an OS call reaches this seam.
/// The descriptor is opaque and meaningful only while the borrowed handle lives.
pub(crate) fn observe_sync(
    requested: crate::Durability,
    syscall: &'static str,
    descriptor: u64,
    call: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    crate::task::with_persistence_context(|| {
        let span = tracing::info_span!(
            "acyclic.runtime.durability",
            syscall,
            requested = match requested {
                crate::Durability::Full => "full",
                crate::Durability::Barrier => "barrier",
            },
            descriptor,
            outcome = tracing::field::Empty,
            error.kind = tracing::field::Empty,
            error.os = tracing::field::Empty,
        );
        let completion = acyclic_observability::Completion::new(&span);
        span.in_scope(|| {
            // Capture last_os_error in `call` before recording or closing spans.
            let result = call();
            if let Err(error) = &result
                && let Some(code) = error.raw_os_error()
            {
                span.record("error.os", code);
            }
            completion.finish(result, crate::obs::kind)
        })
    })
}

/// Makes `descriptor` durable with one native full flush attempt: `fsync` on
/// Linux and `FlushFileBuffers` on Windows. On Apple, where a bare `fsync`
/// stops at the drive cache, it stages the file with `fsync` and then waits
/// for an `F_FULLFSYNC` of its device that started after the stage, sharing
/// it with every concurrent request on that device. Every failure returns to
/// the caller's original metered operation, including an interrupted flush.
/// Unlike std's `sync_all`, nothing is retried here, and
/// Apple never falls back to a weaker flush.
pub fn sync_descriptor_once(
    #[cfg(unix)] descriptor: BorrowedFd<'_>,
    #[cfg(windows)] descriptor: BorrowedHandle<'_>,
) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    return crate::apple::sync_descriptor_once(descriptor);
    #[cfg(not(target_vendor = "apple"))]
    sync_descriptor_requested_once(descriptor, crate::Durability::Full)
}

/// The same physical full flush, retaining the caller's requested guarantee.
#[cfg(not(target_vendor = "apple"))]
#[allow(
    unsafe_code,
    reason = "one flush call on the caller's borrowed descriptor"
)]
pub(crate) fn sync_descriptor_requested_once(
    #[cfg(unix)] descriptor: BorrowedFd<'_>,
    #[cfg(windows)] descriptor: BorrowedHandle<'_>,
    requested: crate::Durability,
) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    let (syscall, identity) = ("fsync", descriptor.as_raw_fd() as u64);
    #[cfg(windows)]
    let (syscall, identity) = (
        "FlushFileBuffers",
        descriptor.as_raw_handle() as usize as u64,
    );
    observe_sync(requested, syscall, identity, || {
        #[cfg(target_os = "linux")]
        // SAFETY: the borrowed descriptor stays open for this one flush.
        let failed = unsafe { libc::fsync(descriptor.as_raw_fd()) } == -1;
        #[cfg(windows)]
        // SAFETY: the borrowed handle stays open for this one flush.
        let failed = unsafe {
            windows_sys::Win32::Storage::FileSystem::FlushFileBuffers(descriptor.as_raw_handle())
        } == 0;
        if failed {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    })
}

/// Issues exactly one native ordering barrier of `descriptor`: its writes
/// reach stable storage before any write issued after it on the same device,
/// and become durable at that device's next full flush. Apple issues
/// `fcntl(F_BARRIERFSYNC)`; an unsupported barrier returns to the caller,
/// which can separately admit a full flush. Linux and Windows have no ordering
/// primitive weaker than the full flush and issue [`sync_descriptor_once`].
/// Every failure returns to the caller's original metered operation.
pub fn order_descriptor_once(
    #[cfg(unix)] descriptor: BorrowedFd<'_>,
    #[cfg(windows)] descriptor: BorrowedHandle<'_>,
) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    return crate::apple::order_descriptor_once(descriptor);
    #[cfg(not(target_vendor = "apple"))]
    sync_descriptor_requested_once(descriptor, crate::Durability::Barrier)
}

/// One data-only Linux attempt; its caller owns Interrupted retry.
#[cfg(target_os = "linux")]
#[allow(
    unsafe_code,
    reason = "one data flush on the caller's borrowed descriptor"
)]
pub(crate) fn sync_data_descriptor_once(
    descriptor: BorrowedFd<'_>,
    requested: crate::Durability,
) -> io::Result<()> {
    observe_sync(
        requested,
        "fdatasync",
        descriptor.as_raw_fd() as u64,
        || {
            // SAFETY: the borrowed descriptor stays open for this one flush.
            if unsafe { libc::fdatasync(descriptor.as_raw_fd()) } == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        },
    )
}

#[cfg(test)]
#[path = "held_sync_observation_tests.rs"]
mod observation_tests;

#[cfg(test)]
#[path = "held_sync_sources_tests.rs"]
mod source_tests;
#[cfg(all(test, windows))]
#[path = "held_sync_windows_tests.rs"]
mod windows_tests;

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{fs::File, io::Write as _, os::fd::AsFd as _};

    // Checkout token publication flushes files and each held ancestor. Test
    // both against the real filesystem, and a descriptor that cannot flush.
    // On Apple, ENODEV also distinguishes F_FULLFSYNC from bare fsync's
    // EINVAL. A successful no-op cannot satisfy either host's detector.
    fn full_flush_contract(flush: impl Fn(BorrowedFd<'_>) -> io::Result<()>) -> io::Result<bool> {
        let directory = tempfile::tempdir()?;
        let mut file = File::create(directory.path().join("record"))?;
        file.write_all(b"incarnation")?;
        flush(file.as_fd())?;
        flush(File::open(directory.path())?.as_fd())?;
        let device = File::open("/dev/null")?;
        #[cfg(target_vendor = "apple")]
        let expected = libc::ENODEV;
        #[cfg(target_os = "linux")]
        let expected = libc::EINVAL;
        Ok(matches!(flush(device.as_fd()), Err(error) if error.raw_os_error() == Some(expected)))
    }

    #[test]
    fn full_flush_supports_regular_files_and_directories_and_preserves_errors() {
        assert!(full_flush_contract(sync_descriptor_once).unwrap());
    }

    #[test]
    #[cfg(target_vendor = "apple")]
    fn negative_control_bare_fsync_does_not_meet_the_full_flush_contract() {
        assert!(
            !full_flush_contract(
                |descriptor| rustix::fs::fsync(descriptor).map_err(io::Error::from)
            )
            .unwrap()
        );
    }

    #[test]
    fn negative_control_swallowing_flush_errors_does_not_meet_the_contract() {
        assert!(!full_flush_contract(|_| Ok(())).unwrap());
    }
}
