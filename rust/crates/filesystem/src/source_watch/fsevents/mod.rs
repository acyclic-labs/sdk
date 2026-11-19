//! macOS source notifications with bounded delivery and funded shutdown.
#![allow(unsafe_code, reason = "reads the held-root path with fcntl")]

use super::{Delivery, HostChange, check_wait};
use crate::cancellation::custody::Custody;
use crate::demand::{SourceDemandReadWork, SourcePayloadOwnership};
use crate::fsevents::{
    EventBatch, EventStream, ITEM_CREATED, ITEM_REMOVED, ITEM_RENAMED, KERNEL_DROPPED, MOUNT,
    MUST_SCAN_SUBDIRS, ROOT_CHANGED, UNMOUNT, USER_DROPPED,
};
use crate::native_host::{buffer, clone_path};
use crate::{DemandError, WorkCounters};
use std::ffi::{CStr, OsStr};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::{Mutex, PoisonError};

mod events;
use acyclic_native_runtime::file_identity_from_metadata as identity;
#[cfg(test)]
use events::change;
#[cfg(test)]
#[cfg(any(not(unit_test_split), unit_test_mount_watch))]
pub(super) mod tests;

const ITEM_FLAGS: u32 = 0x00ff_ff00;
const READ: WorkCounters = WorkCounters {
    backend_read_operations: 1,
    ..WorkCounters::UNCHARGED
};

pub(crate) struct NativeSourceWatch {
    shared: Arc<Shared>,
    stream: Custody<Option<EventStream>>,
}

struct Shared {
    root: PathBuf,
    held: (u64, u64),
    stream_bytes: AtomicU64,
    delivery: Arc<Delivery>,
    ownership: SourcePayloadOwnership,
    /// Last: disconnects only after the original payload and work owners drop.
    #[cfg(test)]
    drop_complete: super::DataDrop,
}

impl NativeSourceWatch {
    #[cfg(test)]
    pub(super) fn data_drop(&self) -> &super::DataDrop {
        &self.shared.drop_complete
    }

    pub(crate) fn start_driver<W: SourceDemandReadWork>(
        directory: &crate::native_host::HostRoot,
        delivery: Arc<Delivery>,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        let ownership = SourcePayloadOwnership::new(work)?;
        let mut bytes = 0;
        let result = (|| {
            let root = held_path(directory.directory_fd(), work)?;
            bytes = root.capacity() as u64;
            ownership.retain(bytes, work)?;
            let actual = directory.identity();
            let control = super::arc_control_bytes::<Shared>();
            work.claim(control)?;
            bytes += control;
            ownership.retain(control, work)?;
            let shared = Arc::new(Shared {
                root,
                held: (actual.device, actual.object),
                stream_bytes: AtomicU64::new(0),
                delivery,
                ownership,
                #[cfg(test)]
                drop_complete: Mutex::new(None),
            });
            let handler_bytes = super::arc_control_bytes::<Arc<Shared>>();
            work.claim(handler_bytes)?;
            bytes += handler_bytes;
            shared.ownership.retain(handler_bytes, work)?;
            let reader = shared.clone();
            let handler = Arc::new(move |events: Result<EventBatch<'_>, Arc<DemandError>>| {
                let mut work = reader.delivery.callback_work();
                let delivered =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match events {
                        Ok(events) => reader.events(events.iter(), &mut work),
                        Err(error) => Err(io::Error::other(error).into()),
                    }))
                    .unwrap_or(Err(DemandError::WorkerUnavailable));
                if let Err(error) = delivered {
                    reader.delivery.failed_with_work(error, &mut work);
                }
            });
            let mut stream = EventStream::start(
                std::slice::from_ref(&shared.root),
                handler,
                Some(&shared.stream_bytes),
                work,
            )?;
            stream.set_terminal(shared.clone());
            // The live stream owns later changes. Check the original
            // binding after arming so a move during startup is detected too.
            work.started_io(WorkCounters {
                items_examined: 1,
                ..READ
            })?;
            if identity(&std::fs::symlink_metadata(&shared.root)?) != shared.held {
                return Err(DemandError::SourceUnavailable);
            }
            Ok(Self {
                shared,
                stream: Custody::new(Some(stream)),
            })
        })();
        if !work.is_native_original() {
            work.release(bytes)?;
        }
        result
    }

    /// Reports a stream failure without claiming delivery: the caller fences
    /// with a cookie it observes itself.
    pub(crate) fn take_failure_with_work<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
    ) -> Result<(), DemandError> {
        work.check()?;
        self.shared.delivery.take_failure(work)
    }

    pub(crate) fn fence_with_work<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
    ) -> Result<bool, DemandError> {
        work.check()?;
        self.shared.delivery.take_failure(work)?;
        if self.shared.root_unchanged(work)? {
            self.delivery().deliver(&[HostChange::Unconfirmed], work)?;
        } else if self.delivery().notify_lost() {
            self.delivery().deliver(&[HostChange::Everything], work)?;
        }
        Ok(false)
    }

    #[cfg(test)]
    pub(crate) fn fence(&self) -> io::Result<()> {
        self.fence_with_work(&mut crate::native_host::ordinary_work())
            .map(|_| ())
            .map_err(crate::demand::native_demand_read_error)
    }

    pub(super) fn stop_driver<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
        wait: Option<&crate::CancellationToken>,
        prepaid: bool,
    ) -> Result<(), DemandError> {
        if !prepaid {
            work.check()?;
        }
        self.delivery().ensure_stop_available()?;
        let mut custody = self.stream.lock_with_wait(wait)?;
        check_wait(wait)?;
        let Some(stream) = custody.as_mut() else {
            return Ok(());
        };
        let retirement = if prepaid {
            stream.ensure_cleanup_wait_available(work)?;
            stream.cleanup_admitted(work)
        } else {
            stream.stop_with_work(work)
        };
        if stream.is_stopped() {
            *custody = None;
        }
        drop(custody);
        let result = retirement.complete();
        if prepaid && !self.is_stopped() {
            return result.and(Err(DemandError::WorkerUnavailable));
        }
        result
    }

    pub(crate) fn delivery(&self) -> &Delivery {
        &self.shared.delivery
    }
    pub(crate) fn platform_owned_bytes(&self) -> u64 {
        let bytes = self.shared.stream_bytes.load(Ordering::Acquire);
        super::arc_control_bytes::<Shared>()
            + self.shared.root.capacity() as u64
            + bytes
            + if bytes == 0 {
                0
            } else {
                super::arc_control_bytes::<Arc<Shared>>()
            }
    }

    /// The host's own report that it dropped changes, as the stream flags it.
    #[cfg(test)]
    pub(crate) fn inject_overflow(&self) -> io::Result<()> {
        self.inject(&self.shared.root, KERNEL_DROPPED);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn inject(&self, path: &Path, flags: u32) {
        let mut work = self.delivery().callback_work();
        if let Err(error) = self
            .shared
            .events(std::iter::once((path, flags)), &mut work)
        {
            self.delivery().failed_with_work(error, &mut work);
        }
    }
}

impl Drop for NativeSourceWatch {
    fn drop(&mut self) {
        if !self.is_stopped() {
            let mut work = self.shared.delivery.callback_work();
            if let Some(stream) = self.stream.get_mut().take() {
                let mut stream = stream;
                let retirement = stream.cleanup_admitted(&mut work);
                drop(stream);
                let _ = retirement.complete();
            }
        }
    }
}

impl crate::fsevents::StreamTerminal for Shared {
    fn stream_stopped(&self) {
        if let Err(error) = self
            .ownership
            .retire(super::arc_control_bytes::<Arc<Shared>>())
        {
            let mut work = self.delivery.callback_work();
            self.delivery.failed_with_work(error, &mut work);
        }
        self.delivery.notify_lost();
        self.stream_bytes.store(0, Ordering::Release);
        self.delivery.finish_retirement();
        self.delivery.sink.delivery_lost();
    }
}

fn held_path<W: SourceDemandReadWork>(
    descriptor: i32,
    work: &mut W,
) -> Result<PathBuf, DemandError> {
    let mut bytes = [0_u8; libc::PATH_MAX as usize];
    work.started_io(READ)?;
    // SAFETY: F_GETPATH writes at most PATH_MAX bytes into this live array.
    if unsafe { libc::fcntl(descriptor, libc::F_GETPATH, bytes.as_mut_ptr()) } < 0 {
        return Err(io::Error::last_os_error().into());
    }
    let bytes = CStr::from_bytes_until_nul(&bytes)
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
    clone_path(Path::new(OsStr::from_bytes(bytes.to_bytes())), work).map(|(path, _)| path)
}
