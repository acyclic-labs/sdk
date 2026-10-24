//! macOS source notifications with bounded delivery and funded shutdown.
#![allow(unsafe_code, reason = "reads the held-root path with fcntl")]

use super::{Delivery, HostChange};
use crate::demand::{SourceDemandReadWork, SourcePayloadOwnership};
use crate::fsevents::{
    EventBatch, EventStream, ITEM_CREATED, ITEM_REMOVED, ITEM_RENAMED, KERNEL_DROPPED, MOUNT,
    MUST_SCAN_SUBDIRS, UNMOUNT, USER_DROPPED,
};
use crate::native_host::{buffer, clone_path};
use crate::{DemandError, WorkCounters};
use std::ffi::{CStr, OsStr};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

mod events;
#[cfg(test)]
use events::change;
use events::identity;
#[cfg(test)]
mod tests;

const ITEM_FLAGS: u32 = 0x00ff_ff00;
const READ: WorkCounters = WorkCounters {
    backend_read_operations: 1,
    ..WorkCounters::UNCHARGED
};

pub(crate) struct NativeSourceWatch {
    shared: Arc<Shared>,
    stream: Mutex<Option<EventStream>>,
}

struct Shared {
    root: PathBuf,
    held: (u64, u64),
    stream_bytes: AtomicU64,
    delivery: Arc<Delivery>,
    ownership: SourcePayloadOwnership,
}

impl NativeSourceWatch {
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
            work.started_io(WorkCounters {
                items_examined: 1,
                ..READ
            })?;
            let held = identity(&std::fs::symlink_metadata(&root)?);
            let actual = directory.identity();
            if held != (actual.device, actual.object) {
                return Err(DemandError::SourceUnavailable);
            }
            let control = super::arc_control_bytes::<Shared>();
            work.claim(control)?;
            bytes += control;
            ownership.retain(control, work)?;
            let shared = Arc::new(Shared {
                root,
                held,
                stream_bytes: AtomicU64::new(0),
                delivery,
                ownership,
            });
            let handler_bytes = super::arc_control_bytes::<Arc<Shared>>();
            work.claim(handler_bytes)?;
            bytes += handler_bytes;
            shared.ownership.retain(handler_bytes, work)?;
            let reader = shared.clone();
            let handler = Arc::new(move |events: EventBatch<'_>| {
                let mut work = reader.delivery.callback_work();
                if let Err(error) = reader.events(events.iter(), &mut work) {
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
            Ok(Self {
                shared,
                stream: Mutex::new(Some(stream)),
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
        _timeout: Duration,
        work: &mut W,
    ) -> Result<bool, DemandError> {
        work.check()?;
        self.shared.delivery.take_failure(work)?;
        work.started_io(WorkCounters {
            items_examined: 1,
            ..READ
        })?;
        if std::fs::symlink_metadata(&self.shared.root)
            .is_ok_and(|metadata| identity(&metadata) == self.shared.held)
        {
            self.delivery().deliver(&[HostChange::Unconfirmed], work)?;
        } else if self.delivery().exact.swap(false, Ordering::AcqRel) {
            self.delivery().deliver(&[HostChange::Everything], work)?;
        }
        Ok(false)
    }

    #[cfg(test)]
    pub(crate) fn fence(&self) -> io::Result<()> {
        self.fence_with_work(Duration::ZERO, &mut crate::native_host::ordinary_work())
            .map(|_| ())
            .map_err(crate::demand::native_demand_read_error)
    }

    pub(crate) fn stop_with_work<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
    ) -> Result<(), DemandError> {
        work.check()?;
        self.delivery().ensure_stop_available()?;
        let mut custody = self.stream.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(stream) = custody.as_mut() else {
            return Ok(());
        };
        let result = stream.stop_with_work(work);
        if stream.is_stopped() {
            *custody = None;
        }
        result
    }

    pub(crate) fn stop_with_cleanup_wait(
        &self,
        wait: &crate::CancellationToken,
    ) -> Result<(), DemandError> {
        self.cleanup(Some(wait))
    }

    pub(crate) fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        self.cleanup(None)
    }

    fn cleanup(&self, wait: Option<&crate::CancellationToken>) -> Result<(), DemandError> {
        check_wait(wait)?;
        let mut work = self
            .delivery()
            .original
            .as_ref()
            .ok_or(DemandError::NativeWorkUnavailable)?
            .clone();
        self.delivery().ensure_stop_available()?;
        let mut custody = if wait.is_some() {
            match self.stream.try_lock() {
                Ok(custody) => custody,
                Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => {
                    return Err(DemandError::WorkerUnavailable);
                }
            }
        } else {
            self.stream.lock().unwrap_or_else(PoisonError::into_inner)
        };
        check_wait(wait)?;
        let Some(stream) = custody.as_mut() else {
            return Ok(());
        };
        stream.ensure_cleanup_wait_available(&work)?;
        let result = stream.cleanup_admitted(&mut work);
        if stream.is_stopped() {
            *custody = None;
        }
        if !self.is_stopped() {
            return result.and(Err(DemandError::WorkerUnavailable));
        }
        result.and(check_wait(wait))
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.shared.stream_bytes.load(Ordering::Acquire) == 0
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
            if let Some(stream) = self
                .stream
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
            {
                let mut stream = stream;
                let _ = stream.cleanup_admitted(&mut work);
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
        self.delivery.exact.store(false, Ordering::Release);
        self.stream_bytes.store(0, Ordering::Release);
    }
}

fn check_wait(wait: Option<&crate::CancellationToken>) -> Result<(), DemandError> {
    wait.map_or(Ok(()), |wait| {
        wait.check().map_err(|_| DemandError::Cancelled)
    })
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
