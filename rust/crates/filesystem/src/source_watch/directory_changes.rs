//! Windows: `ReadDirectoryChangesExW` over the whole source root.

#![allow(
    unsafe_code,
    reason = "watches through the Win32 ReadDirectoryChangesW API"
)]

use super::{Delivery, HostChange};
use std::io;
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;
use windows::Win32::Foundation::{
    ERROR_INVALID_FUNCTION, ERROR_IO_INCOMPLETE, ERROR_NOTIFY_ENUM_DIR, ERROR_OPERATION_ABORTED,
    HANDLE,
};
use windows::Win32::Storage::FileSystem::{
    FILE_LIST_DIRECTORY, FILE_NOTIFY_CHANGE_ATTRIBUTES, FILE_NOTIFY_CHANGE_CREATION,
    FILE_NOTIFY_CHANGE_DIR_NAME, FILE_NOTIFY_CHANGE_FILE_NAME, FILE_NOTIFY_CHANGE_LAST_WRITE,
    FILE_NOTIFY_CHANGE_SECURITY, FILE_NOTIFY_CHANGE_SIZE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    ReadDirectoryChangesExW, ReadDirectoryNotifyExtendedInformation,
    ReadDirectoryNotifyInformation,
};
use windows::Win32::System::IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED};
use windows::Win32::System::Threading::{CreateEventW, INFINITE, SetEvent, WaitForSingleObject};

use crate::WorkCounters;
use crate::demand::{DemandError, SourceDemandReadWork};
use crate::native_host::NativeHostWork;
mod decode;
use decode::translate;
const READ: WorkCounters = WorkCounters {
    backend_read_operations: 1,
    ..WorkCounters::UNCHARGED
};
const WRITE: WorkCounters = WorkCounters {
    backend_write_operations: 1,
    ..WorkCounters::UNCHARGED
};

const BUFFER_WORDS: usize = 64 * 1024 / size_of::<u64>();

pub(crate) struct NativeSourceWatch {
    shared: Arc<Shared>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

struct Shared {
    delivery: Arc<Delivery>,
    reads: Mutex<Option<Reads>>,
    extended: AtomicBool,
    complete: bool,
    fenced: Mutex<u64>,
    fence_delivered: Condvar,
    stopping: AtomicBool,
    cleanup: Option<crate::materializer::NativeWorkReservation>,
    allocation: Option<crate::NativeAllocation>,
}

struct Reads {
    directory: Option<OwnedHandle>,
    completed: Option<OwnedHandle>,
    overlapped: Box<OVERLAPPED>,
    buffer: Option<Box<std::mem::MaybeUninit<[u64; BUFFER_WORDS]>>>,
    pending: bool,
    requested: u64,
    cleanup: Option<crate::materializer::NativeWorkReservation>,
    allocation: Option<crate::NativeAllocation>,
}

// SAFETY: the kernel owns only the pinned boxes during a pending read.
unsafe impl Send for Reads {}

impl Reads {
    fn cleanup_call<T>(&self, delta: WorkCounters, operation: impl FnOnce() -> T) -> T {
        if let Some(credit) = &self.cleanup {
            if let Err(error) = credit.record_completed(delta) {
                credit.work().with_original_leaf(|state| {
                    state.record_native_io_failure(crate::native_name::original_name_error(error));
                    state.poison_native_original_work();
                });
            }
        }
        operation()
    }
    fn wake(&self) {
        if self.pending {
            self.cleanup_call(WRITE, || unsafe {
                let _ = CancelIoEx(
                    HANDLE(
                        self.directory
                            .as_ref()
                            .expect("opened directory")
                            .as_raw_handle(),
                    ),
                    Some(&raw const *self.overlapped),
                );
            });
        }
        self.cleanup_call(WRITE, || unsafe {
            let _ = SetEvent(HANDLE(
                self.completed
                    .as_ref()
                    .expect("completion event")
                    .as_raw_handle(),
            ));
        });
    }
}

impl Drop for Reads {
    fn drop(&mut self) {
        if self.pending {
            self.cleanup_call(WRITE, || unsafe {
                let _ = CancelIoEx(
                    HANDLE(
                        self.directory
                            .as_ref()
                            .expect("opened directory")
                            .as_raw_handle(),
                    ),
                    Some(&raw const *self.overlapped),
                );
            });
            self.cleanup_call(READ, || unsafe {
                let mut transferred = 0;
                let _ = GetOverlappedResult(
                    HANDLE(
                        self.directory
                            .as_ref()
                            .expect("opened directory")
                            .as_raw_handle(),
                    ),
                    &raw const *self.overlapped,
                    &mut transferred,
                    true,
                );
            });
        }
        self.cleanup_call(
            WorkCounters {
                backend_write_operations: u64::from(self.directory.is_some())
                    + u64::from(self.completed.is_some()),
                ..WorkCounters::UNCHARGED
            },
            || (),
        );
        // Handles and buffers drop before their retained-capacity guard.
    }
}

impl NativeSourceWatch {
    pub(super) fn start_driver<W: SourceDemandReadWork>(
        root: &crate::native_host::HostRoot,
        delivery: Arc<Delivery>,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        let original = work
            .native_source_work()
            .map(|source| source.materialization_work())
            .transpose()?;
        let cleanup = original
            .as_ref()
            .map(|work| {
                work.allocation_work().reserve_work(WorkCounters {
                    backend_read_operations: 2,
                    backend_write_operations: 6,
                    ..WorkCounters::UNCHARGED
                })
            })
            .transpose()
            .map_err(crate::native_name::original_name_error)?;
        let thread_cleanup = cleanup
            .as_ref()
            .map(|credit| {
                credit.partition_work(WorkCounters {
                    backend_read_operations: 1,
                    backend_write_operations: 1,
                    ..WorkCounters::UNCHARGED
                })
            })
            .transpose()
            .map_err(crate::native_name::original_name_error)?;
        let bytes = (size_of::<Shared>()
            + 2 * size_of::<usize>()
            + size_of::<OVERLAPPED>()
            + size_of::<[u64; BUFFER_WORDS]>()) as u64;
        let mut allocation = if let Some(original) = &original {
            Some(
                original
                    .claim(bytes, 3, WorkCounters::UNCHARGED)
                    .map_err(crate::native_name::original_name_error)?,
            )
        } else {
            work.claim_allocation(bytes, 3)?;
            None
        };
        let header = allocation
            .as_mut()
            .map(|allocation| {
                allocation.split_off((size_of::<Shared>() + 2 * size_of::<usize>()) as u64)
            })
            .transpose()
            .map_err(crate::native_name::original_name_error)?;
        let mut shared = Arc::new(Shared {
            delivery,
            extended: AtomicBool::new(true),
            complete: false,
            fenced: Mutex::new(0),
            fence_delivered: Condvar::new(),
            stopping: AtomicBool::new(false),
            cleanup: thread_cleanup,
            allocation: header,
            reads: Mutex::new(Some(Reads {
                directory: None,
                completed: None,
                overlapped: Box::default(),
                buffer: Some(Box::new_uninit()),
                pending: false,
                requested: 0,
                cleanup,
                allocation,
            })),
        });
        let complete = {
            let mut state = shared.reads.lock().unwrap_or_else(PoisonError::into_inner);
            let reads = state.as_mut().expect("prepared reader");
            reads.directory = Some(open_for_changes(root, work)?);
            let complete = crate::native_host::reports_every_change(
                reads.directory.as_ref().expect("opened directory"),
                work,
            )?;
            work.started_io(WRITE)?;
            // SAFETY: an unnamed, exclusively owned manual-reset event.
            let event =
                unsafe { CreateEventW(None, true, false, None) }.map_err(io::Error::from)?;
            reads.completed = Some(unsafe { OwnedHandle::from_raw_handle(event.0) });
            complete
        };
        Arc::get_mut(&mut shared)
            .expect("startup owns Shared")
            .complete = complete;
        {
            let mut state = shared.reads.lock().unwrap_or_else(PoisonError::into_inner);
            let reads = state.as_mut().expect("new reader");
            if let Err(error) = shared.issue_read(reads, work) {
                if !matches!(&error, DemandError::Io(error) if error.raw_os_error() == Some(ERROR_INVALID_FUNCTION.to_hresult().0))
                {
                    return Err(error);
                }
                shared.extended.store(false, Ordering::Release);
                shared.issue_read(reads, work)?;
            }
        }
        work.started_io(WRITE)?;
        let reader = Arc::clone(&shared);
        let thread = std::thread::Builder::new()
            .name("acyclic-source-watch".to_owned())
            .spawn(move || {
                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| reader.run())).is_err()
                {
                    let _ = reader
                        .delivery
                        .with_work(|_| Err::<(), _>(DemandError::WorkerUnavailable));
                    drop(
                        reader
                            .reads
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .take(),
                    );
                    *reader.fenced.lock().unwrap_or_else(PoisonError::into_inner) = u64::MAX;
                    reader.fence_delivered.notify_all();
                }
            })?;
        if !work.is_native_original() {
            work.release(bytes)?;
        }
        Ok(Self {
            shared,
            thread: Mutex::new(Some(thread)),
        })
    }

    pub(crate) fn fence_with_work<W: SourceDemandReadWork>(
        &self,
        timeout: Duration,
        work: &mut W,
    ) -> Result<(), DemandError> {
        work.check()?;
        let shared = &self.shared;
        let fence = {
            let mut state = shared.reads.lock().unwrap_or_else(PoisonError::into_inner);
            let Some(reads) = state.as_mut() else {
                return shared.delivery.take_failure(work);
            };
            reads.requested = reads
                .requested
                .checked_add(1)
                .ok_or(crate::WorkError::Overflow)?;
            work.started_io(WRITE)?;
            // SAFETY: cancellation targets only the pinned pending read.
            unsafe {
                if reads.pending {
                    let _ = CancelIoEx(
                        HANDLE(
                            reads
                                .directory
                                .as_ref()
                                .expect("opened directory")
                                .as_raw_handle(),
                        ),
                        Some(&raw const *reads.overlapped),
                    );
                } else {
                    SetEvent(HANDLE(
                        reads
                            .completed
                            .as_ref()
                            .expect("completion event")
                            .as_raw_handle(),
                    ))
                    .map_err(io::Error::from)?;
                }
            }
            reads.requested
        };
        let fenced = shared.fenced.lock().unwrap_or_else(PoisonError::into_inner);
        let (_, timeout) = shared
            .fence_delivered
            .wait_timeout_while(fenced, timeout, |delivered| *delivered < fence)
            .unwrap_or_else(PoisonError::into_inner);
        if timeout.timed_out() {
            shared.delivery.deliver(&[HostChange::Everything]);
        } else if !shared.complete {
            shared.delivery.deliver(&[HostChange::Unconfirmed]);
        }
        shared.delivery.take_failure(work)
    }

    pub(crate) fn stop_with_work<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
    ) -> Result<(), DemandError> {
        work.check()?;
        if self.shared.delivery.original.is_some() && !self.shared.delivery.birth_matches(work) {
            return Err(DemandError::NativeWorkUnavailable);
        }
        self.stop()
    }
    pub(crate) fn stop_with_cleanup_wait(
        &self,
        wait: &crate::CancellationToken,
    ) -> Result<(), DemandError> {
        wait.check().map_err(|_| DemandError::Cancelled)?;
        self.stop()?;
        wait.check().map_err(|_| DemandError::Cancelled)
    }
    pub(crate) fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        self.stop()
    }
    fn stop(&self) -> Result<(), DemandError> {
        let mut thread = self
            .thread
            .try_lock()
            .map_err(|_| DemandError::WorkerUnavailable)?;
        if thread
            .as_ref()
            .is_some_and(|thread| thread.thread().id() == std::thread::current().id())
        {
            return Err(DemandError::WorkerUnavailable);
        }
        let first = !self.shared.stopping.swap(true, Ordering::AcqRel);
        if first {
            if let Some(reads) = self
                .shared
                .reads
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_ref()
            {
                reads.wake();
            }
        }
        if let Some(thread) = thread.take() {
            if let Some(credit) = &self.shared.cleanup {
                credit
                    .record_completed(WorkCounters {
                        backend_read_operations: 1,
                        backend_write_operations: 1,
                        ..WorkCounters::UNCHARGED
                    })
                    .map_err(crate::native_name::original_name_error)?;
            }
            thread.join().map_err(|_| DemandError::WorkerUnavailable)?;
        }
        drop(
            self.shared
                .reads
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take(),
        );
        Ok(())
    }
    pub(crate) fn is_stopped(&self) -> bool {
        self.shared
            .reads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none()
    }
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        let reads = self
            .shared
            .reads
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        (size_of::<Shared>() + size_of::<Self>()) as u64
            + reads.as_ref().map_or(0, |_| {
                (size_of::<OVERLAPPED>() + size_of::<[u64; BUFFER_WORDS]>()) as u64
            })
    }
    pub(super) fn delivery(&self) -> &Delivery {
        &self.shared.delivery
    }
    #[cfg(test)]
    pub(super) fn paused(&self) -> impl Sized + '_ {
        self.shared
            .reads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for NativeSourceWatch {
    fn drop(&mut self) {
        let first = !self.shared.stopping.swap(true, Ordering::AcqRel);
        if first {
            if let Some(reads) = self
                .shared
                .reads
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_ref()
            {
                reads.wake();
            }
        }
        let thread = self
            .thread
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(thread) = thread {
            let joining = thread.thread().id() != std::thread::current().id();
            if let Some(credit) = &self.shared.cleanup {
                if let Err(error) = credit.record_completed(WorkCounters {
                    backend_read_operations: u64::from(joining),
                    backend_write_operations: 1,
                    ..WorkCounters::UNCHARGED
                }) {
                    credit.work().with_original_leaf(|state| {
                        state.record_native_io_failure(crate::native_name::original_name_error(
                            error,
                        ));
                        state.poison_native_original_work();
                    });
                }
            }
            if joining {
                let _ = thread.join();
            }
        }
    }
}

impl Shared {
    fn run(&self) {
        loop {
            let event = {
                let state = self.reads.lock().unwrap_or_else(PoisonError::into_inner);
                let Some(reads) = state.as_ref() else {
                    break;
                };
                HANDLE(
                    reads
                        .completed
                        .as_ref()
                        .expect("completion event")
                        .as_raw_handle(),
                )
            };
            if self
                .delivery
                .with_work(|work| work.started_io(READ))
                .is_err()
            {
                break;
            }
            // SAFETY: the worker retains the event and pinned read state.
            unsafe { WaitForSingleObject(event, INFINITE) };
            let drained = self.delivery.with_work(|work| {
                let mut state = self.reads.lock().unwrap_or_else(PoisonError::into_inner);
                let Some(reads) = state.as_mut() else {
                    return Ok(());
                };
                let fence = reads.requested;
                let transferred = self.finish_read(reads, true, work)?;
                let buffer = reads.buffer.take().expect("completed read buffer");
                drop(state);
                let result = if let Some(length) = transferred {
                    if length == 0 {
                        self.delivery
                            .sink
                            .host_changed(&[HostChange::Everything], work)
                    } else {
                        // SAFETY: terminal completion initialized this prefix.
                        let bytes = unsafe {
                            std::slice::from_raw_parts(buffer.as_ptr().cast::<u8>(), length)
                        };
                        translate(
                            bytes,
                            self.extended.load(Ordering::Acquire),
                            &self.delivery,
                            work,
                        )
                    }
                } else {
                    Ok(())
                };
                let mut state = self.reads.lock().unwrap_or_else(PoisonError::into_inner);
                let reads = state.as_mut().expect("worker owns read state");
                reads.buffer = Some(buffer);
                result?;
                if !self.stopping.load(Ordering::Acquire) {
                    self.issue_read(reads, work)?;
                }
                drop(state);
                let mut fenced = self.fenced.lock().unwrap_or_else(PoisonError::into_inner);
                *fenced = (*fenced).max(fence);
                self.fence_delivered.notify_all();
                Ok(())
            });
            if drained.is_err()
                || self.stopping.load(Ordering::Acquire)
                || !self.delivery.is_exact()
            {
                break;
            }
        }
        drop(
            self.reads
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take(),
        );
        *self.fenced.lock().unwrap_or_else(PoisonError::into_inner) = u64::MAX;
        self.fence_delivered.notify_all();
    }

    fn issue_read<W: SourceDemandReadWork>(
        &self,
        reads: &mut Reads,
        work: &mut W,
    ) -> Result<(), DemandError> {
        *reads.overlapped = OVERLAPPED {
            hEvent: HANDLE(
                reads
                    .completed
                    .as_ref()
                    .expect("completion event")
                    .as_raw_handle(),
            ),
            ..OVERLAPPED::default()
        };
        work.started_io(READ)?;
        // SAFETY: pinned buffers stay alive until completion, including startup failure.
        unsafe {
            ReadDirectoryChangesExW(
                HANDLE(
                    reads
                        .directory
                        .as_ref()
                        .expect("opened directory")
                        .as_raw_handle(),
                ),
                reads
                    .buffer
                    .as_mut()
                    .expect("pending read buffer")
                    .as_mut_ptr()
                    .cast(),
                size_of::<[u64; BUFFER_WORDS]>() as u32,
                true,
                FILE_NOTIFY_CHANGE_FILE_NAME
                    | FILE_NOTIFY_CHANGE_DIR_NAME
                    | FILE_NOTIFY_CHANGE_ATTRIBUTES
                    | FILE_NOTIFY_CHANGE_SIZE
                    | FILE_NOTIFY_CHANGE_LAST_WRITE
                    | FILE_NOTIFY_CHANGE_CREATION
                    | FILE_NOTIFY_CHANGE_SECURITY,
                None,
                Some(&raw mut *reads.overlapped),
                None,
                if self.extended.load(Ordering::Acquire) {
                    ReadDirectoryNotifyExtendedInformation
                } else {
                    ReadDirectoryNotifyInformation
                },
            )
        }
        .map_err(io::Error::from)?;
        reads.pending = true;
        Ok(())
    }

    fn finish_read<W: SourceDemandReadWork>(
        &self,
        reads: &mut Reads,
        wait: bool,
        work: &mut W,
    ) -> Result<Option<usize>, DemandError> {
        if !reads.pending {
            return Ok(None);
        }
        work.started_io(READ)?;
        let mut transferred = 0;
        // SAFETY: the pending read owns these pinned boxes until this completion.
        let result = unsafe {
            GetOverlappedResult(
                HANDLE(
                    reads
                        .directory
                        .as_ref()
                        .expect("opened directory")
                        .as_raw_handle(),
                ),
                &raw const *reads.overlapped,
                &mut transferred,
                wait,
            )
        };
        if result
            .as_ref()
            .err()
            .is_some_and(|error| error.code() == ERROR_IO_INCOMPLETE.to_hresult())
        {
            return Ok(None);
        }
        reads.pending = false;
        match result {
            Ok(()) => Ok(Some(
                (transferred as usize).min(size_of::<[u64; BUFFER_WORDS]>()),
            )),
            Err(error) if error.code() == ERROR_OPERATION_ABORTED.to_hresult() => Ok(None),
            Err(error) if error.code() == ERROR_NOTIFY_ENUM_DIR.to_hresult() => Ok(Some(0)),
            Err(error) => Err(io::Error::from(error).into()),
        }
    }
}

/// Opens the held root itself again (an empty name relative to it), for
fn open_for_changes<W: SourceDemandReadWork>(
    root: &crate::native_host::HostRoot,
    work: &mut W,
) -> Result<OwnedHandle, DemandError> {
    use windows::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows::Wdk::Storage::FileSystem::{
        FILE_DIRECTORY_FILE, FILE_OPEN, FILE_OPEN_FOR_BACKUP_INTENT, NtCreateFile,
    };
    use windows::Win32::Foundation::{RtlNtStatusToDosError, UNICODE_STRING};
    use windows::Win32::Storage::FileSystem::FILE_FLAGS_AND_ATTRIBUTES;
    use windows::Win32::System::IO::IO_STATUS_BLOCK;

    let name = UNICODE_STRING::default();
    let attributes = OBJECT_ATTRIBUTES {
        Length: u32::try_from(size_of::<OBJECT_ATTRIBUTES>()).unwrap_or(u32::MAX),
        RootDirectory: HANDLE(root.directory_handle().as_raw_handle()),
        ObjectName: &raw const name,
        ..OBJECT_ATTRIBUTES::default()
    };
    let mut handle = HANDLE::default();
    let mut status_block = IO_STATUS_BLOCK::default();
    // SAFETY: every pointer names a live value for this synchronous call;
    // without a synchronous-I/O option the handle is asynchronous.
    work.started_io(WRITE)?;
    let status = unsafe {
        NtCreateFile(
            &raw mut handle,
            FILE_LIST_DIRECTORY,
            &raw const attributes,
            &raw mut status_block,
            None,
            FILE_FLAGS_AND_ATTRIBUTES(0),
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            FILE_OPEN,
            FILE_DIRECTORY_FILE | FILE_OPEN_FOR_BACKUP_INTENT,
            None,
            0,
        )
    };
    if status.is_err() {
        // SAFETY: a pure status-code translation.
        let code = unsafe { RtlNtStatusToDosError(status) };
        return Err(io::Error::from_raw_os_error(i32::try_from(code).unwrap_or(i32::MAX)).into());
    }
    // SAFETY: a handle the call just returned is ours to own.
    Ok(unsafe { OwnedHandle::from_raw_handle(handle.0) })
}
