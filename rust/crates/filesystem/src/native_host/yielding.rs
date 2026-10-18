use super::*;
use std::os::windows::io::AsRawHandle as _;
use std::sync::{Arc, Mutex};
use windows::Win32::{
    Foundation::{
        ERROR_HANDLE_EOF, ERROR_IO_PENDING, HANDLE, INVALID_HANDLE_VALUE, WAIT_FAILED, WAIT_TIMEOUT,
    },
    System::{
        IO::{CancelIoEx, DeviceIoControl, GetOverlappedResult, OVERLAPPED},
        Ioctl::{
            FSCTL_REQUEST_OPLOCK, OPLOCK_LEVEL_CACHE_HANDLE, OPLOCK_LEVEL_CACHE_READ,
            REQUEST_OPLOCK_CURRENT_VERSION, REQUEST_OPLOCK_INPUT_BUFFER,
            REQUEST_OPLOCK_INPUT_FLAG_REQUEST, REQUEST_OPLOCK_OUTPUT_BUFFER,
        },
        Threading::{
            CreateEventW, INFINITE, RegisterWaitForSingleObject, UnregisterWaitEx,
            WT_EXECUTEONLYONCE, WaitForSingleObject,
        },
    },
};
use windows::core::Owned;

/// A read whose handle yielded to rename or deletion.
#[derive(Debug)]
pub struct Yielded;
impl std::fmt::Display for Yielded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("the file gave way to a rename or delete")
    }
}
impl std::error::Error for Yielded {}

/// A read handle that closes when its read-handle oplock breaks.
pub struct YieldingFile {
    held: Arc<YieldingHeld>,
    wait: Option<HANDLE>,
    // Declared after held storage: Drop closes the request, then fields free
    // the Arc storage before this guard refunds the original ledger.
    allocation: Option<crate::NativeAllocation>,
}
struct YieldingHeld {
    file: Mutex<Option<Arc<File>>>,
    request: *mut OplockRequest,
}
struct OplockRequest {
    overlapped: OVERLAPPED,
    input: REQUEST_OPLOCK_INPUT_BUFFER,
    output: REQUEST_OPLOCK_OUTPUT_BUFFER,
    event: Owned<HANDLE>,
}
// SAFETY: request storage stays pinned until completion; handle access is locked.
unsafe impl Send for YieldingHeld {}
// SAFETY: same invariant as Send.
unsafe impl Sync for YieldingHeld {}
// SAFETY: only Drop unregisters the callback, waiting for its completion.
unsafe impl Send for YieldingFile {}
// SAFETY: shared handle access is locked.
unsafe impl Sync for YieldingFile {}

impl YieldingFile {
    pub(super) fn new<W: SourceDemandReadWork>(
        file: File,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        let version = u16::try_from(REQUEST_OPLOCK_CURRENT_VERSION)
            .map_err(|_| DemandError::InvalidRequest)?;
        let input_length = u16::try_from(size_of::<REQUEST_OPLOCK_INPUT_BUFFER>())
            .map_err(|_| DemandError::InvalidRequest)?;
        let output_length = u32::try_from(size_of::<REQUEST_OPLOCK_OUTPUT_BUFFER>())
            .map_err(|_| DemandError::InvalidRequest)?;
        let bytes = (size_of::<File>() + size_of::<YieldingHeld>()) as u64;
        let allocation = if let Some(original) = work.native_source_work() {
            Some(
                original
                    .claim(bytes, 2, WorkCounters::UNCHARGED)
                    .map_err(crate::native_name::original_name_error)?,
            )
        } else {
            work.claim_allocation(bytes, 2, WorkCounters::UNCHARGED)?;
            None
        };
        let handle = HANDLE(file.as_raw_handle());
        let mut yielding = Self {
            held: Arc::new(YieldingHeld {
                file: Mutex::new(Some(Arc::new(file))),
                request: std::ptr::null_mut(),
            }),
            wait: None,
            allocation,
        };
        let result = (|| {
            let held = Arc::get_mut(&mut yielding.held).ok_or(DemandError::InvalidRequest)?;
            // SAFETY: creation transfers sole ownership of this unnamed event.
            let event = unsafe {
                Owned::new(CreateEventW(None, true, false, None).map_err(io::Error::other)?)
            };
            let bytes = size_of::<OplockRequest>() as u64;
            if let Some(allocation) = &mut yielding.allocation {
                allocation
                    .grow(bytes)
                    .map_err(crate::native_name::original_name_error)?;
            } else {
                work.claim_allocation(bytes, 1, WorkCounters::UNCHARGED)?;
            }
            let request = Box::new(OplockRequest {
                overlapped: OVERLAPPED {
                    hEvent: *event,
                    ..Default::default()
                },
                input: REQUEST_OPLOCK_INPUT_BUFFER {
                    StructureVersion: version,
                    StructureLength: input_length,
                    RequestedOplockLevel: OPLOCK_LEVEL_CACHE_READ | OPLOCK_LEVEL_CACHE_HANDLE,
                    Flags: REQUEST_OPLOCK_INPUT_FLAG_REQUEST,
                },
                output: REQUEST_OPLOCK_OUTPUT_BUFFER::default(),
                event,
            });
            if let Err(error) = work.started_io(READ) {
                drop(request);
                if let Some(allocation) = &mut yielding.allocation {
                    allocation
                        .release(bytes)
                        .map_err(crate::native_name::original_name_error)?;
                } else {
                    work.release(bytes)?;
                }
                return Err(error);
            }
            let request = Box::into_raw(request);
            held.request = request;
            // SAFETY: request and held handle live until completion; Drop closes the handle and waits.
            let requested = unsafe {
                DeviceIoControl(
                    handle,
                    FSCTL_REQUEST_OPLOCK,
                    Some((&raw const (*request).input).cast()),
                    u32::from(input_length),
                    Some((&raw mut (*request).output).cast()),
                    output_length,
                    None,
                    Some(&raw mut (*request).overlapped),
                )
            };
            if !requested.is_err_and(|error| error.code() == ERROR_IO_PENDING.to_hresult()) {
                held.request = std::ptr::null_mut();
                // SAFETY: no pending request or registered callback refers to this storage.
                drop(unsafe { Box::from_raw(request) });
                if let Some(allocation) = &mut yielding.allocation {
                    allocation
                        .release(bytes)
                        .map_err(crate::native_name::original_name_error)?;
                } else {
                    work.release(bytes)?;
                }
                return Ok(());
            }
            let mut wait = HANDLE::default();
            // SAFETY: Drop unregisters and waits before destroying the callback state.
            unsafe {
                RegisterWaitForSingleObject(
                    &raw mut wait,
                    *(*request).event,
                    Some(yield_on_break),
                    Some(Arc::as_ptr(&yielding.held).cast()),
                    INFINITE,
                    WT_EXECUTEONLYONCE,
                )
            }
            .map_err(io::Error::other)?;
            yielding.wait = Some(wait);
            Ok(())
        })();
        if let Err(error) = result {
            let ordinary = yielding.allocation.is_none();
            let bytes = yielding.logical_owned_bytes();
            drop(yielding);
            if ordinary {
                work.release(bytes)?;
            }
            return Err(error);
        }
        Ok(yielding)
    }
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        (size_of::<YieldingHeld>()
            + size_of::<File>()
            + if self.held.request.is_null() {
                0
            } else {
                size_of::<OplockRequest>()
            }) as u64
    }
    pub(crate) fn was_yielded(&self) -> bool {
        self.held
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none()
    }
    /// Borrows the handle until it yields.
    pub fn with<T, E: From<io::Error>>(
        &self,
        operation: impl FnOnce(&File) -> Result<T, E>,
    ) -> Result<T, E> {
        let held = self
            .held
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        operation(held.as_ref().ok_or_else(|| io::Error::other(Yielded))?)
    }
    /// Appends at most `length` bytes read at `offset` to `data`'s
    /// uninitialized capacity; zero means end of file.
    pub(crate) fn read_spare_at<W: SourceDemandReadWork>(
        &self,
        offset: u64,
        data: &mut Vec<u8>,
        length: usize,
        work: &mut W,
    ) -> Result<usize, DemandError> {
        work.check()?;
        if length == 0 {
            return Ok(0);
        }
        work.with_native_read_meter(|meter| {
            use acyclic_native_runtime::{
                NativeByteReservation, NativeOperationKind, NativeOperationReservation,
            };
            let mut operations =
                NativeOperationReservation::new(meter, NativeOperationKind::Read, 2)
                    .map_err(crate::demand::native_demand_error)?;
            let length = length.min(u32::MAX as usize);
            let destination = data
                .spare_capacity_mut()
                .get_mut(..length)
                .ok_or(DemandError::InvalidRequest)?;
            let mut bytes = NativeByteReservation::new(meter, NativeOperationKind::Read, length)
                .map_err(crate::demand::native_demand_error)?;
            let held = self
                .held
                .file
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let file = held.as_ref().ok_or_else(|| io::Error::other(Yielded))?;
            let handle = HANDLE(file.as_raw_handle());
            // SAFETY: creation transfers sole event ownership.
            let event = unsafe {
                Owned::new(CreateEventW(None, true, false, None).map_err(io::Error::other)?)
            };
            let mut overlapped = OVERLAPPED {
                hEvent: *event,
                ..Default::default()
            };
            overlapped.Anonymous.Anonymous.Offset = (offset & 0xffff_ffff) as u32;
            overlapped.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
            let (started, start_charge) = operations.attempt(|| {
                // SAFETY: the spare storage remains live through terminal completion,
                // including cancellation, and ReadFile writes at most `length` bytes.
                let started = unsafe {
                    windows_sys::Win32::Storage::FileSystem::ReadFile(
                        handle.0,
                        destination.as_mut_ptr().cast(),
                        length as u32,
                        std::ptr::null_mut(),
                        (&raw mut overlapped).cast(),
                    )
                };
                if started == 0 {
                    Err(windows::core::Error::from_thread())
                } else {
                    Ok(())
                }
            });
            let pending = started
                .as_ref()
                .is_err_and(|error| error.code() == ERROR_IO_PENDING.to_hresult());
            let file = Arc::clone(file);
            drop(held);
            let mut interrupted = None;
            if pending {
                loop {
                    #[cfg(test)]
                    acyclic_test_support::differential::record(("wait", 0_u64, 0_u64));
                    let status = meter
                        .examine(1, 0)
                        .map_err(crate::demand::native_demand_error)
                        .map(|()| {
                            // SAFETY: the event remains owned by this live read through the wait.
                            unsafe { WaitForSingleObject(*event, 10) }
                        });
                    match status {
                        Ok(WAIT_TIMEOUT) => continue,
                        Ok(WAIT_FAILED) => {
                            interrupted = Some(DemandError::Io(io::Error::last_os_error()));
                        }
                        Err(error) => interrupted = Some(error),
                        _ => {}
                    }
                    if interrupted.is_some() {
                        // SAFETY: the live request belongs to this held handle.
                        let _ = unsafe {
                            CancelIoEx(HANDLE(file.as_raw_handle()), Some(&raw const overlapped))
                        };
                    }
                    break;
                }
            }
            let mut transferred = 0;
            let (read, completion_charge) = if started.is_ok() || pending {
                operations.attempt(|| {
                    // SAFETY: waits for this request before its event or buffer can be destroyed.
                    unsafe {
                        GetOverlappedResult(
                            HANDLE(file.as_raw_handle()),
                            &raw const overlapped,
                            &raw mut transferred,
                            true,
                        )
                    }
                })
            } else {
                (started, Ok(()))
            };
            let recorded = bytes.record(transferred as usize);
            start_charge.map_err(crate::demand::native_demand_error)?;
            completion_charge.map_err(crate::demand::native_demand_error)?;
            recorded.map_err(crate::demand::native_demand_error)?;
            if let Some(error) = interrupted {
                return Err(error);
            }
            meter.check().map_err(crate::demand::native_demand_error)?;
            match read {
                Ok(()) => {
                    let count = transferred as usize;
                    if count > length {
                        return Err(io::Error::from(io::ErrorKind::InvalidData).into());
                    }
                    // SAFETY: the completed read initialized `count` bytes at the start of the spare capacity.
                    unsafe { data.set_len(data.len() + count) };
                    Ok(count)
                }
                Err(error) if error.code() == ERROR_HANDLE_EOF.to_hresult() => Ok(0),
                Err(_) if self.was_yielded() => Err(io::Error::other(Yielded).into()),
                Err(error) => Err(io::Error::from_raw_os_error(error.code().0 & 0xffff).into()),
            }
        })
    }
}

unsafe extern "system" fn yield_on_break(context: *mut std::ffi::c_void, _: bool) {
    // SAFETY: unregistering waits for this callback before destroying its state.
    let held = unsafe { &*context.cast::<YieldingHeld>() };
    if let Some(file) = held
        .file
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
    {
        // SAFETY: reads begun before taking the lock keep this handle alive through completion.
        let _ = unsafe { CancelIoEx(HANDLE(file.as_raw_handle()), None) };
    }
}
impl Drop for YieldingFile {
    fn drop(&mut self) {
        if let Some(wait) = self.wait.take() {
            // SAFETY: no callback survives blocking unregistration.
            let _ = unsafe { UnregisterWaitEx(wait, Some(INVALID_HANDLE_VALUE)) };
        }
        drop(
            self.held
                .file
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take(),
        );
        if !self.held.request.is_null() {
            // SAFETY: closing the handle completes the request; wait before freeing its storage.
            unsafe {
                WaitForSingleObject((*self.held.request).overlapped.hEvent, INFINITE);
                drop(Box::from_raw(self.held.request));
            }
        }
    }
}
impl HostRoot {
    pub(crate) fn open_file_yielding<W: SourceDemandReadWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<YieldingFile, DemandError> {
        let (parent, leaf) = wide_parent_leaf(path)?;
        let parent = self.open_dir_held(parent, work)?;
        let mut opened = |flags| {
            open_windows_at(
                &parent,
                leaf,
                fs::FILE_GENERIC_READ,
                nt::FILE_OPEN,
                nt::FILE_NON_DIRECTORY_FILE | flags,
                work,
            )
        };
        let file = match opened(nt::FILE_OPEN_REQUIRING_OPLOCK) {
            Ok(file) => file,
            Err(DemandError::Io(_)) => opened(nt::NTCREATEFILE_CREATE_OPTIONS(0))?,
            Err(error) => return Err(error),
        };
        YieldingFile::new(file, work)
    }
}

#[cfg(test)]
#[path = "yielding_baseline.rs"]
mod yielding_baseline;
