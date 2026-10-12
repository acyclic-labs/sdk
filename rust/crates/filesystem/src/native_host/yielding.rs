use super::*;

/// A read failed because its file yielded to a rename or deletion.
#[derive(Debug)]
pub struct Yielded;

impl std::fmt::Display for Yielded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("the file gave way to a rename or delete")
    }
}

impl std::error::Error for Yielded {}

/// A read handle that yields to rename or deletion when its oplock breaks.
pub struct YieldingFile {
    held: std::sync::Arc<YieldingHeld>,
    /// The thread-pool wait that closes the handle once the oplock breaks.
    wait: Option<windows::Win32::Foundation::HANDLE>,
}

struct YieldingHeld {
    file: std::sync::Mutex<Option<std::sync::Arc<File>>>,
    // Pinned until kernel completion; null for an ordinary handle.
    request: *mut OplockRequest,
}

// SAFETY: the kernel request stays pinned until completion; handle access is locked.
unsafe impl Send for YieldingHeld {}

// SAFETY: as for `Send`; the file is behind a lock.
unsafe impl Sync for YieldingHeld {}

// SAFETY: the wait handle is only unregistered, once, by the owner.
unsafe impl Send for YieldingFile {}

// SAFETY: shared access touches only the locked file.
unsafe impl Sync for YieldingFile {}

#[repr(C)]
struct OplockRequest {
    overlapped: windows::Win32::System::IO::OVERLAPPED,
    input: windows::Win32::System::Ioctl::REQUEST_OPLOCK_INPUT_BUFFER,
    output: windows::Win32::System::Ioctl::REQUEST_OPLOCK_OUTPUT_BUFFER,
}

impl YieldingFile {
    pub(super) fn new<W: SourceDemandReadWork>(
        file: File,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        use std::os::windows::io::AsRawHandle as _;
        use windows::Win32::Foundation::{CloseHandle, ERROR_IO_PENDING, HANDLE};
        use windows::Win32::System::IO::DeviceIoControl;
        use windows::Win32::System::Ioctl::{
            FSCTL_REQUEST_OPLOCK, OPLOCK_LEVEL_CACHE_HANDLE, OPLOCK_LEVEL_CACHE_READ,
            REQUEST_OPLOCK_CURRENT_VERSION, REQUEST_OPLOCK_INPUT_BUFFER,
            REQUEST_OPLOCK_INPUT_FLAG_REQUEST, REQUEST_OPLOCK_OUTPUT_BUFFER,
        };
        use windows::Win32::System::Threading::{
            CreateEventW, INFINITE, RegisterWaitForSingleObject, WT_EXECUTEONLYONCE,
        };
        let handle = HANDLE(file.as_raw_handle());
        let mut yielding = Self::plain(file, work)?;
        let result = (|| {
            // SAFETY: event creation has no borrowed pointer arguments.
            let event =
                unsafe { CreateEventW(None, true, false, None) }.map_err(io::Error::other)?;
            let bytes = std::mem::size_of::<OplockRequest>() as u64;
            if let Err(error) = work.allocate(bytes) {
                unsafe {
                    let _ = CloseHandle(event);
                }
                return Err(error);
            }
            let request = Box::into_raw(Box::new(OplockRequest {
                overlapped: windows::Win32::System::IO::OVERLAPPED {
                    hEvent: event,
                    ..Default::default()
                },
                input: REQUEST_OPLOCK_INPUT_BUFFER {
                    StructureVersion: u16::try_from(REQUEST_OPLOCK_CURRENT_VERSION)
                        .unwrap_or(u16::MAX),
                    StructureLength: size_of::<REQUEST_OPLOCK_INPUT_BUFFER>() as u16,
                    RequestedOplockLevel: OPLOCK_LEVEL_CACHE_READ | OPLOCK_LEVEL_CACHE_HANDLE,
                    Flags: REQUEST_OPLOCK_INPUT_FLAG_REQUEST,
                },
                output: REQUEST_OPLOCK_OUTPUT_BUFFER::default(),
            }));
            if let Err(error) = work.started_io(READ) {
                unsafe {
                    let _ = CloseHandle(event);
                    drop(Box::from_raw(request));
                }
                work.release(bytes)?;
                return Err(error);
            }
            std::sync::Arc::get_mut(&mut yielding.held)
                .ok_or_else(|| io::Error::other("oplock state was unexpectedly shared"))?
                .request = request;
            // SAFETY: stable owned request and held handle remain alive until the oplock completes; YieldingFile::drop cancels and waits.
            let requested = unsafe {
                DeviceIoControl(
                    handle,
                    FSCTL_REQUEST_OPLOCK,
                    Some((&raw const (*request).input).cast()),
                    size_of::<REQUEST_OPLOCK_INPUT_BUFFER>() as u32,
                    Some((&raw mut (*request).output).cast()),
                    size_of::<REQUEST_OPLOCK_OUTPUT_BUFFER>() as u32,
                    None,
                    Some(&raw mut (*request).overlapped),
                )
            };
            if !requested.is_err_and(|error| error.code() == ERROR_IO_PENDING.to_hresult()) {
                std::sync::Arc::get_mut(&mut yielding.held)
                    .ok_or_else(|| io::Error::other("oplock state was unexpectedly shared"))?
                    .request = std::ptr::null_mut();
                unsafe {
                    let _ = CloseHandle(event);
                    drop(Box::from_raw(request));
                }
                work.release(bytes)?;
                return Ok(());
            }
            let mut wait = HANDLE::default();
            // SAFETY: the state outlives this registered callback, unregistered before state destruction by YieldingFile::drop.
            unsafe {
                RegisterWaitForSingleObject(
                    &raw mut wait,
                    event,
                    Some(yield_on_break),
                    Some(std::sync::Arc::as_ptr(&yielding.held).cast()),
                    INFINITE,
                    WT_EXECUTEONLYONCE,
                )
            }
            .map_err(io::Error::other)?;
            yielding.wait = Some(wait);
            Ok(())
        })();
        if let Err(error) = result {
            let bytes = yielding.logical_owned_bytes();
            drop(yielding);
            work.release(bytes)?;
            return Err(error);
        }
        Ok(yielding)
    }
    pub(super) fn plain<W: SourceDemandReadWork>(
        file: File,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        let file_bytes = std::mem::size_of::<File>() as u64;
        let held_bytes = std::mem::size_of::<YieldingHeld>() as u64;
        work.allocate(file_bytes)?;
        let file = std::sync::Arc::new(file);
        if let Err(error) = work.allocate(held_bytes) {
            drop(file);
            work.release(file_bytes)?;
            return Err(error);
        }
        Ok(Self {
            held: std::sync::Arc::new(YieldingHeld {
                file: std::sync::Mutex::new(Some(file)),
                request: std::ptr::null_mut(),
            }),
            wait: None,
        })
    }
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        (std::mem::size_of::<YieldingHeld>()
            + std::mem::size_of::<File>()
            + if self.held.request.is_null() {
                0
            } else {
                std::mem::size_of::<OplockRequest>()
            }) as u64
    }
    pub(crate) fn was_yielded(&self) -> bool {
        self.held
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none()
    }
    /// Runs `operation` on the handle unless it gave way.
    pub fn with<T, E: From<io::Error>>(
        &self,
        operation: impl FnOnce(&File) -> Result<T, E>,
    ) -> Result<T, E> {
        let file = self
            .held
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match file.as_ref() {
            Some(file) => operation(file),
            None => Err(io::Error::other(Yielded).into()),
        }
    }
    pub(crate) fn read_at<W: SourceDemandReadWork>(
        &self,
        offset: u64,
        destination: &mut [u8],
        work: &mut W,
    ) -> Result<usize, DemandError> {
        work.check()?;
        if destination.is_empty() {
            return Ok(0);
        }
        use std::os::windows::io::AsRawHandle as _;
        use windows::Win32::Foundation::{
            CloseHandle, ERROR_HANDLE_EOF, ERROR_IO_PENDING, HANDLE, WAIT_FAILED, WAIT_TIMEOUT,
        };
        use windows::Win32::Storage::FileSystem::ReadFile;
        use windows::Win32::System::IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED};
        use windows::Win32::System::Threading::{CreateEventW, WaitForSingleObject};
        work.with_native_read_meter(|meter| {
            let mut operations = acyclic_native_runtime::NativeOperationReservation::new(
                meter,
                acyclic_native_runtime::NativeOperationKind::Read,
                2,
            )
            .map_err(crate::demand::native_demand_error)?;
            let mut byte_reservation = acyclic_native_runtime::NativeByteReservation::new(
                meter,
                acyclic_native_runtime::NativeOperationKind::Read,
                destination.len().min(u32::MAX as usize),
            )
            .map_err(crate::demand::native_demand_error)?;
            let held = self
                .held
                .file
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let file = held.as_ref().ok_or_else(|| io::Error::other(Yielded))?;
            let handle = HANDLE(file.as_raw_handle());
            // SAFETY: no pointers are passed.
            let event =
                unsafe { CreateEventW(None, true, false, None) }.map_err(io::Error::other)?;
            let mut overlapped = OVERLAPPED {
                hEvent: event,
                ..Default::default()
            };
            let (low, high) = (offset & u64::from(u32::MAX), offset >> 32);
            overlapped.Anonymous.Anonymous.Offset = u32::try_from(low).unwrap_or(u32::MAX);
            overlapped.Anonymous.Anonymous.OffsetHigh = u32::try_from(high).unwrap_or(u32::MAX);
            let length = destination.len().min(u32::MAX as usize);
            let buffer = destination.get_mut(..length).unwrap_or_default();
            let mut transferred = 0;
            // SAFETY: the buffer and `overlapped` outlive the request, which is waited for before either goes.
            let mut pending = false;
            let (started, start_charge) = operations.attempt(|| {
                unsafe { ReadFile(handle, Some(buffer), None, Some(&raw mut overlapped)) }.or_else(
                    |error| {
                        if error.code() == ERROR_IO_PENDING.to_hresult() {
                            pending = true;
                            Ok(())
                        } else {
                            Err(error)
                        }
                    },
                )
            });
            let file = std::sync::Arc::clone(file);
            drop(held);
            let mut interrupted = None;
            if pending {
                loop {
                    if let Err(error) = meter
                        .examine(1, 0)
                        .map_err(crate::demand::native_demand_error)
                    {
                        interrupted = Some(error);
                        unsafe {
                            let _ = CancelIoEx(
                                HANDLE(file.as_raw_handle()),
                                Some(&raw const overlapped),
                            );
                        }
                        break;
                    }
                    let status = unsafe { WaitForSingleObject(event, 10) };
                    if status == WAIT_TIMEOUT {
                        continue;
                    }
                    if status == WAIT_FAILED {
                        interrupted = Some(DemandError::Io(io::Error::last_os_error()));
                        unsafe {
                            let _ = CancelIoEx(
                                HANDLE(file.as_raw_handle()),
                                Some(&raw const overlapped),
                            );
                        }
                    }
                    break;
                }
            }
            let mut completion_charge = Ok(());
            let read = started.and_then(|()| {
                let (read, recorded) = operations.attempt(|| unsafe {
                    GetOverlappedResult(
                        HANDLE(file.as_raw_handle()),
                        &raw const overlapped,
                        &raw mut transferred,
                        true,
                    )
                });
                completion_charge = recorded;
                read
            });
            // SAFETY: the request is complete; nothing waits on the event.
            let _ = unsafe { CloseHandle(event) };
            let bytes_recorded = byte_reservation.record(transferred as usize);
            start_charge.map_err(crate::demand::native_demand_error)?;
            completion_charge.map_err(crate::demand::native_demand_error)?;
            bytes_recorded.map_err(crate::demand::native_demand_error)?;
            if let Some(error) = interrupted {
                return Err(error);
            }
            meter.check().map_err(crate::demand::native_demand_error)?;
            match read {
                Ok(()) => Ok(transferred as usize),
                Err(error) if error.code() == ERROR_HANDLE_EOF.to_hresult() => Ok(0),
                Err(_)
                    if self
                        .held
                        .file
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .is_none() =>
                {
                    Err(DemandError::from(io::Error::other(Yielded)))
                }
                Err(error) => Err(DemandError::from(io::Error::from_raw_os_error(
                    error.code().0 & 0xFFFF,
                ))),
            }
        })
    }
}

impl Drop for YieldingFile {
    fn drop(&mut self) {
        use windows::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
        use windows::Win32::System::Threading::{INFINITE, UnregisterWaitEx, WaitForSingleObject};
        if let Some(wait) = self.wait.take() {
            // SAFETY: unregistration waits for every callback before destroying its state.
            let _ = unsafe { UnregisterWaitEx(wait, Some(INVALID_HANDLE_VALUE)) };
        }
        drop(
            self.held
                .file
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take(),
        );
        let request = self.held.request;
        if !request.is_null() {
            // SAFETY: closing the file completes the request; wait before freeing its storage.
            unsafe {
                let event = (*request).overlapped.hEvent;
                WaitForSingleObject(event, INFINITE);
                let _ = CloseHandle(event);
                drop(Box::from_raw(request));
            }
        }
    }
}

/// Closes the handle whose oplock broke, which lets the rename or delete
/// that broke it proceed.
unsafe extern "system" fn yield_on_break(context: *mut std::ffi::c_void, _timed_out: bool) {
    // SAFETY: the context is the held state, alive until this wait is
    // unregistered, which waits for this callback.
    let held = unsafe { &*context.cast::<YieldingHeld>() };
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::CancelIoEx;
    let file = held
        .file
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(file) = file {
        // SAFETY: the handle is live; reads were started before this lock
        // was acquired, and their waiters keep the handle alive.
        let _ = unsafe { CancelIoEx(HANDLE(file.as_raw_handle()), None) };
    }
}

impl HostRoot {
    pub(crate) fn open_file_yielding<W: SourceDemandReadWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<YieldingFile, DemandError> {
        use windows::{Wdk::Storage::FileSystem as nt, Win32::Storage::FileSystem as fs};
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
