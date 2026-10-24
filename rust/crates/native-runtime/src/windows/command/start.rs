//! The one `CreateProcessW` call in this workspace. A child inherits exactly
//! its own standard handles, listed in `PROC_THREAD_ATTRIBUTE_HANDLE_LIST`,
//! and nothing else this process holds: not the host's standard handles
//! unless it asked for them, not a sibling's pipes, not a lock. No handle of
//! this process changes: each listed handle is an inheritable duplicate
//! made for this start and closed after it.

use std::ffi::c_void;
use std::io;
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
use windows_sys::Win32::Foundation::{
    DUPLICATE_SAME_ACCESS, DuplicateHandle, ERROR_INVALID_HANDLE, HANDLE, INVALID_HANDLE_VALUE,
};
use windows_sys::Win32::System::Threading::{
    CREATE_UNICODE_ENVIRONMENT, CreateProcessW, DeleteProcThreadAttributeList,
    EXTENDED_STARTUPINFO_PRESENT, GetCurrentProcess, InitializeProcThreadAttributeList,
    LPPROC_THREAD_ATTRIBUTE_LIST, PROC_THREAD_ATTRIBUTE_HANDLE_LIST, PROCESS_INFORMATION,
    STARTF_USESTDHANDLES, STARTUPINFOEXW, UpdateProcThreadAttribute,
};

/// What [`start`] creates: an application, its command line and the three
/// standard handles it receives, each possibly null.
pub(crate) struct Start<'a> {
    /// The NUL-terminated application path.
    pub(crate) application: &'a [u16],
    /// The NUL-terminated command line, which `CreateProcessW` may modify.
    pub(crate) command_line: &'a mut [u16],
    /// Creation flags; Unicode environment is always added.
    pub(crate) flags: u32,
    /// A complete environment block, or `None` for this process's own.
    pub(crate) environment: Option<&'a [u16]>,
    /// The NUL-terminated working directory, or `None` for this process's.
    pub(crate) directory: Option<&'a [u16]>,
    /// Standard input, output and error. A null, invalid or already closed
    /// handle gives the child none in that slot; it never fails the start.
    pub(crate) stdio: [HANDLE; 3],
}

/// A started process.
pub(crate) struct Started {
    pub(crate) process: OwnedHandle,
    pub(crate) id: u32,
}

/// An inheritable duplicate of `handle` made for one start, or `None` when
/// `handle` names nothing this process holds.
fn inheritable(handle: HANDLE) -> io::Result<Option<OwnedHandle>> {
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        return Ok(None);
    }
    let mut duplicate: HANDLE = std::ptr::null_mut();
    // SAFETY: duplicates within this process; a stale value fails with
    // ERROR_INVALID_HANDLE and changes nothing.
    let duplicated = unsafe {
        DuplicateHandle(
            GetCurrentProcess(),
            handle,
            GetCurrentProcess(),
            &raw mut duplicate,
            0,
            1,
            DUPLICATE_SAME_ACCESS,
        )
    };
    if duplicated == 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(ERROR_INVALID_HANDLE.cast_signed()) {
            return Ok(None);
        }
        return Err(error);
    }
    // SAFETY: DuplicateHandle returned a new handle this process owns.
    Ok(Some(unsafe { OwnedHandle::from_raw_handle(duplicate) }))
}

/// Snapshot all inherited slots before any duplicate or pipe can recycle a
/// closed slot's numeric value. Validation itself allocates no handles.
pub(super) fn pin_inherited(mut slots: [HANDLE; 3]) -> io::Result<[Option<OwnedHandle>; 3]> {
    for slot in &mut slots {
        if slot.is_null() || *slot == INVALID_HANDLE_VALUE {
            *slot = std::ptr::null_mut();
            continue;
        }
        let mut flags = 0;
        // SAFETY: queries a raw value without taking ownership or allocating.
        if unsafe { windows_sys::Win32::Foundation::GetHandleInformation(*slot, &raw mut flags) }
            == 0
        {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(ERROR_INVALID_HANDLE.cast_signed()) {
                return Err(error);
            }
            *slot = std::ptr::null_mut();
        }
    }
    let [input, output, error] = slots;
    Ok([
        inheritable(input)?,
        inheritable(output)?,
        inheritable(error)?,
    ])
}

/// An initialized attribute list holding one handle list.
struct HandleList {
    buffer: Vec<u8>,
}

impl HandleList {
    /// Lists `handles`, which must outlive the list.
    fn new(handles: &[HANDLE]) -> io::Result<Self> {
        let mut size = 0;
        // SAFETY: a null list with a size output only queries the size; the
        // documented probe fails with ERROR_INSUFFICIENT_BUFFER.
        unsafe { InitializeProcThreadAttributeList(std::ptr::null_mut(), 1, 0, &raw mut size) };
        let mut buffer = vec![0u8; size];
        // SAFETY: the buffer holds the size the probe returned.
        if unsafe {
            InitializeProcThreadAttributeList(buffer.as_mut_ptr().cast(), 1, 0, &raw mut size)
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        // Owned from here, so a failed update still deletes the list.
        let mut list = Self { buffer };
        // SAFETY: an initialized list; `handles` outlives it per the contract.
        if unsafe {
            UpdateProcThreadAttribute(
                list.raw(),
                0,
                PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                handles.as_ptr().cast::<c_void>(),
                std::mem::size_of_val(handles),
                std::ptr::null_mut(),
                std::ptr::null(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(list)
    }

    fn raw(&mut self) -> LPPROC_THREAD_ATTRIBUTE_LIST {
        self.buffer.as_mut_ptr().cast()
    }
}

impl Drop for HandleList {
    fn drop(&mut self) {
        // SAFETY: the list was initialized in `new`.
        unsafe { DeleteProcThreadAttributeList(self.raw()) };
    }
}

/// Creates the process `request` describes.
pub(crate) fn start(request: Start<'_>) -> io::Result<Started> {
    let Start {
        application,
        command_line,
        flags,
        environment,
        directory,
        stdio: [input, output, error],
    } = request;
    let stdio = [
        inheritable(input)?,
        inheritable(output)?,
        inheritable(error)?,
    ];
    let listed: Vec<HANDLE> = stdio
        .iter()
        .flatten()
        .map(|handle| handle.as_raw_handle())
        .collect();
    let raw = |slot: &Option<OwnedHandle>| {
        slot.as_ref()
            .map_or(std::ptr::null_mut(), |handle| handle.as_raw_handle())
    };
    // SAFETY: zero is the documented initial state of these structures.
    let mut startup: STARTUPINFOEXW = unsafe { std::mem::zeroed() };
    let [input, output, error] = &stdio;
    if !listed.is_empty() {
        startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
        startup.StartupInfo.hStdInput = raw(input);
        startup.StartupInfo.hStdOutput = raw(output);
        startup.StartupInfo.hStdError = raw(error);
    }
    let mut list = if listed.is_empty() {
        None
    } else {
        Some(HandleList::new(&listed)?)
    };
    let mut flags = flags | CREATE_UNICODE_ENVIRONMENT;
    if let Some(list) = list.as_mut() {
        startup.lpAttributeList = list.raw();
        flags |= EXTENDED_STARTUPINFO_PRESENT;
        startup.StartupInfo.cb =
            u32::try_from(size_of::<STARTUPINFOEXW>()).map_err(|_| io::ErrorKind::InvalidInput)?;
    } else {
        startup.StartupInfo.cb = u32::try_from(size_of_val(&startup.StartupInfo))
            .map_err(|_| io::ErrorKind::InvalidInput)?;
    }
    // SAFETY: zero is the documented initial state of this output.
    let mut process: PROCESS_INFORMATION = unsafe { std::mem::zeroed() };
    // SAFETY: the application, command line, environment and directory are
    // live and NUL-terminated; inheritance is limited to the listed handles,
    // which outlive the call, and is off when there are none.
    let created = unsafe {
        CreateProcessW(
            application.as_ptr(),
            command_line.as_mut_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            i32::from(list.is_some()),
            flags,
            environment.map_or(std::ptr::null(), |block| block.as_ptr().cast::<c_void>()),
            directory.map_or(std::ptr::null(), <[u16]>::as_ptr),
            &raw const startup.StartupInfo,
            &raw mut process,
        )
    };
    if created == 0 {
        return Err(io::Error::last_os_error());
    }
    drop(list);
    drop(stdio);
    // SAFETY: successful creation returned two handles this process owns;
    // the thread's is not needed.
    let (process_handle, thread) = unsafe {
        (
            OwnedHandle::from_raw_handle(process.hProcess),
            OwnedHandle::from_raw_handle(process.hThread),
        )
    };
    drop(thread);
    Ok(Started {
        process: process_handle,
        id: process.dwProcessId,
    })
}
