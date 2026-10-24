//! Detached service startup whose only inherited handle is its readiness pipe.
use std::os::windows::io::FromRawHandle;
use std::{fs::File, io, path::Path};
use windows_sys::Win32::{
    Foundation::INVALID_HANDLE_VALUE,
    System::{
        Console::{GetStdHandle, STD_OUTPUT_HANDLE, SetStdHandle},
        Threading::{CREATE_NEW_PROCESS_GROUP, DETACHED_PROCESS},
    },
};

/// Starts `executable` as a detached service whose standard output is the
/// write end of the returned readiness pipe, and which inherits nothing else.
pub(crate) fn spawn_service_process(executable: &Path) -> io::Result<File> {
    let (reader, writer) = io::pipe()?;
    crate::command::Command::new(executable)
        .args(["__service", crate::SERVICE_READY_ARGUMENT])
        .creation_flags(CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS)
        .stdin(crate::command::Stdio::null())
        .stdout(writer)
        .stderr(crate::command::Stdio::null())
        .spawn()?;
    Ok(File::from(std::os::windows::io::OwnedHandle::from(reader)))
}

pub(crate) fn take_standard_output() -> io::Result<Option<File>> {
    // SAFETY: querying a standard handle has no preconditions.
    let handle = unsafe { GetStdHandle(STD_OUTPUT_HANDLE) };
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        return Ok(None);
    }
    // SAFETY: clearing a standard handle has no preconditions; this process now solely owns the handle it held.
    if unsafe { SetStdHandle(STD_OUTPUT_HANDLE, std::ptr::null_mut()) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the handle is live and no longer reachable as standard output.
    Ok(Some(unsafe { File::from_raw_handle(handle) }))
}
