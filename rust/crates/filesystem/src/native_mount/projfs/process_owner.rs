//! Process incarnation evidence; a PID alone never proves ownership or death.
use super::super::custody::{ProcessObservation, require_dead_incarnation};
use super::*;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use windows::Win32::Foundation::{
    ERROR_INVALID_PARAMETER, FILETIME, HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows::Win32::System::Threading::{
    GetCurrentProcess, GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    PROCESS_SYNCHRONIZE, WaitForSingleObject,
};

struct ProcessHandle {
    handle: OwnedHandle,
    close: crate::NativeWorkReservation,
}

impl Drop for ProcessHandle {
    fn drop(&mut self) {
        self.close.started_drop_io(WriteOperations.delta(1));
    }
}

fn creation_time(handle: HANDLE) -> std::io::Result<u64> {
    let mut creation = FILETIME::default();
    let mut exit = FILETIME::default();
    let mut kernel = FILETIME::default();
    let mut user = FILETIME::default();
    // SAFETY: the handle is held by the caller; all four output pointers are live.
    unsafe { GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user) }?;
    Ok(u64::from(creation.dwLowDateTime) | (u64::from(creation.dwHighDateTime) << 32))
}

pub(in crate::native_mount) fn current_process_owner(
    work: &crate::NativeMaterializationWork,
) -> Result<(u32, u64), NativeMountError> {
    // SAFETY: GetCurrentProcess returns the current process's borrowed pseudo-handle.
    let time = (&mut &*work).attempt_io(ReadOperations.delta(1), || {
        creation_time(unsafe { GetCurrentProcess() })
    })?;
    Ok((std::process::id(), time))
}

pub(in crate::native_mount) fn require_dead_process_owner(
    pid: u32,
    started: u64,
    work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    if pid == 0 || started == 0 {
        return require_dead_incarnation(pid, started, ProcessObservation::Unknown);
    }
    let close = work.reserve_work(WriteOperations.delta(1))?;
    let opened = (&mut &*work).attempt_io(ReadOperations.delta(1), || {
        // SAFETY: no borrowed pointers; success transfers one process handle.
        unsafe {
            OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                false,
                pid,
            )
        }
        .map(|handle| ProcessHandle {
            // SAFETY: a successful OpenProcess transfers one valid handle.
            handle: unsafe { OwnedHandle::from_raw_handle(handle.0) },
            close,
        })
        .map_err(std::io::Error::from)
    });
    let handle = match opened {
        Ok(handle) => handle,
        Err(crate::NativeTreeMaterializationError::Io(error))
            if error.raw_os_error() == i32::try_from(ERROR_INVALID_PARAMETER.0).ok() =>
        {
            return require_dead_incarnation(pid, started, ProcessObservation::Absent);
        }
        Err(crate::NativeTreeMaterializationError::Io(_)) => {
            return require_dead_incarnation(pid, started, ProcessObservation::Unknown);
        }
        Err(error) => return Err(error.into()),
    };
    let observed = (&mut &*work).attempt_io(ReadOperations.delta(1), || {
        creation_time(HANDLE(handle.handle.as_raw_handle()))
    })?;
    // A reused PID names a different process. The recorded incarnation is dead.
    work.charge(ReadOperations.delta(1))?;
    // SAFETY: the process handle is owned through this nonblocking observation.
    match unsafe { WaitForSingleObject(HANDLE(handle.handle.as_raw_handle()), 0) } {
        WAIT_OBJECT_0 => require_dead_incarnation(
            pid,
            started,
            ProcessObservation::Present {
                started: observed,
                exited: true,
            },
        ),
        WAIT_TIMEOUT => require_dead_incarnation(
            pid,
            started,
            ProcessObservation::Present {
                started: observed,
                exited: false,
            },
        ),
        _ => require_dead_incarnation(pid, started, ProcessObservation::Unknown),
    }
}
