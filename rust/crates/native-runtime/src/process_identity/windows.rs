#![allow(
    unsafe_code,
    reason = "process identity and exit evidence require Win32 handle queries"
)]
use std::io;
use std::os::windows::io::{AsRawHandle, BorrowedHandle, FromRawHandle, OwnedHandle};
use windows_sys::Win32::Foundation::{
    ERROR_INVALID_PARAMETER, FILETIME, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, GetProcessTimes, INFINITE, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    PROCESS_SYNCHRONIZE, WaitForSingleObject,
};

/// Reads only the birth identity of a borrowed process handle.
pub fn process_started(process: &impl AsRawHandle) -> io::Result<u64> {
    let mut times = [FILETIME {
        dwLowDateTime: 0,
        dwHighDateTime: 0,
    }; 4];
    let [creation, exit, kernel, user] = &mut times;
    // SAFETY: the caller keeps the handle live; all four outputs are writable.
    if unsafe { GetProcessTimes(process.as_raw_handle().cast(), creation, exit, kernel, user) } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(u64::from(creation.dwHighDateTime) << 32 | u64::from(creation.dwLowDateTime))
}

/// The current process's birth identity, without opening or closing a handle.
pub fn current_process_started() -> io::Result<u64> {
    // SAFETY: the current-process pseudo-handle is borrowed, never closed.
    let process = unsafe { BorrowedHandle::borrow_raw(GetCurrentProcess().cast()) };
    process_started(&process)
}

/// One stable process handle, retained across identity and exit observations.
pub struct ProcessProbe(OwnedHandle);

impl ProcessProbe {
    pub(crate) fn from_process(process: &impl AsRawHandle) -> io::Result<Self> {
        // SAFETY: the caller retains the borrowed process through duplication.
        let process = unsafe { BorrowedHandle::borrow_raw(process.as_raw_handle()) };
        Ok(Self(process.try_clone_to_owned()?))
    }

    /// `None` requires OS proof that this nonzero PID does not exist.
    pub fn open(pid: u32) -> io::Result<Option<Self>> {
        if pid == 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        // SAFETY: no borrowed inputs; success transfers one process handle.
        let process = unsafe {
            OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                0,
                pid,
            )
        };
        if process.is_null() {
            return classify_open_error(io::Error::last_os_error());
        }
        // SAFETY: OpenProcess returned one new owned handle.
        Ok(Some(Self(unsafe {
            OwnedHandle::from_raw_handle(process.cast())
        })))
    }

    /// Reads the birth time through this stable handle.
    pub fn started(&self) -> io::Result<u64> {
        process_started(&self.0)
    }

    /// Nonblocking exit evidence through the same handle.
    pub fn exited(&self) -> io::Result<bool> {
        self.wait(0)
    }

    /// Waits for this process, without reopening its possibly reused PID.
    pub fn await_exit(&self) -> io::Result<()> {
        self.wait(INFINITE).and_then(|exited| {
            if exited {
                Ok(())
            } else {
                Err(io::ErrorKind::WouldBlock.into())
            }
        })
    }

    fn wait(&self, milliseconds: u32) -> io::Result<bool> {
        // SAFETY: the owned handle remains open for this wait.
        match unsafe { WaitForSingleObject(self.0.as_raw_handle().cast(), milliseconds) } {
            WAIT_OBJECT_0 => Ok(true),
            WAIT_TIMEOUT => Ok(false),
            _ => Err(io::Error::last_os_error()),
        }
    }
}

fn classify_open_error(error: io::Error) -> io::Result<Option<ProcessProbe>> {
    if error.raw_os_error() == i32::try_from(ERROR_INVALID_PARAMETER).ok() {
        Ok(None)
    } else {
        Err(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn an_open_failure_proves_absence_only_for_an_unknown_pid(code in 1..i32::MAX) {
            let absent = matches!(classify_open_error(io::Error::from_raw_os_error(code)), Ok(None));
            prop_assert_eq!(absent, code == ERROR_INVALID_PARAMETER.cast_signed());
        }
    }

    #[test]
    fn current_process_probe_and_pseudo_handle_agree_and_access_denied_is_not_death() {
        let probe = ProcessProbe::open(std::process::id()).unwrap().unwrap();
        assert_eq!(probe.started().unwrap(), current_process_started().unwrap());
        assert!(!probe.exited().unwrap());
        assert!(ProcessProbe::open(0).is_err());
        assert!(classify_open_error(io::Error::from_raw_os_error(5)).is_err());
        assert!(matches!(
            classify_open_error(io::Error::from_raw_os_error(87)),
            Ok(None)
        ));
    }
}
