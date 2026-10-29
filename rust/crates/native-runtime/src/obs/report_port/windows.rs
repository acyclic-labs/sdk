use super::{ReportPort, identity};
use std::{
    io,
    os::windows::io::AsRawHandle as _,
    sync::atomic::{AtomicBool, Ordering},
};
use tokio::net::windows::named_pipe::ClientOptions;
use windows_sys::Win32::System::Pipes::GetNamedPipeServerProcessId;

static TAKEN: AtomicBool = AtomicBool::new(false);

// The original process owner listens before launch. A missing or busy pipe is
// a failed admission, never a readiness condition to retry.
pub(super) fn open() -> io::Result<Option<ReportPort>> {
    let Some(endpoint) = std::env::var_os("ACYCLIC_REPORT_PIPE") else {
        return Ok(None);
    };
    let generation = std::env::var("ACYCLIC_REPORT_GENERATION").map_err(io::Error::other)?;
    let owner = std::env::var("ACYCLIC_REPORT_OWNER_PID")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value != 0)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid report owner PID"))?;
    let expected = format!(r"\\.\pipe\acyclic-report-{generation}");
    if !identity(&generation) || endpoint.as_os_str() != std::ffi::OsStr::new(&expected) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "report endpoint does not match the selected launch",
        ));
    }
    if TAKEN.swap(true, Ordering::AcqRel) {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "selected report endpoint already taken",
        ));
    }
    connect(&endpoint, generation, owner).map(Some)
}

fn connect(endpoint: &std::ffi::OsStr, generation: String, owner: u32) -> io::Result<ReportPort> {
    let channel = ClientOptions::new().open(endpoint)?;
    let mut server = 0;
    // SAFETY: the asynchronous pipe owns the live handle; server is writable.
    #[expect(unsafe_code, reason = "verifies the original native pipe owner")]
    let queried =
        unsafe { GetNamedPipeServerProcessId(channel.as_raw_handle().cast(), &raw mut server) };
    if queried == 0 {
        return Err(io::Error::last_os_error());
    }
    if server != owner {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "report pipe belongs to another process owner",
        ));
    }
    ReportPort::new(generation, channel)
}

#[cfg(test)]
mod tests;
