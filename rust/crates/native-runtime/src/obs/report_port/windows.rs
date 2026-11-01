use super::{ReportAdmission, identity};
use std::{
    io,
    os::windows::{fs::OpenOptionsExt as _, io::AsRawHandle as _},
    sync::atomic::{AtomicBool, Ordering},
};
use windows_sys::Win32::{
    Storage::FileSystem::{FILE_FLAG_OVERLAPPED, SECURITY_IDENTIFICATION, SECURITY_SQOS_PRESENT},
    System::Pipes::GetNamedPipeServerProcessId,
};

static TAKEN: AtomicBool = AtomicBool::new(false);

// The original process owner listens before launch. A missing or busy pipe is
// a failed admission, never a readiness condition to retry.
pub(super) fn open() -> io::Result<Option<ReportAdmission>> {
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

fn connect(
    endpoint: &std::ffi::OsStr,
    generation: String,
    owner: u32,
) -> io::Result<ReportAdmission> {
    // Match the original byte-mode overlapped client admission, including
    // identification-only impersonation, without requiring a reactor yet.
    let channel = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .share_mode(0)
        .custom_flags(FILE_FLAG_OVERLAPPED | SECURITY_IDENTIFICATION | SECURITY_SQOS_PRESENT)
        .open(endpoint)?;
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
    Ok(ReportAdmission {
        generation,
        channel: channel.into(),
    })
}

#[cfg(test)]
mod tests;
