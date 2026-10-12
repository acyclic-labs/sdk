//! Same-user Windows named-pipe connections.

use super::*;

/// Connects to the service's pipe, trying `attempts` times while none
/// exists. A pipe whose every instance is connected is busy only until the
/// service creates the next, so that wait counts no attempt.
#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "waiting for a busy pipe needs the WaitNamedPipeW Win32 call"
)]
pub(crate) async fn connect_windows_control_pipe(
    data: &Path,
    attempts: usize,
    deadline: tokio::time::Instant,
) -> Result<tokio::net::windows::named_pipe::NamedPipeClient, ControlRequestError> {
    use windows_sys::Win32::Foundation::ERROR_PIPE_BUSY;
    use windows_sys::Win32::System::Pipes::WaitNamedPipeW;

    let pipe = windows_control_pipe_path(data);
    let mut attempt = 1;
    loop {
        let remaining = remaining_control_wait(deadline)?;
        let error = match tokio::net::windows::named_pipe::ClientOptions::new().open(&pipe) {
            // Only this user's own service may answer.
            Ok(client) => {
                return match windows_pipe_server_is_this_user(&client) {
                    Ok(true) => Ok(client),
                    Ok(false) => Err(ControlRequestError::Unavailable(
                        "Acyclic service pipe is served by another user".to_owned(),
                    )),
                    Err(error) => Err(ControlRequestError::Unavailable(format!(
                        "cannot identify the Acyclic service pipe's server: {error}"
                    ))),
                };
            }
            Err(error) => error,
        };
        if error.raw_os_error() == i32::try_from(ERROR_PIPE_BUSY).ok() {
            let name = pipe.encode_utf16().chain([0]).collect::<Vec<_>>();
            let wait = u32::try_from(remaining.as_millis())
                .unwrap_or(u32::MAX)
                .max(1);
            // A failed wait (the pipe vanished) shows in the next open.
            // SAFETY: `name` is a live NUL-terminated UTF-16 string that the
            // task owns for the whole call.
            let _ =
                tokio::task::spawn_blocking(move || unsafe { WaitNamedPipeW(name.as_ptr(), wait) })
                    .await;
            continue;
        }
        if attempt >= attempts {
            return Err(ControlRequestError::Unavailable(format!(
                "Acyclic service is not running: {error}"
            )));
        }
        attempt += 1;
        tokio::time::sleep(std::time::Duration::from_millis(20).min(remaining)).await;
    }
}

/// Whether the process serving `client`'s pipe runs as this process's user.
#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "queries the pipe's server process and both processes' token users"
)]
pub(crate) fn windows_pipe_server_is_this_user(
    client: &tokio::net::windows::named_pipe::NamedPipeClient,
) -> io::Result<bool> {
    use std::os::windows::io::AsRawHandle as _;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::Pipes::GetNamedPipeServerProcessId;
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    let mut server = 0_u32;
    // SAFETY: the pipe handle is live for the call and `server` is writable.
    if unsafe { GetNamedPipeServerProcessId(client.as_raw_handle() as HANDLE, &mut server) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: opening a process by id has no preconditions; a null handle is
    // an error.
    let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, server) };
    if process.is_null() {
        return Err(io::Error::last_os_error());
    }
    let server_user = windows_token_user(process);
    // SAFETY: `process` was opened above and is closed once.
    unsafe { CloseHandle(process) };
    // SAFETY: the pseudo-handle of this process needs no closing.
    let own_user = windows_token_user(unsafe { GetCurrentProcess() })?;
    Ok(server_user? == own_user)
}

/// The security identifier of the user `process` runs as, as bytes.
#[cfg(windows)]
#[allow(unsafe_code, reason = "reads a process token's user")]
pub(crate) fn windows_token_user(
    process: windows_sys::Win32::Foundation::HANDLE,
) -> io::Result<Vec<u8>> {
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::Security::{
        GetLengthSid, GetTokenInformation, TOKEN_QUERY, TOKEN_USER, TokenUser,
    };
    use windows_sys::Win32::System::Threading::OpenProcessToken;

    let mut token: HANDLE = std::ptr::null_mut();
    // SAFETY: `process` is a live process handle and `token` is writable.
    if unsafe { OpenProcessToken(process, TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let user = (|| {
        let mut length = 0_u32;
        // SAFETY: a null buffer of length zero asks only for the length.
        unsafe { GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &mut length) };
        let words = usize::try_from(length)
            .map_err(io::Error::other)?
            .div_ceil(8);
        let mut buffer = vec![0_u64; words.max(1)];
        // SAFETY: `buffer` holds `length` writable, 8-aligned bytes.
        if unsafe {
            GetTokenInformation(
                token,
                TokenUser,
                buffer.as_mut_ptr().cast(),
                length,
                &mut length,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the call filled a TOKEN_USER at the start of `buffer`,
        // whose SID points within it.
        let sid = unsafe { (*buffer.as_ptr().cast::<TOKEN_USER>()).User.Sid };
        // SAFETY: `sid` is a valid SID within `buffer`.
        let bytes = unsafe { GetLengthSid(sid) };
        let bytes = usize::try_from(bytes).map_err(io::Error::other)?;
        // SAFETY: the SID spans `bytes` readable bytes within `buffer`.
        Ok(unsafe { std::slice::from_raw_parts(sid.cast::<u8>(), bytes) }.to_vec())
    })();
    // SAFETY: `token` was opened above and is closed once.
    unsafe { CloseHandle(token) };
    user
}

#[cfg(windows)]
pub(crate) fn windows_control_pipe_path(data: &Path) -> String {
    format!(
        r"\\.\pipe\acyclic-{}",
        short_hash(data.as_os_str().to_string_lossy().as_bytes())
    )
}
