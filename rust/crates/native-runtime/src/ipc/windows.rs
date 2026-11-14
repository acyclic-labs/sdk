use super::{PeerCredentials, User};
use std::{
    io,
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
};
use windows_sys::Win32::System::{
    Pipes::{
        GetNamedPipeClientProcessId, GetNamedPipeServerProcessId, NMPWAIT_WAIT_FOREVER,
        WaitNamedPipeW,
    },
    Threading::{GetCurrentProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION},
};

pub(super) async fn connect(
    name: &std::ffi::OsStr,
) -> io::Result<tokio::net::windows::named_pipe::NamedPipeClient> {
    use std::os::windows::ffi::OsStrExt as _;
    loop {
        match tokio::net::windows::named_pipe::ClientOptions::new().open(name) {
            Err(error)
                if error.raw_os_error()
                    == Some(windows_sys::Win32::Foundation::ERROR_PIPE_BUSY.cast_signed()) =>
            {
                let name: Vec<u16> = name.encode_wide().chain(Some(0)).collect();
                crate::spawn_blocking(move || {
                    // SAFETY: the NUL-terminated name remains owned for the
                    // entire OS wait. No timeout or periodic probe is used.
                    if unsafe { WaitNamedPipeW(name.as_ptr(), NMPWAIT_WAIT_FOREVER) } == 0 {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(())
                    }
                })
                .await??;
            }
            result => return result,
        }
    }
}

pub(super) fn current_user() -> io::Result<User> {
    // SAFETY: the current-process pseudo-handle is borrowed and never closed.
    crate::private_file::process_user_id(unsafe { GetCurrentProcess() }).map(User::Windows)
}

pub(super) fn peer(pipe: &impl AsRawHandle, server: bool) -> io::Result<PeerCredentials> {
    if !crate::private_file::is_private_security(pipe.as_raw_handle())? {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "local pipe is not owner-private",
        ));
    }
    let mut pid = 0;
    // SAFETY: the caller retains the original pipe handle throughout both queries.
    // Invalid handles fail at the OS boundary before any identity is admitted.
    let queried = unsafe {
        if server {
            GetNamedPipeClientProcessId(pipe.as_raw_handle(), &raw mut pid)
        } else {
            GetNamedPipeServerProcessId(pipe.as_raw_handle(), &raw mut pid)
        }
    };
    if queried == 0 {
        return Err(io::Error::last_os_error());
    }
    if pid == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "pipe peer has no process identity",
        ));
    }
    // SAFETY: querying this PID returns a new owned process handle or null.
    let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if process.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: this successful OpenProcess handle is transferred exactly once.
    let process = unsafe { OwnedHandle::from_raw_handle(process) };
    let user = User::Windows(crate::private_file::process_user_id(
        process.as_raw_handle(),
    )?);
    Ok(PeerCredentials {
        user,
        process: Some(pid),
    })
}

/// Owner-private named-pipe instances, rearmed before a connection is returned.
pub struct PipeListener {
    name: std::ffi::OsString,
    owner: PeerCredentials,
    pending: tokio::net::windows::named_pipe::NamedPipeServer,
}

fn create(
    name: &std::ffi::OsStr,
    first: bool,
) -> io::Result<tokio::net::windows::named_pipe::NamedPipeServer> {
    crate::private_file::with_private_security(|attributes| {
        // SAFETY: the protected descriptor remains live until creation returns.
        unsafe {
            tokio::net::windows::named_pipe::ServerOptions::new()
                .first_pipe_instance(first)
                .reject_remote_clients(true)
                .create_with_security_attributes_raw(name, attributes)
        }
    })
}

impl PipeListener {
    /// Claim the initial local pipe name with an owner-only protected DACL.
    pub fn bind(name: &std::ffi::OsStr) -> io::Result<Self> {
        if !name.to_string_lossy().starts_with(r"\\.\pipe\") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "pipe address must be local",
            ));
        }
        Ok(Self {
            name: name.to_owned(),
            owner: PeerCredentials::current()?,
            pending: create(name, true)?,
        })
    }

    /// Accept a kernel-authenticated same-user client and retain a listening
    /// instance for the next connection. Cancellation retains the original instance.
    pub async fn accept(&mut self) -> io::Result<tokio::net::windows::named_pipe::NamedPipeServer> {
        self.pending.connect().await?;
        let next = create(&self.name, false)?;
        let connected = std::mem::replace(&mut self.pending, next);
        let peer = PeerCredentials::from_pipe_server(&connected)?;
        peer.require_same_user(&self.owner)?;
        Ok(connected)
    }
}
