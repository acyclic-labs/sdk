//! Peer identities come from the connected OS endpoint, never a message or
//! rendezvous file. Compare them with the owner captured before accepting work.
//! Windows pipe callers must also create an owner-only protected DACL.
use std::io;

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "queries kernel pipe peers and their held process tokens"
)]
mod windows;
#[cfg(windows)]
pub use windows::PipeListener;

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::UnixListener;
#[cfg(target_os = "linux")]
pub(crate) use unix::socket_path;

/// Stable local endpoint for a service's private directory. No shared temporary
/// namespace or credential file participates in address selection.
pub fn endpoint(directory: &std::path::Path) -> io::Result<std::path::PathBuf> {
    crate::private_file::validate_directory(directory)?;
    #[cfg(unix)]
    {
        unix::endpoint(directory)
    }
    #[cfg(windows)]
    {
        let mut digest = blake3::Hasher::new_derive_key("acyclic.native-runtime.ipc.endpoint.v1");
        digest.update(directory.as_os_str().as_encoded_bytes());
        let name = digest.finalize().to_hex();
        Ok(std::path::PathBuf::from(format!(
            r"\\.\pipe\acyclic-ipc-{name}"
        )))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum User {
    #[cfg(unix)]
    Unix(u32),
    #[cfg(windows)]
    Windows(Vec<u8>),
}

/// OS credentials of an original connected peer. No caller-supplied identity
/// or bearer is accepted as a substitute for the kernel query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerCredentials {
    user: User,
    process: Option<u32>,
}

impl PeerCredentials {
    /// Capture the process owner's identity before opening or serving an endpoint.
    pub fn current() -> io::Result<Self> {
        #[cfg(unix)]
        let user = User::Unix(crate::private_file::effective_user());
        #[cfg(windows)]
        let user = windows::current_user()?;
        Ok(Self {
            user,
            process: Some(std::process::id()),
        })
    }

    /// The peer PID when this OS supplies it. The Windows adapter retains the
    /// process handle while reading its token; this value alone is not authority.
    #[must_use]
    pub const fn process_id(&self) -> Option<u32> {
        self.process
    }

    /// Admit only a peer whose kernel user matches the original endpoint owner.
    pub fn require_same_user(&self, owner: &Self) -> io::Result<()> {
        if self.user != owner.user {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "local peer belongs to another OS user",
            ));
        }
        Ok(())
    }

    /// Query the connected Unix socket on either end. Tokio uses the platform's
    /// native peer-credential operation, including Linux and macOS.
    #[cfg(unix)]
    pub fn from_unix(stream: &tokio::net::UnixStream) -> io::Result<Self> {
        let peer = stream.peer_cred()?;
        Ok(Self {
            user: User::Unix(peer.uid()),
            process: peer
                .pid()
                .map(u32::try_from)
                .transpose()
                .map_err(io::Error::other)?,
        })
    }

    /// Connect to an absolute, owner-private socket, checking both its path
    /// identity and the connected kernel peer before returning any channel.
    #[cfg(unix)]
    pub async fn connect_unix(&self, path: &std::path::Path) -> io::Result<tokio::net::UnixStream> {
        use std::os::unix::fs::MetadataExt as _;
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "local socket path must be absolute",
            ));
        }
        let before = self.socket_metadata(path)?;
        #[cfg(target_os = "linux")]
        let (_parent, address) = unix::socket_path(path)?;
        #[cfg(not(target_os = "linux"))]
        let address = path;
        let stream = tokio::net::UnixStream::connect(&address).await?;
        Self::from_unix(&stream)?.require_same_user(self)?;
        let after = self.socket_metadata(path)?;
        if (before.dev(), before.ino()) != (after.dev(), after.ino()) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "local socket identity changed during connect",
            ));
        }
        Ok(stream)
    }

    #[cfg(unix)]
    fn socket_metadata(&self, path: &std::path::Path) -> io::Result<std::fs::Metadata> {
        use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};
        let metadata = std::fs::symlink_metadata(path)?;
        let User::Unix(uid) = &self.user;
        if !metadata.file_type().is_socket()
            || metadata.uid() != *uid
            || metadata.mode() & 0o077 != 0
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "local endpoint is not an owner-private socket",
            ));
        }
        Ok(metadata)
    }

    /// Remove a stale owner-private socket after the caller has acquired
    /// exclusive custody of its endpoint namespace. Regular files and aliases
    /// are never consumed as recovery records.
    #[cfg(unix)]
    pub fn retire_unix(&self, path: &std::path::Path) -> io::Result<()> {
        match self.socket_metadata(path) {
            Ok(_) => std::fs::remove_file(path),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Query the client connected to a local named-pipe server handle.
    /// The pipe must reject remote clients and carry an owner-only DACL.
    #[cfg(windows)]
    pub fn from_pipe_server(pipe: &impl std::os::windows::io::AsRawHandle) -> io::Result<Self> {
        windows::peer(pipe, true)
    }

    /// Query the server connected to a local named-pipe client handle.
    /// The caller must also verify custody of the held endpoint's security descriptor.
    #[cfg(windows)]
    pub fn from_pipe_client(pipe: &impl std::os::windows::io::AsRawHandle) -> io::Result<Self> {
        windows::peer(pipe, false)
    }

    /// Connect to an owner-private local pipe and verify its kernel server identity.
    /// Contention waits on the native pipe-availability event. An absent owner
    /// is an error; starting one belongs to the endpoint owner.
    #[cfg(windows)]
    pub async fn connect_pipe(
        &self,
        name: &std::ffi::OsStr,
    ) -> io::Result<tokio::net::windows::named_pipe::NamedPipeClient> {
        if !name.to_string_lossy().starts_with(r"\\.\pipe\") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "pipe address must be local",
            ));
        }
        let pipe = windows::connect(name).await?;
        Self::from_pipe_client(&pipe)?.require_same_user(self)?;
        Ok(pipe)
    }
}

#[cfg(test)]
mod tests;
