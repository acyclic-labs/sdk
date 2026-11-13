use super::PeerCredentials;
use std::{io, os::unix::fs::MetadataExt as _, path::Path};

#[cfg(not(target_vendor = "apple"))]
pub(super) fn endpoint(directory: &Path) -> io::Result<std::path::PathBuf> {
    Ok(directory.join("node.sock"))
}

#[cfg(target_vendor = "apple")]
#[allow(
    unsafe_code,
    reason = "confstr supplies the OS-managed per-user private temporary directory"
)]
pub(super) fn endpoint(directory: &Path) -> io::Result<std::path::PathBuf> {
    use base64::Engine as _;
    use std::os::unix::ffi::OsStrExt as _;
    // SAFETY: a null buffer and zero length query the required allocation.
    let length = unsafe { libc::confstr(libc::_CS_DARWIN_USER_TEMP_DIR, std::ptr::null_mut(), 0) };
    if length == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut bytes = vec![0; length];
    // SAFETY: the owned buffer has exactly the queried capacity including NUL.
    let actual = unsafe {
        libc::confstr(
            libc::_CS_DARWIN_USER_TEMP_DIR,
            bytes.as_mut_ptr().cast(),
            length,
        )
    };
    if actual != length || bytes.pop() != Some(0) {
        return Err(io::ErrorKind::InvalidData.into());
    }
    let root = Path::new(std::ffi::OsStr::from_bytes(&bytes));
    crate::private_file::validate_directory(root)?;
    let mut digest = blake3::Hasher::new_derive_key("acyclic.native-runtime.ipc.endpoint.v1");
    digest.update(directory.as_os_str().as_encoded_bytes());
    let name =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest.finalize().as_bytes());
    Ok(root.join(name))
}

/// Linux resolves the short sockaddr through a held directory descriptor, so
/// even a deeply nested private state path needs no shared-name rendezvous.
#[cfg(target_os = "linux")]
pub(crate) fn socket_path(path: &Path) -> io::Result<(std::fs::File, std::path::PathBuf)> {
    use std::os::{fd::AsRawFd as _, unix::fs::OpenOptionsExt as _};
    let parent = path.parent().ok_or(io::ErrorKind::InvalidInput)?;
    let name = path.file_name().ok_or(io::ErrorKind::InvalidInput)?;
    let held = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
        .open(parent)?;
    let address =
        std::path::PathBuf::from(format!("/proc/self/fd/{}", held.as_raw_fd())).join(name);
    Ok((held, address))
}

/// An owner-private Unix endpoint. Kernel credentials are checked before an
/// accepted connection reaches its protocol. The owner controls path lifetime.
pub struct UnixListener {
    listener: tokio::net::UnixListener,
    owner: PeerCredentials,
    path: std::path::PathBuf,
    identity: (u64, u64),
}

impl UnixListener {
    /// Bind a new socket beneath an existing owner-private directory. A prior
    /// entry is never read, replaced, or treated as an endpoint descriptor.
    pub fn bind(path: &Path) -> io::Result<Self> {
        use std::os::unix::fs::PermissionsExt as _;
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "local socket path must be absolute",
            ));
        }
        let owner = PeerCredentials::current()?;
        let parent = std::fs::symlink_metadata(path.parent().ok_or(io::ErrorKind::InvalidInput)?)?;
        if !parent.is_dir()
            || parent.uid() != crate::private_file::effective_user()
            || parent.mode() & 0o077 != 0
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "local endpoint directory is not owner-private",
            ));
        }
        // The directory denies access while the socket's mode is established.
        #[cfg(target_os = "linux")]
        let (_parent, address) = socket_path(path)?;
        #[cfg(not(target_os = "linux"))]
        let address = path;
        let listener = tokio::net::UnixListener::bind(&address)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        let metadata = owner.socket_metadata(path)?;
        Ok(Self {
            listener,
            owner,
            path: path.to_owned(),
            identity: (metadata.dev(), metadata.ino()),
        })
    }

    /// Accept one same-user peer. A rejected connection is dropped without
    /// closing the listener, and cancellation retains its pending accept.
    pub async fn accept(&self) -> io::Result<tokio::net::UnixStream> {
        let (stream, _) = self.listener.accept().await?;
        PeerCredentials::from_unix(&stream)?.require_same_user(&self.owner)?;
        Ok(stream)
    }
}

impl Drop for UnixListener {
    fn drop(&mut self) {
        if let Ok(metadata) = self.owner.socket_metadata(&self.path)
            && (metadata.dev(), metadata.ino()) == self.identity
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
