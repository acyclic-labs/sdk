//! Capabilities supplied by an authenticated host policy at native launch.
//!
//! This module does not decide approvals. A model's command line, environment,
//! current directory or tool arguments cannot grant authority. Preparation is
//! per active execution; sharing a grant holds no process, thread or watcher.

use std::{
    collections::BTreeSet,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

/// Filesystem authority of an execution, including all its descendants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionAccess {
    /// Read and execute the granted paths; no workspace mutation.
    ReadOnly,
    /// Read and execute granted paths; mutate only the workspace subtree.
    WorkspaceWrite,
}

/// Network authority. An allowlist names TCP destination ports, on any address.
/// It is deliberately not represented as a hostname or address allowlist.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionNetwork {
    /// No sockets, including local IPC sockets, can be created or connected.
    Off,
    /// Ordinary networking, subject to inherited host restrictions.
    On,
    /// Outbound TCP connections to these destination ports only. No listening,
    /// UDP, raw sockets or local IPC. An empty set grants no network access.
    TcpPorts(BTreeSet<u16>),
}

impl ExecutionNetwork {
    fn permits(&self, requested: &Self) -> bool {
        match (self, requested) {
            (Self::On, _) | (_, Self::Off) => true,
            (Self::Off, Self::TcpPorts(ports)) => ports.is_empty(),
            (Self::TcpPorts(ceiling), Self::TcpPorts(ports)) => ports.is_subset(ceiling),
            _ => false,
        }
    }
}

/// Immutable execution authority. Construct only after Auth and approval have
/// decided the complete grant. The launch core consumes the same value for
/// shell commands, subprocesses and executable tool adapters.
#[derive(Clone, Debug)]
pub struct ExecutionGrant(Arc<Grant>);

#[derive(Debug)]
struct Grant {
    workspace: BoundPath,
    access: ExecutionAccess,
    read: Vec<BoundPath>,
    network: ExecutionNetwork,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct BoundPath {
    path: PathBuf,
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    identity: crate::FileIdentity,
}

impl BoundPath {
    fn new(path: &Path) -> io::Result<Self> {
        let path = canonical(path)?;
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        let identity = {
            let mut options = std::fs::OpenOptions::new();
            options.read(true);
            #[cfg(windows)]
            {
                use std::os::windows::fs::OpenOptionsExt;
                options.custom_flags(
                    windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS,
                );
            }
            crate::file_identity(&options.open(&path).map_err(failure)?).map_err(failure)?
        };
        Ok(Self {
            path,
            #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
            identity,
        })
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn verify(&self, file: &std::fs::File) -> io::Result<()> {
        if crate::file_identity(file).map_err(failure)? != self.identity {
            return Err(refused("execution grant root was replaced"));
        }
        Ok(())
    }
}

impl std::ops::Deref for BoundPath {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<Path> for BoundPath {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl ExecutionGrant {
    /// Binds existing, absolute workspace and read-only roots. Symlinks are
    /// resolved here; the adapter binds native objects again at active launch.
    /// No ambient home, temporary directory or filesystem root is added.
    pub fn new(
        workspace: &Path,
        access: ExecutionAccess,
        read: impl IntoIterator<Item = PathBuf>,
        network: ExecutionNetwork,
    ) -> io::Result<Self> {
        let workspace = BoundPath::new(workspace)?;
        if !workspace.is_dir() {
            return Err(refused("execution workspace is not a directory"));
        }
        let read = read
            .into_iter()
            .map(|path| BoundPath::new(&path))
            .collect::<io::Result<BTreeSet<_>>>()?
            .into_iter()
            .collect();
        if matches!(&network, ExecutionNetwork::TcpPorts(ports) if ports.contains(&0)) {
            return Err(refused("TCP port zero is not a destination grant"));
        }
        Ok(Self(Arc::new(Grant {
            workspace,
            access,
            read,
            network,
        })))
    }

    /// Checks authority attenuation before returning another immutable grant.
    /// Read-only roles cannot reacquire writes through a child grant; network
    /// and read roots can only narrow. No approvals occur in this method.
    pub fn attenuate(
        &self,
        workspace: &Path,
        access: ExecutionAccess,
        read: impl IntoIterator<Item = PathBuf>,
        network: ExecutionNetwork,
    ) -> io::Result<Self> {
        let requested = Self::new(workspace, access, read, network)?;
        if !requested.0.workspace.starts_with(&self.0.workspace)
            || (self.0.access == ExecutionAccess::ReadOnly
                && requested.0.access == ExecutionAccess::WorkspaceWrite)
            || !self.0.network.permits(&requested.0.network)
            || requested.0.read.iter().any(|path| {
                !path.starts_with(&self.0.workspace)
                    && !self.0.read.iter().any(|root| path.starts_with(root))
            })
        {
            return Err(refused("execution grant exceeds its authority ceiling"));
        }
        Ok(requested)
    }

    /// Workspace to which the policy bound this execution.
    #[must_use]
    pub fn workspace(&self) -> &Path {
        &self.0.workspace
    }

    /// Prepares OS objects without starting untrusted code. A failure is a
    /// refusal, never an instruction to retry without confinement.
    pub fn prepare(&self) -> io::Result<PreparedExecution> {
        #[cfg(target_os = "linux")]
        let platform = linux::Prepared::new(&self.0)?;
        #[cfg(target_os = "macos")]
        let platform = macos::Prepared::new(&self.0)?;
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        return Err(refused(
            "native execution boundary is unavailable on this platform",
        ));
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        Ok(PreparedExecution {
            platform,
            grant: self.clone(),
        })
    }
}

/// Active native enforcement objects, prepared before process creation and
/// retained through launch. This is not serializable or model supplied.
pub struct PreparedExecution {
    #[cfg(target_os = "linux")]
    platform: linux::Prepared,
    #[cfg(target_os = "macos")]
    platform: macos::Prepared,
    grant: ExecutionGrant,
}

impl PreparedExecution {
    /// Adds enforcement after the process owner's custody hook. The native
    /// launch core must consume the command exactly once, immediately after
    /// this call. On Unix the hook runs only in the final executing child.
    pub(crate) fn install(self, command: &mut std::process::Command) -> io::Result<()> {
        let cwd = command
            .get_current_dir()
            .ok_or_else(|| refused("confined execution requires an explicit working directory"))?;
        if !canonical(cwd)?.starts_with(self.grant.workspace()) {
            return Err(refused(
                "execution working directory escapes the granted workspace",
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        self.platform.install(command);
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        return Err(refused(
            "native execution boundary is unavailable on this platform",
        ));
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        Ok(())
    }
}

fn canonical(path: &Path) -> io::Result<PathBuf> {
    if !path.is_absolute() {
        return Err(refused("execution grant paths must be absolute"));
    }
    std::fs::canonicalize(path).map_err(failure)
}

pub(crate) fn failure(error: io::Error) -> io::Error {
    if error.to_string().starts_with("tool execution failed: ") {
        return error;
    }
    refused(error)
}

fn refused(reason: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        format!("tool execution failed: {reason}"),
    )
}

#[cfg(test)]
mod tests;
