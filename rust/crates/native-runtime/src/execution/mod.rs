//! Capabilities supplied by an authenticated host policy at native launch.
//!
//! This module does not decide approvals. A model's command line, environment,
//! current directory or tool arguments cannot grant authority. Preparation is
//! per active execution; sharing a grant holds no process, thread or watcher.
//! Native root handles pin object identity for the grant's lifetime.

use std::{
    collections::BTreeSet,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

#[cfg(target_os = "linux")]
mod linux;

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

/// A definitive refusal before guest execution. The ordinary tool result
/// renderer can consume the reason without repeating the failure prefix.
#[derive(Debug)]
pub struct ExecutionRefusal(String);

impl ExecutionRefusal {
    /// Native enforcement or grant validation reason, without presentation.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ExecutionRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "tool execution failed: {}", self.0)
    }
}

impl std::error::Error for ExecutionRefusal {}

#[derive(Debug)]
struct Grant {
    workspace: BoundPath,
    access: ExecutionAccess,
    read: Vec<BoundPath>,
    network: ExecutionNetwork,
}

#[derive(Debug)]
struct BoundPath {
    path: PathBuf,
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    identity: crate::FileIdentity,
    // A numeric identity alone can be reused after unlink. This shared handle
    // pins the original object, preventing an ABA replacement while idle.
    _object: std::fs::File,
}

impl BoundPath {
    fn new(path: &Path) -> io::Result<Self> {
        let path = canonical(path)?;
        let object = Self::open_path(&path)?;
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        let identity = crate::file_identity(&object).map_err(failure)?;
        Ok(Self {
            path,
            #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
            identity,
            _object: object,
        })
    }

    fn open_path(path: &Path) -> io::Result<std::fs::File> {
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_PATH | libc::O_CLOEXEC);
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options
                .custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS);
        }
        options.open(path).map_err(failure)
    }

    #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
    fn bind(&self) -> io::Result<std::fs::File> {
        let file = Self::open_path(self)?;
        self.verify(&file)?;
        Ok(file)
    }

    #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
    fn verify(&self, file: &std::fs::File) -> io::Result<()> {
        if crate::file_identity(file).map_err(failure)? != self.identity {
            return Err(refused("execution grant root was replaced"));
        }
        Ok(())
    }

    fn contains(&self, requested: &Self) -> io::Result<bool> {
        let Ok(relative) = requested.path.strip_prefix(&self.path) else {
            return Ok(false);
        };
        #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
        {
            let root = self.bind()?;
            if relative.as_os_str().is_empty() {
                requested.verify(&root)?;
                return Ok(true);
            }
            #[cfg(target_os = "linux")]
            let child = linux::open_beneath(&root, relative)?;
            #[cfg(target_os = "linux")]
            {
                requested.verify(&child)?;
                return Ok(true);
            }
        }
        #[cfg(not(target_os = "linux"))]
        Err(unavailable(
            "native grant attenuation is unavailable on this platform",
        ))
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
        let mut read = read
            .into_iter()
            .map(|path| BoundPath::new(&path))
            .collect::<io::Result<Vec<_>>>()?;
        read.sort_unstable_by(|left, right| left.path.cmp(&right.path));
        read.dedup_by(|left, right| left.path == right.path);
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
        if (self.0.access == ExecutionAccess::ReadOnly
            && requested.0.access == ExecutionAccess::WorkspaceWrite)
            || !self.0.network.permits(&requested.0.network)
            || !self.0.workspace.contains(&requested.0.workspace)?
        {
            return Err(refused("execution grant exceeds its authority ceiling"));
        }
        for path in &requested.0.read {
            if self.0.workspace.contains(path)? {
                continue;
            }
            let mut permitted = false;
            for root in &self.0.read {
                if root.contains(path)? {
                    permitted = true;
                    break;
                }
            }
            if !permitted {
                return Err(refused("execution grant exceeds its authority ceiling"));
            }
        }
        Ok(requested)
    }

    /// Workspace to which the policy bound this execution.
    #[must_use]
    pub fn workspace(&self) -> &Path {
        &self.0.workspace
    }

    pub(crate) fn validate_launch(
        &self,
        cwd: Option<&Path>,
        explicit_environment: bool,
    ) -> io::Result<()> {
        if !explicit_environment {
            return Err(refused(
                "confined execution requires an explicit environment",
            ));
        }
        let cwd = cwd
            .ok_or_else(|| refused("confined execution requires an explicit working directory"))?;
        if !canonical(cwd)?.starts_with(self.workspace()) {
            return Err(refused(
                "execution working directory escapes the granted workspace",
            ));
        }
        Ok(())
    }

    /// Prepares OS objects without starting untrusted code. A failure is a
    /// refusal, never an instruction to retry without confinement.
    pub fn prepare(&self) -> io::Result<PreparedExecution> {
        #[cfg(target_os = "linux")]
        let roots = std::iter::once(&self.0.workspace)
            .chain(&self.0.read)
            .map(BoundPath::bind)
            .collect::<io::Result<Vec<_>>>()?;
        #[cfg(target_os = "linux")]
        let platform = linux::Prepared::new(&self.0, roots)?;
        #[cfg(not(target_os = "linux"))]
        return Err(unavailable(
            "native execution boundary is unavailable on this platform",
        ));
        #[cfg(target_os = "linux")]
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
    grant: ExecutionGrant,
}

impl std::fmt::Debug for PreparedExecution {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedExecution")
            .field("grant", &self.grant)
            .finish_non_exhaustive()
    }
}

impl PreparedExecution {
    /// Adds enforcement after the process owner's custody hook. The command
    /// has already been admitted once and is consumed by the launch owner.
    pub(crate) fn install(self, _command: &mut std::process::Command) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        self.platform.install(_command);
        #[cfg(not(target_os = "linux"))]
        return Err(unavailable(
            "native execution boundary is unavailable on this platform",
        ));
        #[cfg(target_os = "linux")]
        Ok(())
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn encode_linux(
        &self,
        descriptors: &mut Vec<std::os::fd::RawFd>,
        body: &mut Vec<u8>,
    ) -> io::Result<()> {
        self.platform.encode(descriptors, body)
    }
}

fn canonical(path: &Path) -> io::Result<PathBuf> {
    if !path.is_absolute() {
        return Err(refused("execution grant paths must be absolute"));
    }
    std::fs::canonicalize(path).map_err(failure)
}

pub(crate) fn failure(error: io::Error) -> io::Error {
    if error
        .get_ref()
        .is_some_and(|cause| cause.is::<ExecutionRefusal>())
    {
        return error;
    }
    refused(error)
}

fn refused(reason: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        ExecutionRefusal(reason.to_string()),
    )
}

fn unavailable(reason: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        ExecutionRefusal(reason.to_string()),
    )
}

#[cfg(test)]
mod tests;
