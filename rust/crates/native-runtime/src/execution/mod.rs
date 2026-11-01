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
    #[cfg(target_os = "linux")]
    filter: Vec<linux::Instruction>,
}

#[derive(Debug)]
struct BoundPath {
    path: PathBuf,
    is_dir: bool,
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
        let metadata = object.metadata().map_err(failure)?;
        #[cfg(any(target_os = "linux", target_vendor = "apple"))]
        let identity = crate::file::file_identity_from_metadata(&metadata);
        #[cfg(windows)]
        let identity = crate::file_identity(&object).map_err(failure)?;
        Ok(Self {
            path,
            is_dir: metadata.is_dir(),
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
        self.bind_controlled(None)
    }

    #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
    fn bind_controlled(
        &self,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<std::fs::File> {
        use crate::{NativeOperationKind::Read, meter::native_control_attempt};
        let file = native_control_attempt(meter, Read, || Self::open_path(self))?;
        native_control_attempt(meter, Read, || self.verify(&file))?;
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
        if !workspace.is_dir {
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
        #[cfg(target_os = "linux")]
        let filter = linux::filter(&network)?;
        Ok(Self(Arc::new(Grant {
            workspace,
            access,
            read,
            network,
            #[cfg(target_os = "linux")]
            filter,
        })))
    }

    /// Checks authority attenuation before returning another immutable grant.
    /// Read-only roles cannot reacquire writes through a child grant; network
    /// and read roots can only narrow. Write and network ceilings are checked
    /// before resolving paths or consuming read roots. No approvals occur here.
    pub fn attenuate(
        &self,
        workspace: &Path,
        access: ExecutionAccess,
        read: impl IntoIterator<Item = PathBuf>,
        network: ExecutionNetwork,
    ) -> io::Result<Self> {
        self.check_authority(access, &network)?;
        let requested = Self::new(workspace, access, read, network)?;
        self.check_roots(&requested)?;
        Ok(requested)
    }

    /// Verifies an already bound grant against this host ceiling, without
    /// rebuilding either grant or weakening root object identity checks.
    pub fn check_attenuation(&self, requested: &Self) -> io::Result<()> {
        self.check_authority(requested.0.access, &requested.0.network)?;
        self.check_roots(requested)
    }

    fn check_authority(
        &self,
        access: ExecutionAccess,
        network: &ExecutionNetwork,
    ) -> io::Result<()> {
        if (self.0.access == ExecutionAccess::ReadOnly && access == ExecutionAccess::WorkspaceWrite)
            || !self.0.network.permits(network)
        {
            return Err(refused("execution grant exceeds its authority ceiling"));
        }
        Ok(())
    }

    fn check_roots(&self, requested: &Self) -> io::Result<()> {
        if !self.0.workspace.contains(&requested.0.workspace)? {
            return Err(refused("execution grant exceeds its authority ceiling"));
        }
        for path in &requested.0.read {
            // An already granted root needs only its original object proof.
            // An overlapping ancestor may require an unavailable subtree proof.
            if let Ok(index) = self
                .0
                .read
                .binary_search_by(|root| root.path.cmp(&path.path))
            {
                if self.0.read[index].contains(path)? {
                    continue;
                }
            }
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
        Ok(())
    }

    /// Workspace to which the policy bound this execution.
    #[must_use]
    pub fn workspace(&self) -> &Path {
        &self.0.workspace
    }

    pub(crate) fn validate_launch<'a>(
        &self,
        cwd: Option<&'a Path>,
        explicit_environment: bool,
    ) -> io::Result<&'a Path> {
        if !explicit_environment {
            return Err(refused(
                "confined execution requires an explicit environment",
            ));
        }
        let cwd = cwd
            .ok_or_else(|| refused("confined execution requires an explicit working directory"))?;
        // This is admission, not a second path resolution. The native adapter
        // resolves the relative directory once beneath the verified workspace.
        if !cwd.is_absolute()
            || cwd
                .components()
                .any(|component| component == std::path::Component::ParentDir)
            || !cwd.starts_with(self.workspace())
        {
            return Err(refused(
                "execution working directory escapes the granted workspace",
            ));
        }
        Ok(cwd)
    }

    /// Prepares OS objects without starting untrusted code. A failure is a
    /// refusal, never an instruction to retry without confinement.
    pub fn prepare(&self) -> io::Result<PreparedExecution> {
        self.prepare_launch(None, None).map_err(failure)
    }

    pub(crate) fn prepare_launch(
        &self,
        _cwd: Option<&Path>,
        _meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<PreparedExecution> {
        #[cfg(target_os = "linux")]
        let workspace = self.0.workspace.bind_controlled(_meter)?;
        #[cfg(target_os = "linux")]
        let platform = linux::Prepared::new(&self.0, workspace, _cwd, _meter)?;
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
    #[cfg(target_os = "linux")]
    pub(crate) fn working_directory(&self) -> &std::fs::File {
        self.platform.working_directory()
    }

    /// Adds enforcement after the process owner's custody hook. The command
    /// has already been admitted once and is consumed by the launch owner.
    pub(crate) fn install(self, _command: &mut std::process::Command) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        self.platform.install(_command, self.grant);
        #[cfg(not(target_os = "linux"))]
        return Err(unavailable(
            "native execution boundary is unavailable on this platform",
        ));
        #[cfg(target_os = "linux")]
        Ok(())
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn encode_linux(&self, body: &mut Vec<u8>) -> io::Result<std::os::fd::RawFd> {
        self.platform.encode(&self.grant.0.filter, body)
    }
}

fn canonical(path: &Path) -> io::Result<PathBuf> {
    if !path.is_absolute() {
        return Err(refused("execution grant paths must be absolute"));
    }
    std::fs::canonicalize(path).map_err(failure)
}

pub(crate) fn failure(error: io::Error) -> io::Error {
    if error.kind() == io::ErrorKind::Interrupted
        || error
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
pub(crate) mod tests;
