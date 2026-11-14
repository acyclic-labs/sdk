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

mod defaults;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(windows)]
pub(crate) mod windows;

/// Filesystem authority of an execution, including all its descendants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionAccess {
    /// Read and execute granted paths, enumerate granted directories and query
    /// their ordinary inode metadata; no mutation. Linux extended attributes
    /// are unsupported because Landlock cannot confine their queries.
    ReadOnly,
    /// Read and execute granted paths; mutate only workspace and writable volume subtrees.
    WorkspaceWrite,
}

/// Network authority. An allowlist names TCP destination ports, on any address.
/// It is deliberately not represented as a hostname or address allowlist.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionNetwork {
    /// No network sockets can be created or connected. On Linux, anonymous
    /// Unix stream/seqpacket pairs remain available between confined processes.
    Off,
    /// No network traffic may cross the execution boundary. Communication
    /// between confined descendants may remain available. Unlike `Off`, this
    /// does not promise to prohibit socket creation inside that boundary.
    Isolated,
    /// Ordinary networking, subject to inherited host restrictions.
    On,
    /// Outbound TCP connections to these destination ports only. No listening,
    /// UDP, raw sockets or host IPC. On Linux, anonymous Unix stream/seqpacket
    /// pairs remain available. An empty set grants no network access.
    TcpPorts(BTreeSet<u16>),
}

impl ExecutionNetwork {
    fn permits(&self, requested: &Self) -> bool {
        match (self, requested) {
            (Self::On, _) | (_, Self::Off) | (Self::Isolated, Self::Isolated) => true,
            (Self::Isolated | Self::Off, Self::TcpPorts(ports)) => ports.is_empty(),
            (Self::TcpPorts(ceiling), Self::TcpPorts(ports)) => ports.is_subset(ceiling),
            _ => false,
        }
    }
}

/// Immutable execution authority. Construct only after Auth and approval have
/// decided the complete grant. The launch core consumes the same value for
/// shell commands, subprocesses and executable tool adapters.
#[derive(Clone, Debug)]
pub struct ExecutionGrant {
    policy: Arc<Grant>,
    access: ExecutionAccess,
    #[cfg(target_os = "linux")]
    compiled: Option<Arc<linux::Policy>>,
    #[cfg(windows)]
    compiled: Option<Arc<windows::Policy>>,
}

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

/// Root exit left unfinished or failed descendants in the command tree.
#[derive(Debug)]
pub struct ExecutionIncomplete;

impl std::fmt::Display for ExecutionIncomplete {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(
            "sandbox command left unfinished or failed descendants; command tree cleaned up",
        )
    }
}

impl std::error::Error for ExecutionIncomplete {}

#[derive(Debug)]
struct Grant {
    workspace: BoundPath,
    workspace_write: bool,
    write: acyclic_collections::CompiledPrefixIndex<BoundPath, ()>,
    read: acyclic_collections::CompiledPrefixIndex<BoundPath, ()>,
    #[cfg(target_os = "macos")]
    navigation: BTreeSet<PathBuf>,
    #[cfg(target_os = "macos")]
    lookup_metadata: BTreeSet<PathBuf>,
    network: ExecutionNetwork,
    #[cfg(target_os = "linux")]
    filter: &'static [linux::Instruction],
}

impl Grant {
    #[cfg(target_os = "macos")]
    fn permits_lookup_metadata(
        &self,
        path: &Path,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<bool> {
        if let Some(meter) = meter {
            meter.examine(1, 0)?;
        }
        if self.navigation.contains(path) || self.lookup_metadata.contains(path) {
            return Ok(true);
        }
        if path.starts_with(&self.workspace.path) {
            drop(self.workspace.bind_controlled(meter)?);
            return Ok(true);
        }
        for index in [&self.write, &self.read] {
            let roots = Self::roots(index, path, meter)?;
            if let Some(root) = roots.exact.or(roots.shortest)
                && (root.entry.is_dir || root.entry.path == path)
            {
                drop(root.entry.bind_controlled(meter)?);
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn contains(
        &self,
        requested: &BoundPath,
        writable: bool,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<bool> {
        let roots = [
            Some(Self::roots(&self.write, &requested.path, meter)?),
            if writable {
                None
            } else {
                Some(Self::roots(&self.read, &requested.path, meter)?)
            },
        ];
        for matches in roots.iter().flatten() {
            if let Some(root) = &matches.exact
                && root.entry.contains(requested, meter)?
            {
                return Ok(true);
            }
        }
        if (!writable || self.workspace_write) && self.workspace.contains(requested, meter)? {
            return Ok(true);
        }
        for matches in roots.iter().flatten() {
            if let Some(root) = &matches.shortest
                && root.entry.contains(requested, meter)?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
    fn cwd_root(&self, cwd: &Path) -> io::Result<&BoundPath> {
        if cwd.starts_with(&self.workspace.path) {
            return Ok(&self.workspace);
        }
        for index in [&self.write, &self.read] {
            let roots = Self::roots(index, cwd, None)?;
            if let Some(root) = roots.exact.or(roots.shortest) {
                return Ok(root.entry);
            }
        }
        Err(refused(
            "execution working directory escapes the granted workspace",
        ))
    }

    fn roots<'a>(
        roots: &'a acyclic_collections::CompiledPrefixIndex<BoundPath, ()>,
        path: &Path,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<acyclic_collections::PrefixMatches<'a, BoundPath>> {
        let query = path.as_os_str().as_encoded_bytes();
        roots
            .matches(
                query,
                |root| {
                    let key: &[u8] = root.as_ref();
                    // Canonical paths have normalized separators. Test the next
                    // byte, not all prior components, at each radix terminal.
                    key.last().is_some_and(|byte| separator(*byte))
                        || query.get(key.len()).is_some_and(|byte| separator(*byte))
                },
                &mut RootIndexRead(meter),
            )
            .map_err(|error| match error {
                acyclic_collections::CompiledPrefixError::Work(error) => error,
                other => io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("execution root index: {other:?}"),
                ),
            })
    }
}

fn separator(byte: u8) -> bool {
    byte == b'/' || cfg!(windows) && byte == b'\\'
}

struct RootIndexRead<'a>(Option<&'a dyn crate::NativeReadMeter>);

impl acyclic_collections::IndexReadWork for RootIndexRead<'_> {
    type Error = io::Error;

    fn validate(&mut self) -> io::Result<()> {
        self.0.map_or(Ok(()), crate::NativeReadMeter::check)
    }

    fn examine(&mut self, items: u64) -> io::Result<()> {
        self.0.map_or(Ok(()), |meter| meter.examine(items, 0))
    }
}

// Explicit host grant construction is unmetered, including its original path
// vector and pinned descriptors. Admission reads use their original ledger.
struct RootIndexBuild;

impl acyclic_collections::IndexWork for RootIndexBuild {
    type Error = std::convert::Infallible;
    type Allocation = ();

    fn check(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn charge_items(&mut self, _: u64) -> Result<(), Self::Error> {
        Ok(())
    }

    fn claim(&mut self, _: u64) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Debug)]
struct BoundPath {
    path: PathBuf,
    // Lookup aliases are metadata authority, not additional content roots.
    #[cfg(target_os = "macos")]
    aliases: Vec<PathBuf>,
    is_dir: bool,
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    identity: crate::FileIdentity,
    // A numeric identity alone can be reused after unlink. This shared handle
    // pins the original object, preventing an ABA replacement while idle.
    _object: std::fs::File,
}

impl BoundPath {
    fn new(path: &Path) -> io::Result<Self> {
        if !path.is_absolute() {
            return Err(refused("execution grant paths must be absolute"));
        }
        let canonical = std::fs::canonicalize(path).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("cannot bind execution root {}: {error}", path.display()),
            )
        })?;
        #[cfg(target_os = "macos")]
        if path
            .components()
            .any(|part| part == std::path::Component::ParentDir)
        {
            return Err(refused(
                "execution lookup paths must not contain parent components",
            ));
        }
        #[cfg(target_os = "macos")]
        let aliases = (path != canonical.as_path())
            .then(|| path.to_path_buf())
            .into_iter()
            .collect();
        let path = canonical;
        let object = Self::open_path(&path)?;
        let metadata = object.metadata()?;
        #[cfg(any(target_os = "linux", target_vendor = "apple"))]
        let identity = crate::file::file_identity_from_metadata(&metadata);
        #[cfg(windows)]
        let identity = crate::file_identity(&object)?;
        Ok(Self {
            path,
            #[cfg(target_os = "macos")]
            aliases,
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
        #[cfg(target_os = "macos")]
        {
            use std::os::unix::fs::OpenOptionsExt;
            // Binding or verifying a special-file root must never wait for a
            // peer. Identity admission is independent of later guest I/O.
            options.custom_flags(libc::O_NONBLOCK);
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options
                .access_mode(windows_sys::Win32::Storage::FileSystem::FILE_READ_ATTRIBUTES)
                .custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS)
                // A bound Windows root cannot be renamed while the grant is live.
                .share_mode(
                    windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ
                        | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_WRITE,
                );
        }
        options.open(path)
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
        if crate::file_identity(file)? != self.identity {
            return Err(refused("execution grant root was replaced"));
        }
        Ok(())
    }

    fn contains(
        &self,
        requested: &Self,
        _meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<bool> {
        let Ok(relative) = requested.path.strip_prefix(&self.path) else {
            return Ok(false);
        };
        #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
        {
            let exact = relative.as_os_str().is_empty();
            // Both grants pin their original objects. Compare those identities
            // once, then bind the ceiling to detect replacement at its path.
            if exact && self.identity != requested.identity {
                return Err(refused("execution grant root was replaced"));
            }
            let root = self.bind_controlled(_meter)?;
            if exact {
                drop(root);
                return Ok(true);
            }
            #[cfg(target_os = "linux")]
            let child = linux::open_beneath(&root, relative, _meter)?;
            #[cfg(target_os = "macos")]
            let child = macos::open_beneath(root, relative, false, _meter)?;
            #[cfg(windows)]
            let child = windows::open_beneath(&root, relative, false, _meter)?;
            #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
            {
                crate::meter::native_control_attempt(
                    _meter,
                    crate::NativeOperationKind::Read,
                    || requested.verify(&child),
                )?;
                Ok(true)
            }
        }
        #[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
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

impl AsRef<[u8]> for BoundPath {
    fn as_ref(&self) -> &[u8] {
        self.path.as_os_str().as_encoded_bytes()
    }
}

fn bind_roots(
    paths: impl IntoIterator<Item = PathBuf>,
) -> io::Result<acyclic_collections::CompiledPrefixIndex<BoundPath, ()>> {
    let roots = paths
        .into_iter()
        .map(|path| BoundPath::new(&path))
        .collect::<io::Result<Vec<_>>>()?;
    index_roots(roots)
}

fn index_roots(
    mut roots: Vec<BoundPath>,
) -> io::Result<acyclic_collections::CompiledPrefixIndex<BoundPath, ()>> {
    roots.sort_unstable_by(|left, right| {
        AsRef::<[u8]>::as_ref(left).cmp(AsRef::<[u8]>::as_ref(right))
    });
    roots.dedup_by(|left, right| {
        if left.path != right.path {
            return false;
        }
        #[cfg(target_os = "macos")]
        right.aliases.append(&mut left.aliases);
        true
    });
    acyclic_collections::CompiledPrefixIndex::from_sorted(roots, &mut RootIndexBuild).map_err(
        |error| {
            use acyclic_collections::CompiledPrefixError;
            match error {
                CompiledPrefixError::Work(impossible) => match impossible {},
                CompiledPrefixError::Order(_) => refused("execution root keys are not ordered"),
                CompiledPrefixError::Capacity => {
                    refused("execution root index exceeds address space")
                }
                CompiledPrefixError::Allocation(error) => io::Error::other(error),
                CompiledPrefixError::Invariant => refused("execution root index invariant changed"),
            }
        },
    )
}

impl ExecutionGrant {
    /// The default workspace boundary: only the composed paths and native
    /// runtime dependencies are readable, and network traffic cannot cross
    /// the execution boundary. Explicit authenticated grants can select
    /// additional filesystem or network authority through `composed`.
    /// Workspace reads include directory enumeration and read-only inode
    /// metadata queries. Native path traversal permits the ancestor metadata
    /// needed to locate those granted objects without granting outside writes.
    pub fn workspace_default(
        workspace: &Path,
        access: ExecutionAccess,
        write: impl IntoIterator<Item = PathBuf>,
        read: impl IntoIterator<Item = PathBuf>,
    ) -> io::Result<Self> {
        Self::composed(
            workspace,
            access,
            write,
            defaults::runtime_read_roots()?.into_iter().chain(read),
            ExecutionNetwork::Isolated,
        )
    }

    /// Composes native-only read bindings under their retained host ceilings.
    /// Missing or unreadable optional roots add no authority. Each available
    /// root is bound once and checked against the ceiling's original
    /// filesystem objects before entering the immutable launch policy.
    pub fn workspace_default_with_optional_read_scopes(
        workspace: &Path,
        access: ExecutionAccess,
        write: impl IntoIterator<Item = PathBuf>,
        read: impl IntoIterator<Item = PathBuf>,
        scoped_read: impl IntoIterator<Item = (PathBuf, Self)>,
    ) -> io::Result<Self> {
        Self::composed_with_read_scopes(
            workspace,
            access,
            write,
            defaults::runtime_read_roots()?.into_iter().chain(read),
            ExecutionNetwork::Isolated,
            scoped_read,
        )
    }

    /// Binds existing, absolute workspace and read-only roots. Symlinks are
    /// resolved here; the adapter binds native objects again at active launch.
    /// No ambient home, temporary directory or filesystem root is added.
    /// Invalid network authority refuses before binding paths or consuming
    /// the read-root iterator.
    /// On Linux, construction also makes the trusted host process non-dumpable
    /// to protect its environment and memory from read-capable guests.
    pub fn new(
        workspace: &Path,
        access: ExecutionAccess,
        read: impl IntoIterator<Item = PathBuf>,
        network: ExecutionNetwork,
    ) -> io::Result<Self> {
        Self::composed(workspace, access, [], read, network)
    }

    /// Binds one composed workspace, including independently writable volumes.
    /// Additional roots are explicit host authority, never command arguments.
    /// `access` controls the primary root; additional write roots remain
    /// writable. `into_read_only` removes mutation from every root together. Roots are
    /// pinned and indexed once; no process, mount, thread or watcher is created.
    /// Linux construction makes the host non-dumpable for its remaining
    /// lifetime. Embedders must not re-enable dumpability while guests exist.
    pub fn composed(
        workspace: &Path,
        access: ExecutionAccess,
        write: impl IntoIterator<Item = PathBuf>,
        read: impl IntoIterator<Item = PathBuf>,
        network: ExecutionNetwork,
    ) -> io::Result<Self> {
        Self::composed_with_read_scopes(workspace, access, write, read, network, [])
    }

    fn composed_with_read_scopes(
        workspace: &Path,
        access: ExecutionAccess,
        write: impl IntoIterator<Item = PathBuf>,
        read: impl IntoIterator<Item = PathBuf>,
        network: ExecutionNetwork,
        scoped_read: impl IntoIterator<Item = (PathBuf, Self)>,
    ) -> io::Result<Self> {
        if matches!(&network, ExecutionNetwork::TcpPorts(ports) if ports.contains(&0)) {
            return Err(refused("TCP port zero is not a destination grant"));
        }
        #[cfg(target_os = "linux")]
        linux::protect_owner()?;
        let workspace = BoundPath::new(workspace)?;
        if !workspace.is_dir {
            return Err(refused("execution workspace is not a directory"));
        }
        let write = bind_roots(write)?;
        let mut read = read
            .into_iter()
            .map(|path| BoundPath::new(&path))
            .collect::<io::Result<Vec<_>>>()?;
        for (path, ceiling) in scoped_read {
            let root = match BoundPath::new(&path) {
                Ok(root) => root,
                Err(error)
                    if !error
                        .get_ref()
                        .is_some_and(|cause| cause.is::<ExecutionRefusal>())
                        && matches!(
                            error.kind(),
                            io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
                        ) =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            };
            if !ceiling.policy.contains(&root, false, None)? {
                return Err(refused("native read root exceeds its authority ceiling"));
            }
            read.push(root);
        }
        let read = index_roots(read)?;
        #[cfg(target_os = "macos")]
        let roots = || {
            std::iter::once(&workspace)
                .chain(write.entries())
                .chain(read.entries())
        };
        // Exact directory objects support dyld's openat bootstrap. Alias
        // metadata supports lookup only; neither set grants sibling contents.
        // Derive both here so native lowering and attenuation share one policy.
        #[cfg(target_os = "macos")]
        let navigation = roots()
            .flat_map(|root| root.path.ancestors().skip(1))
            .map(Path::to_path_buf)
            .collect();
        #[cfg(target_os = "macos")]
        let lookup_metadata = roots()
            .flat_map(|root| &root.aliases)
            .flat_map(|alias| alias.ancestors())
            .map(Path::to_path_buf)
            .collect();
        let workspace_write = access == ExecutionAccess::WorkspaceWrite;
        let access = if workspace_write || !write.entries().is_empty() {
            ExecutionAccess::WorkspaceWrite
        } else {
            ExecutionAccess::ReadOnly
        };
        #[cfg(target_os = "linux")]
        let filter = linux::filter(&network)?;
        Ok(Self {
            policy: Arc::new(Grant {
                workspace,
                workspace_write,
                write,
                read,
                #[cfg(target_os = "macos")]
                navigation,
                #[cfg(target_os = "macos")]
                lookup_metadata,
                network,
                #[cfg(target_os = "linux")]
                filter,
            }),
            access,
            #[cfg(any(windows, target_os = "linux"))]
            compiled: None,
        })
    }

    /// Retains native policy for repeated launches of this authorized grant.
    /// Compilation binds the original roots once; each launch still verifies
    /// their paths and resolves its working directory. No authority is issued.
    /// Clones share the immutable policy without locks or a lazy initializer.
    /// A later read-only view never uses a compiled writable policy: compile
    /// that view separately to retain its narrower native policy.
    /// Like grant construction, this explicit host operation is unmetered.
    /// Per-launch root and cwd work keeps its original invocation meter.
    pub fn compile_native(self) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            if self
                .compiled
                .as_ref()
                .is_some_and(|policy| policy.access() == self.access)
            {
                return Ok(self);
            }
            let workspace = self.policy.workspace.bind_controlled(None)?;
            let policy = linux::Policy::new(&self.policy, self.access, &workspace, None)?;
            Ok(Self {
                compiled: Some(Arc::new(policy)),
                ..self
            })
        }
        #[cfg(target_os = "macos")]
        {
            macos::Prepared::new(&self.policy, self.access, None, None)?;
            Ok(self)
        }
        #[cfg(windows)]
        {
            if self
                .compiled
                .as_ref()
                .is_some_and(|policy| policy.access == self.access)
            {
                return Ok(self);
            }
            let compiled = Arc::new(windows::Policy::new(&self.policy, self.access)?);
            Ok(Self {
                compiled: Some(compiled),
                ..self
            })
        }
        #[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
        Err(unavailable(
            "native execution boundary is unavailable on this platform",
        ))
    }

    /// Removes workspace mutation authority while retaining the original
    /// pinned roots and network policy. This consumes only the grant value:
    /// no allocation, path resolution, descriptor duplication or syscall.
    /// Clone first to retain the original grant. Launch still verifies every
    /// original root; this does not refresh or authenticate stale authority.
    #[must_use]
    pub fn into_read_only(mut self) -> Self {
        self.access = ExecutionAccess::ReadOnly;
        self
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
        self.check_roots(&requested, None)?;
        Ok(requested)
    }

    /// Verifies an already bound grant against this host ceiling, without
    /// rebuilding either grant or weakening root object identity checks.
    pub fn check_attenuation(&self, requested: &Self) -> io::Result<()> {
        self.check_attenuation_controlled(requested, None)
    }

    /// Checks the same bound objects on the caller's original native ledger.
    /// Each open and identity query reserves its operation before the syscall;
    /// rooted path storage borrows this meter until physical destruction.
    /// This does not charge grant construction or implicit descriptor closes.
    pub fn check_attenuation_controlled(
        &self,
        requested: &Self,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<()> {
        self.check_authority(requested.access, &requested.policy.network)?;
        self.check_roots(requested, meter)
    }

    /// Verifies the same pinned roots on the shared native workers, without
    /// blocking the caller's executor. The task owns both grants and the
    /// original meter until its terminal producer retires the proof. Dropping
    /// the wait never attaches a grant or starts a guest.
    /// Native failures, including task admission and cancellation, retain
    /// their original error kind.
    pub async fn check_attenuation_async(
        &self,
        requested: &Self,
        meter: Option<Arc<dyn crate::NativeReadMeter>>,
    ) -> io::Result<()> {
        self.check_authority(requested.access, &requested.policy.network)?;
        let ceiling = self.clone();
        let requested = requested.clone();
        let proof_meter = meter.clone();
        crate::run_blocking_io(
            move || ceiling.check_roots(&requested, proof_meter.as_deref()),
            meter.map(|meter| (meter, false)),
        )?
        .offloaded()
        .await?
    }

    fn check_authority(
        &self,
        access: ExecutionAccess,
        network: &ExecutionNetwork,
    ) -> io::Result<()> {
        if (self.access == ExecutionAccess::ReadOnly && access == ExecutionAccess::WorkspaceWrite)
            || !self.policy.network.permits(network)
        {
            return Err(refused("execution grant exceeds its authority ceiling"));
        }
        Ok(())
    }

    fn check_roots(
        &self,
        requested: &Self,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<()> {
        // Preserve the primary workspace proof before any index work. Apart
        // from avoiding a lookup for the common case, this keeps admission's
        // original operation and examination ordering on its funding ledger.
        let writable =
            requested.access == ExecutionAccess::WorkspaceWrite && requested.policy.workspace_write;
        if !((!writable || self.policy.workspace_write)
            && self
                .policy
                .workspace
                .contains(&requested.policy.workspace, meter)?)
            && !self
                .policy
                .contains(&requested.policy.workspace, writable, meter)?
        {
            return Err(refused("execution grant exceeds its authority ceiling"));
        }
        for (index, writable) in [
            (
                &requested.policy.write,
                requested.access == ExecutionAccess::WorkspaceWrite,
            ),
            (&requested.policy.read, false),
        ] {
            for path in index.entries() {
                if !self.policy.contains(path, writable, meter)? {
                    return Err(refused("execution grant exceeds its authority ceiling"));
                }
            }
        }
        #[cfg(target_os = "macos")]
        for path in &requested.policy.lookup_metadata {
            if !self.policy.permits_lookup_metadata(path, meter)? {
                return Err(refused(
                    "execution grant exceeds its lookup metadata ceiling",
                ));
            }
        }
        Ok(())
    }

    /// Workspace to which the policy bound this execution.
    #[must_use]
    pub fn workspace(&self) -> &Path {
        &self.policy.workspace
    }

    /// The pinned workspace object, for another primitive to compare its
    /// original root witness without reopening a mutable path.
    #[must_use]
    pub fn workspace_file(&self) -> &std::fs::File {
        &self.policy.workspace._object
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
            || !(cwd.starts_with(self.workspace())
                || [&self.policy.write, &self.policy.read]
                    .into_iter()
                    .any(|index| {
                        Grant::roots(index, cwd, None)
                            .is_ok_and(|roots| roots.exact.or(roots.shortest).is_some())
                    }))
        {
            return Err(refused(
                "execution working directory escapes the granted workspace",
            ));
        }
        Ok(cwd)
    }

    /// Prepares OS objects without starting untrusted code. An error prevents
    /// launch; it never instructs a retry without confinement.
    pub fn prepare(&self) -> io::Result<PreparedExecution> {
        self.prepare_launch(None, None)
    }

    pub(crate) fn prepare_launch(
        &self,
        _cwd: Option<&Path>,
        _meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<PreparedExecution> {
        #[cfg(target_os = "linux")]
        let workspace = self.policy.workspace.bind_controlled(_meter)?;
        #[cfg(target_os = "linux")]
        let platform = linux::Prepared::new(
            &self.policy,
            self.access,
            self.compiled.as_ref(),
            workspace,
            _cwd,
            _meter,
        )?;
        #[cfg(target_os = "macos")]
        let platform = macos::Prepared::new(&self.policy, self.access, _cwd, _meter)?;
        #[cfg(windows)]
        let platform = windows::Prepared::new(
            &self.policy,
            self.access,
            self.compiled.as_ref(),
            _cwd,
            _meter,
        )?;
        #[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
        return Err(unavailable(
            "native execution boundary is unavailable on this platform",
        ));
        #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
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
    #[cfg(windows)]
    platform: windows::Prepared,
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
    #[cfg(windows)]
    pub(crate) fn windows_policy(&self) -> Arc<windows::Policy> {
        Arc::clone(&self.platform.policy)
    }

    #[cfg(windows)]
    pub(crate) fn validate_windows_stdio(
        &self,
        handles: &[windows_sys::Win32::Foundation::HANDLE],
    ) -> io::Result<()> {
        windows::validate_stdio(handles)
    }
    #[cfg(target_os = "macos")]
    pub(crate) fn validate_command(&self, command: &crate::ProcessCommand) -> io::Result<()> {
        macos::validate_stdio(command.sandbox_stdio())
    }
    #[cfg(target_os = "linux")]
    pub(crate) fn working_directory(&self) -> &std::fs::File {
        self.platform.working_directory()
    }

    /// Adds enforcement after the process owner's custody hook. The command
    /// has already been admitted once and is consumed by the launch owner.
    #[cfg(unix)]
    pub(crate) fn install(
        self,
        _command: &mut std::process::Command,
        _arg0: Option<&std::ffi::OsStr>,
    ) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        return Err(unavailable(
            "granted Linux launches require the native runner",
        ));
        #[cfg(target_os = "macos")]
        self.platform.install(_command, _arg0)?;
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        return Err(unavailable(
            "native execution boundary is unavailable on this platform",
        ));
        #[cfg(target_os = "macos")]
        Ok(())
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn encode_linux(&self, body: &mut Vec<u8>) -> io::Result<std::os::fd::RawFd> {
        self.platform.encode(self.grant.policy.filter, body)
    }
}

pub(crate) fn refused(reason: impl std::fmt::Display) -> io::Error {
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
