use super::*;

/// Probes the target's one canonical native namespace mechanism.
#[must_use]
pub fn probe_native_mount() -> NativeMountCapabilities {
    #[cfg(target_os = "linux")]
    let (kind, available, reason) = (
        Some(NativeMountKind::LinuxFuse),
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/fuse")
            .is_ok(),
        "/dev/fuse is not usable",
    );
    #[cfg(target_os = "macos")]
    let (kind, available, reason) = (
        Some(NativeMountKind::MacOsNfs),
        Path::new("/sbin/mount_nfs").is_file(),
        "the built-in macOS NFS mount utility is unavailable",
    );
    #[cfg(windows)]
    let (kind, available, reason) = (
        Some(NativeMountKind::WindowsProjFs),
        Path::new(&std::env::var_os("SystemRoot").unwrap_or_else(|| "C:\\Windows".into()))
            .join("System32")
            .join("ProjectedFSLib.dll")
            .is_file(),
        "ProjectedFSLib.dll is unavailable",
    );
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    let (kind, available, reason) = (None, false, "unsupported target");
    NativeMountCapabilities {
        kind,
        unavailable_reason: (!available).then_some(reason),
    }
}

/// Starts a real native projection over the supplied canonical checkout.
pub fn mount_native(
    request: NativeMountRequest,
    source: Arc<dyn MountFilesystem>,
) -> Result<NativeMountSession, NativeMountError> {
    mount_native_ordinary(request, source, false)
}

pub(super) fn mount_native_ordinary(
    request: NativeMountRequest,
    source: Arc<dyn MountFilesystem>,
    allow_nonempty: bool,
) -> Result<NativeMountSession, NativeMountError> {
    let cancellation = crate::CancellationToken::new();
    let work =
        match crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation) {
            Ok(work) => work,
            Err((error, _work)) => {
                return Err(NativeMountError::StartupAdmissionFailed {
                    cause: error.into(),
                    source,
                });
            }
        };
    // Ordinary bootstrap is explicit, but the supplied source still needs
    // genuine failure custody if that bootstrap cannot complete.
    let storage = match PreparedNativeMountStart::new(&work) {
        Ok(storage) => storage,
        Err(cause) => return Err(NativeMountError::StartupAdmissionFailed { cause, source }),
    };
    let initialized = initialize_native_mount_services()
        .and_then(|()| acyclic_native_runtime::initialize_native_io_services().map_err(Into::into));
    if let Err(error) = initialized {
        let owner = NativeMountSession::new(request, Some(source), None);
        return Err(NativeMountError::StartupFailed(
            storage.failed(error, owner),
        ));
    }
    start_native_mount_prepared_with_meter(request, source, allow_nonempty, &work, storage, None)
}

pub(super) fn start_native_mount_prepared_with_meter(
    request: NativeMountRequest,
    source: Arc<dyn MountFilesystem>,
    allow_nonempty: bool,
    work: &NativeMaterializationWork,
    storage: PreparedNativeMountStart,
    destination_custody: Option<crate::NativeAllocation>,
) -> Result<NativeMountSession, NativeMountError> {
    // The existing driver loop accepts its lifetime before this stack factory
    // acquires a physical fence. The factory never escapes or allocates a frame.
    let mut retirement = Some(NativeMountRetirement {
        retained_source: Some(Arc::clone(&source)),
        destination_guard: None,
        startup_source_watch: None,
        #[cfg(windows)]
        warm_namespace: None,
    });
    let mut driver = None;
    #[cfg(windows)]
    if let Some(lineage) = source.native_namespace_lineage() {
        lineage
            .capture_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .namespace_bound = true;
    }
    let result = start_driver_into(
        &request,
        source,
        work,
        &mut driver,
        &mut retirement,
        |packet| {
            let _services = adapter::CallbackRuntime::existing()?;
            work.state.lock().check()?;
            MountDestinationGuard::acquire_with_work(
                &request.destination,
                allow_nonempty,
                work,
                &mut packet.destination_guard,
                false,
            )
        },
    );
    // A prebirth rejection leaves its packet here. An accepted loop owns it
    // even on failure; the retained driver signals that same owner to retire.
    let (destination_guard, retained_source, startup_source_watch) = match retirement {
        Some(packet) => (
            packet.destination_guard,
            packet.retained_source,
            packet.startup_source_watch,
        ),
        None => (None, None, None),
    };
    let mut owner = NativeMountSession::new(request, retained_source, destination_custody);
    owner.driver = driver;
    owner.destination_guard = destination_guard;
    owner.startup_source_watch = startup_source_watch;
    match result {
        Ok(()) => {
            drop(storage);
            Ok(owner)
        }
        Err(error) => Err(NativeMountError::StartupFailed(
            storage.failed(error, owner),
        )),
    }
}

/// Starts a native projection mounted directly over an existing, non-empty directory â€” its contents are shadowed for the mount's duration and reappear once the returned session stops.
pub fn mount_native_over_existing(
    request: NativeMountRequest,
    source: Arc<dyn MountFilesystem>,
) -> Result<NativeMountSession, NativeMountError> {
    #[cfg(windows)]
    {
        let _ = (request, source);
        Err(NativeMountError::WritableUnavailable(
            "Windows ProjFS cannot shadow an existing directory".to_owned(),
        ))
    }
    #[cfg(not(windows))]
    {
        mount_native_ordinary(request, source, true)
    }
}

/// Provider-owned paths of one destination: its fence, the process registry,
/// and on Windows the ProjFS staging sibling.
#[cfg(windows)]
pub type NativeMountControlPaths = [PathBuf; 3];
/// Provider-owned paths of one destination: its fence and the process registry.
#[cfg(not(windows))]
pub type NativeMountControlPaths = [PathBuf; 2];

/// Exact provider metadata beside an absolute, canonical mount destination.
/// Composed namespaces exclude these paths from their parent volume's source
/// and capture, so a parent never lists another provider's transient state.
pub fn native_mount_control_paths(
    destination: &Path,
) -> Result<NativeMountControlPaths, NativeMountError> {
    if !destination.is_absolute()
        || destination.components().any(|part| {
            matches!(
                part,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
    {
        return Err(NativeMountError::InvalidDestination);
    }
    let (fence, registry) = MountDestinationGuard::paths_for_canonical(destination)?;
    #[cfg(windows)]
    return Ok([fence, registry, projfs::staging_path(destination)]);
    #[cfg(not(windows))]
    Ok([fence, registry])
}

/// Reclaims the process fence for one externally detached destination.
pub fn reclaim_native_mount_destination_fence(destination: &Path) -> Result<(), NativeMountError> {
    let canonical = destination
        .canonicalize()
        .map_err(|_| NativeMountError::InvalidDestination)?;
    MountDestinationGuard::reclaim_canonical(&canonical)
}

/// Recovers one implementation-owned disposable native destination after its provider process dies.
pub fn recover_native_mount_destination(destination: &Path) -> Result<(), NativeMountError> {
    let mut owner = NativeMountRecoveryOwner::acquire(destination)?;
    match owner.recover() {
        Ok(_) => Ok(()),
        Err(error) => Err(NativeMountError::RecoveryFailed {
            error: Box::new(error),
            owner,
        }),
    }
}

/// Detaches and unfences one crash-left native destination while retaining its underlying directory and authored host residue for an explicit caller decision.
pub fn detach_native_mount_destination_after_crash(
    destination: &Path,
) -> Result<(), NativeMountError> {
    let mut owner = NativeMountRecoveryOwner::acquire(destination)?;
    #[cfg(target_os = "macos")]
    let initialized =
        acyclic_native_runtime::initialize_native_io_services().map_err(NativeMountError::from);
    #[cfg(not(target_os = "macos"))]
    let initialized = Ok(());
    let result = initialized.and_then(|()| {
        let work = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .map_err(|(error, _)| NativeMountError::from(error))?;
        owner.detach(&work)
    });
    match result {
        Ok(_) => Ok(()),
        Err(error) => {
            owner.mark_failed();
            Err(NativeMountError::RecoveryFailed {
                error: Box::new(error),
                owner,
            })
        }
    }
}
