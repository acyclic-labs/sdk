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

/// Finite projection of the same native constructor and actual work prefix.
pub fn mount_native_measured(
    request: NativeMountRequest,
    source: Arc<dyn MountFilesystem>,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<crate::OperationReceipt<NativeMountSession>, (NativeMountError, crate::WorkCounters)> {
    let work = match crate::NativeMaterializationWork::new(budget, cancellation) {
        Ok(work) => work,
        Err((error, work)) => {
            return Err((
                NativeMountError::StartupAdmissionFailed {
                    cause: error.into(),
                    source,
                },
                work,
            ));
        }
    };
    let prepared = match prepare_native_mount_start(&work) {
        Ok(prepared) => prepared,
        Err(cause) => {
            return Err((
                NativeMountError::StartupAdmissionFailed { cause, source },
                work.receipt(),
            ));
        }
    };
    match start_native_mount_prepared_with_meter(request, source, false, &work, prepared, None) {
        Ok(value) => Ok(crate::OperationReceipt {
            value,
            work: work.receipt(),
        }),
        Err(error) => Err((error, work.receipt())),
    }
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
        warm_namespace: storage.retired.as_ref().and_then(|old| {
            old.namespace_outcome
                .as_ref()
                .and_then(projfs::ProjFsNamespaceOutcome::qualification)
        }),
    });
    let mut driver = None;
    #[cfg(windows)]
    let mut namespace_binding = None;
    #[cfg(windows)]
    let lineage_admission = source
        .native_namespace_lineage()
        .map(|lineage| {
            adapter::retain_namespace_lineage(&lineage, work, storage.retired.is_some())
                .map(|binding| namespace_binding = Some(binding))
        })
        .transpose()
        .map(|_| ())
        .map_err(NativeMountError::from);
    #[cfg(not(windows))]
    let lineage_admission: Result<(), NativeMountError> = Ok(());
    let result = lineage_admission.and_then(|()| {
        start_driver_into(
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
        )
    });
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
    #[cfg(windows)]
    {
        owner.namespace_binding = namespace_binding;
    }
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

/// Reclaims the process fence for one externally detached destination.
pub fn reclaim_native_mount_destination_fence(destination: &Path) -> Result<(), NativeMountError> {
    let canonical = destination
        .canonicalize()
        .map_err(|_| NativeMountError::InvalidDestination)?;
    MountDestinationGuard::reclaim_canonical(&canonical)
}

/// Reclaims every unlocked native-mount destination fence in one parent.
pub fn reclaim_stale_native_mount_destination_fences(
    parent: &Path,
) -> Result<u32, NativeMountError> {
    const MAXIMUM_PARENT_ENTRIES: usize = 4_096;
    const PREFIX: &str = ".acyclic-fs-mount-";
    const SUFFIX: &str = ".lock";

    let parent = parent
        .canonicalize()
        .map_err(|_| NativeMountError::InvalidDestination)?;
    let metadata =
        std::fs::symlink_metadata(&parent).map_err(|_| NativeMountError::InvalidDestination)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(NativeMountError::InvalidDestination);
    }
    let registry_path = parent.join(".acyclic-fs-mount-registry.lock");
    let registry = acquire_mount_lock(&registry_path, true)?;
    let mut reclaimed = 0_u32;
    for (index, entry) in std::fs::read_dir(&parent)
        .map_err(|error| NativeMountError::Driver(error.to_string()))?
        .enumerate()
    {
        if index >= MAXIMUM_PARENT_ENTRIES {
            return Err(NativeMountError::Driver(
                "native mount parent exceeds its bounded entry count".to_owned(),
            ));
        }
        let entry = entry.map_err(|error| NativeMountError::Driver(error.to_string()))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name == ".acyclic-fs-mount-registry.lock" || !name.starts_with(PREFIX) {
            continue;
        }
        let digest = name
            .strip_prefix(PREFIX)
            .and_then(|value| value.strip_suffix(SUFFIX))
            .ok_or(NativeMountError::InvalidDestination)?;
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(NativeMountError::InvalidDestination);
        }
        let path = entry.path();
        let lock = match acquire_existing_mount_lock(&path) {
            Ok(lock) => lock,
            Err(NativeMountError::DestinationBusy) => continue,
            Err(error) => return Err(error),
        };
        drop(lock);
        match std::fs::remove_file(&path) {
            Ok(()) => {
                reclaimed = reclaimed.checked_add(1).ok_or_else(|| {
                    NativeMountError::Driver("native mount fence count overflow".to_owned())
                })?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(NativeMountError::Driver(error.to_string())),
        }
    }
    drop(registry);
    Ok(reclaimed)
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

/// Reclaims a Windows provider's dead mount path without deleting authored residue.
#[cfg(windows)]
pub fn recover_native_mount_destination_preserving_residue(
    destination: &Path,
) -> Result<Option<PathBuf>, NativeMountError> {
    let name = destination
        .file_name()
        .ok_or(NativeMountError::InvalidDestination)?;
    let parent = destination
        .parent()
        .ok_or(NativeMountError::InvalidDestination)?
        .canonicalize()
        .map_err(|_| NativeMountError::InvalidDestination)?;
    let destination = parent.join(name);
    let mut guard = MountDestinationGuard::acquire_for_recovery(&destination)?;
    let preserved = projfs::quarantine_crashed_destination(&destination)?;
    let cancellation = crate::CancellationToken::new();
    let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
    guard.release(&mut work)?;
    Ok(preserved)
}

/// Detaches and unfences one crash-left native destination while retaining its underlying directory and authored host residue for an explicit caller decision.
pub fn detach_native_mount_destination_after_crash(
    destination: &Path,
) -> Result<(), NativeMountError> {
    let mut owner = NativeMountRecoveryOwner::acquire(destination)?;
    match owner.detach() {
        Ok(_) => Ok(()),
        Err(error) => Err(NativeMountError::RecoveryFailed {
            error: Box::new(error),
            owner,
        }),
    }
}
