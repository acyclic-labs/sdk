use super::*;

pub(super) async fn host_io<T: Send + 'static, E: Send + 'static>(
    lock: Option<File>,
    work: crate::NativeMaterializationWork,
    operation: impl FnOnce(&mut crate::NativeMaterializationWork) -> Result<T, E> + Send + 'static,
) -> Result<Result<T, E>, std::io::Error> {
    let observer = work.runtime_meter();
    let operation = move || {
        // A cancelled waiter must not release the fence while host I/O continues.
        let _restore_lock = lock;
        operation(&mut work.clone())
    };
    let task = acyclic_native_runtime::run_blocking_io(operation, Some((observer, false)))?;
    task.await
}

pub(super) fn restore_demand_error(error: crate::demand::DemandError) -> MaterializeError {
    match error {
        crate::demand::DemandError::Work(error) => MaterializeError::Work(error),
        crate::demand::DemandError::Io(error) => MaterializeError::Io(error),
        crate::demand::DemandError::Cancelled => {
            MaterializeError::Cancelled(crate::CancellationError)
        }
        _ => MaterializeError::InvalidDestination,
    }
}

pub(super) struct PreparedRestore {
    pub(super) root: HostRoot,
    pub(super) destination_parent: HostRoot,
    pub(super) stage_owner: HostRoot,
    pub(super) stage_root: HostRoot,
    pub(super) stage_name: (PathBuf, crate::NativeAllocation),
    pub(super) stage_path: (PathBuf, crate::NativeAllocation),
    pub(super) destination_root: (PathBuf, crate::NativeAllocation),
    pub(super) relative: (PathBuf, crate::NativeAllocation),
    pub(super) lock: File,
}

pub(super) fn prepare_restore(
    destination_root: (PathBuf, crate::NativeAllocation),
    relative: (PathBuf, crate::NativeAllocation),
    expected_root_identity: Option<crate::NativeRootIdentity>,
    replacement: HostPathReplacement,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<PreparedRestore, MaterializeError> {
    let allocation_work = destination_root.1.work().clone();
    let root = HostRoot::open(&destination_root.0, work).map_err(restore_demand_error)?;
    let actual = root.identity();
    if let Some(expected) = expected_root_identity {
        if actual != expected {
            return Err(MaterializeError::AllocationChanged { expected, actual });
        }
    }
    // No parent creation, lock creation, recovery, or stage mutation precedes
    // the identity check on the capability that owns the destination writes.
    let destination_parent = held_parent(&root, &relative.0, work)?;
    let destination_name = relative
        .0
        .file_name()
        .ok_or(MaterializeError::InvalidPath)?;
    let lock = acquire_restore_lock(&destination_parent, &relative.0, true, work)?;
    cleanup_removed_restore(&destination_parent, &relative.0, work)?;
    match replacement {
        HostPathReplacement::Atomic => crate::native_exchange::recover_native_entry_exchange(
            &destination_parent.directory,
            Path::new(destination_name),
            work,
        )
        .map_err(restore_demand_error)?,
        HostPathReplacement::LiveMount => recover_live_mount_replacement(
            &destination_parent,
            Path::new(destination_name),
            &relative.0,
            work,
        )?,
    }
    let stage_parent_path = match replacement {
        HostPathReplacement::Atomic => allocation_work.path(
            destination_root
                .0
                .parent()
                .ok_or(MaterializeError::InvalidPath)?,
            None,
        )?,
        HostPathReplacement::LiveMount => allocation_work.path(
            &destination_root.0,
            Some(relative.0.parent().unwrap_or_else(|| Path::new(""))),
        )?,
    };
    let stage_parent_root = match replacement {
        HostPathReplacement::Atomic => {
            HostRoot::open(&stage_parent_path.0, work).map_err(restore_demand_error)?
        }
        HostPathReplacement::LiveMount => { #[cfg(unix)] { destination_parent.open_child(Path::new(""), work) } #[cfg(windows)] { crate::native_host::with_wide_path(Path::new(""), work, |path, work| destination_parent.open_child(path, work)) } }
            .map_err(restore_demand_error)?,
    };
    let stage_owner = { #[cfg(unix)] { stage_parent_root.create_dir_all_held(Path::new(""), work) } #[cfg(windows)] { crate::native_host::with_wide_path(Path::new(""), work, |path, work| stage_parent_root.create_dir_all_held(path, work)) } }
        .map_err(restore_demand_error)?;
    let (stage_name, stage_root) = create_restore_stage(&stage_owner, &allocation_work, work)?;
    let stage_path = match allocation_work
        .path(&stage_parent_path.0, Some(&stage_name.0))
        .map_err(MaterializeError::from)
    {
        Ok(path) => path,
        Err(error) => {
            let identity = stage_root.identity();
            drop(stage_root);
            let _ = { #[cfg(unix)] { stage_owner.remove(&stage_name.0, Some(identity), work) } #[cfg(windows)] { crate::native_host::with_wide_path(&stage_name.0, work, |path, work| stage_owner.remove(path, Some(identity), work)) } }
                .and_then(|()| {
                    crate::native_host::sync_directory_held(&stage_owner.directory, work)
                });
            return Err(error);
        }
    };
    drop(stage_parent_path);
    Ok(PreparedRestore {
        root,
        destination_parent,
        stage_owner,
        stage_root,
        stage_name,
        stage_path,
        destination_root,
        relative,
        lock,
    })
}

pub(super) fn publish_restore(
    destination_parent: &HostRoot,
    stage_root: &HostRoot,
    relative: &Path,
    replacement: HostPathReplacement,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<(), MaterializeError> {
    let destination_name = relative.file_name().ok_or(MaterializeError::InvalidPath)?;
    let stage_parent = held_parent(stage_root, relative, work)?;
    let name = Path::new(destination_name);
    match { #[cfg(unix)] { destination_parent.stat(name, work) } #[cfg(windows)] { crate::native_host::with_wide_path(name, work, |path, work| destination_parent.stat(path, work)) } } {
        Ok(_) => match replacement {
            HostPathReplacement::Atomic => crate::native_exchange::exchange_native_entries(
                &destination_parent.directory,
                name,
                &stage_parent.directory,
                name,
                work,
            )
            .map_err(restore_demand_error)?,
            HostPathReplacement::LiveMount => replace_live_mount(
                &stage_parent,
                name,
                destination_parent,
                name,
                relative,
                work,
            )?,
        },
        Err(crate::demand::DemandError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            { #[cfg(unix)] { crate::native_host::rename_held(&(stage_parent).directory, name, &(destination_parent).directory, name, acyclic_native_runtime::RenameMode::Replace, None, work) } #[cfg(windows)] { crate::native_host::with_wide_path(name, work, |name, work| crate::native_host::with_wide_path(name, work, |target, work| crate::native_host::rename_held(&(stage_parent).directory, name, &(destination_parent).directory, target, acyclic_native_runtime::RenameMode::Replace, None, work))) } }
                .map_err(restore_demand_error)?;
            crate::native_host::sync_directory_held(&destination_parent.directory, work)
                .map_err(restore_demand_error)?;
            crate::native_host::sync_directory_held(&stage_parent.directory, work)
                .map_err(restore_demand_error)?;
        }
        Err(error) => return Err(restore_demand_error(error)),
    }
    Ok(())
}

pub(super) fn held_parent(
    root: &HostRoot,
    relative: &Path,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<HostRoot, MaterializeError> {
    { #[cfg(unix)] { root.create_dir_all_held(relative.parent().unwrap_or_else(|| Path::new("")), work) } #[cfg(windows)] { crate::native_host::with_wide_path(relative.parent().unwrap_or_else(|| Path::new("")), work, |path, work| root.create_dir_all_held(path, work)) } }
        .map_err(restore_demand_error)
}

pub(super) fn create_restore_stage(
    parent: &HostRoot,
    allocation_work: &crate::NativeMaterializationWork,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<((PathBuf, crate::NativeAllocation), HostRoot), MaterializeError> {
    use std::fmt::Write as _;
    for _ in 0..16 {
        let sequence = RESTORE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let process = std::process::id();
        let digits = |value: u64| {
            if value == 0 {
                1
            } else {
                value.ilog10() as usize + 1
            }
        };
        let bytes = ".acyclic-restore-".len() + digits(u64::from(process)) + 1 + digits(sequence);
        let delta = WorkCounters {
            bytes_encoded: bytes as u64,
            ..WorkCounters::UNCHARGED
        };
        work.admit(delta).map_err(restore_demand_error)?;
        let allocation = allocation_work.claim(
            bytes as u64,
            u64::from((bytes as u64) != 0),
            crate::WorkCounters::UNCHARGED,
        )?;
        let mut name = String::with_capacity(bytes);
        write!(name, ".acyclic-restore-{process}-{sequence}")
            .expect("String formatting cannot fail");
        work.record(delta).map_err(restore_demand_error)?;
        let name = (PathBuf::from(name), allocation);
        match { #[cfg(unix)] { parent.create_dir(&name.0, work) } #[cfg(windows)] { crate::native_host::with_wide_path(&name.0, work, |path, work| parent.create_dir(path, work)) } } {
            Ok(()) => {
                let root = match { #[cfg(unix)] { parent.open_child(&name.0, work) } #[cfg(windows)] { crate::native_host::with_wide_path(&name.0, work, |path, work| parent.open_child(path, work)) } } {
                    Ok(root) => root,
                    // Without the stage capability no identity authorizes
                    // deleting this name, which may already have been swapped.
                    Err(error) => return Err(restore_demand_error(error)),
                };
                if let Err(error) = crate::native_host::sync_directory_held(&parent.directory, work)
                    .map_err(restore_demand_error)
                {
                    let identity = root.identity();
                    drop(root);
                    let _ = { #[cfg(unix)] { parent.remove(&name.0, Some(identity), work) } #[cfg(windows)] { crate::native_host::with_wide_path(&name.0, work, |path, work| parent.remove(path, Some(identity), work)) } }
                        .and_then(|()| {
                            crate::native_host::sync_directory_held(&parent.directory, work)
                        });
                    return Err(error);
                }
                return Ok((name, root));
            }
            Err(crate::demand::DemandError::Io(error))
                if error.kind() == std::io::ErrorKind::AlreadyExists =>
            {
                drop(name);
            }
            Err(error) => return Err(restore_demand_error(error)),
        }
    }
    Err(MaterializeError::Io(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "restore stage names are exhausted",
    )))
}

pub(super) fn restore_artifact_name(
    prefix: &str,
    relative: &Path,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<PathBuf, MaterializeError> {
    use std::fmt::Write as _;
    let mut hasher = blake3::Hasher::new();
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        let bytes = relative.as_os_str().as_bytes();
        work.started_io(WorkCounters {
            bytes_hashed: bytes.len() as u64,
            ..WorkCounters::UNCHARGED
        })
        .map_err(restore_demand_error)?;
        hasher.update(bytes);
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt as _;
        for unit in relative.as_os_str().encode_wide() {
            work.started_io(WorkCounters {
                bytes_hashed: 2,
                bytes_encoded: 2,
                ..WorkCounters::UNCHARGED
            })
            .map_err(restore_demand_error)?;
            hasher.update(&unit.to_le_bytes());
        }
    }
    let bytes = prefix.len().checked_add(64).ok_or(WorkError::Overflow)?;
    let delta = WorkCounters {
        bytes_encoded: bytes as u64,
        ..WorkCounters::UNCHARGED
    };
    work.admit(delta).map_err(restore_demand_error)?;
    work.claim_allocation(bytes as u64, u64::from(bytes != 0), WorkCounters::UNCHARGED).map_err(restore_demand_error)?;
    let mut name = String::with_capacity(bytes);
    write!(name, "{prefix}{}", hasher.finalize().to_hex()).expect("String formatting cannot fail");
    if let Err(error) = work.record(delta) {
        drop(name);
        work.release(bytes as u64).map_err(restore_demand_error)?;
        return Err(restore_demand_error(error));
    }
    Ok(PathBuf::from(name))
}

pub(super) fn with_restore_artifact<T, W: crate::demand::SourceDemandReadWork>(
    prefix: &str,
    relative: &Path,
    work: &mut W,
    operation: impl FnOnce(&Path, &mut W) -> Result<T, MaterializeError>,
) -> Result<T, MaterializeError> {
    let name = restore_artifact_name(prefix, relative, work)?;
    let bytes = name.as_os_str().as_encoded_bytes().len() as u64;
    let result = operation(&name, work);
    drop(name);
    work.release(bytes).map_err(restore_demand_error)?;
    result
}

pub(super) fn acquire_restore_lock(
    parent: &HostRoot,
    relative: &Path,
    blocking: bool,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<File, MaterializeError> {
    with_restore_artifact(".acyclic-restore-lock-", relative, work, |name, work| {
        let file = crate::native_host::open_leaf(
            &parent.directory,
            name,
            &crate::native_host::NativeOpenOptions::ReadWriteCreate,
            work,
        )
        .map_err(restore_demand_error)?;
        loop {
            work.started_io(WorkCounters {
                backend_write_operations: 1,
                ..WorkCounters::UNCHARGED
            })
            .map_err(restore_demand_error)?;
            match fs2::FileExt::try_lock_exclusive(&file) {
                Ok(()) => return Ok(file),
                Err(error) if blocking && error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(error) => return Err(error.into()),
            }
        }
    })
}

pub(super) const REMOVE_RESTORE_WITNESS: &[u8] = b"acyclic-remove-restore-v1\n";
pub(super) const LIVE_RESTORE_WITNESS: &[u8] = b"acyclic-live-restore-v1\n";

pub(super) fn create_restore_witness(
    parent: &HostRoot,
    name: &Path,
    contents: &[u8],
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<(), MaterializeError> {
    use std::io::Write as _;
    let mut file = crate::native_host::open_leaf(
        &parent.directory,
        name,
        &crate::native_host::NativeOpenOptions::CreateNew,
        work,
    )
    .map_err(restore_demand_error)?;
    let mut remaining = contents;
    while !remaining.is_empty() {
        work.admit(WorkCounters {
            output_bytes: remaining.len() as u64,
            backend_write_operations: 1,
            ..WorkCounters::UNCHARGED
        })
        .map_err(restore_demand_error)?;
        work.started_io(WorkCounters {
            backend_write_operations: 1,
            ..WorkCounters::UNCHARGED
        })
        .map_err(restore_demand_error)?;
        match file.write(remaining) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "restore witness write made no progress",
                )
                .into());
            }
            Ok(written) => {
                work.record(WorkCounters {
                    output_bytes: written as u64,
                    ..WorkCounters::UNCHARGED
                })
                .map_err(restore_demand_error)?;
                remaining = &remaining[written..];
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    work.started_io(WorkCounters {
        durability_operations: 1,
        ..WorkCounters::UNCHARGED
    })
    .map_err(restore_demand_error)?;
    file.sync_all()?;
    crate::native_host::sync_directory_held(&parent.directory, work).map_err(restore_demand_error)
}

pub(super) fn validate_restore_witness(
    parent: &HostRoot,
    name: &Path,
    expected: &[u8],
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<bool, MaterializeError> {
    use std::io::Read as _;
    let mut file = match crate::native_host::open_leaf(
        &parent.directory,
        name,
        &crate::native_host::NativeOpenOptions::Read,
        work,
    ) {
        Ok(file) => file,
        Err(crate::demand::DemandError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            return Ok(false);
        }
        Err(error) => return Err(restore_demand_error(error)),
    };
    let mut contents = [0_u8; 64];
    let bound = expected
        .len()
        .checked_add(1)
        .filter(|bound| *bound <= contents.len())
        .ok_or(MaterializeError::InvalidOptions)?;
    let mut read = 0;
    while read < bound {
        work.admit(WorkCounters {
            source_bytes_read: (bound - read) as u64,
            backend_read_operations: 1,
            ..WorkCounters::UNCHARGED
        })
        .map_err(restore_demand_error)?;
        work.started_io(WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::UNCHARGED
        })
        .map_err(restore_demand_error)?;
        match file.read(&mut contents[read..bound]) {
            Ok(0) => break,
            Ok(bytes) => {
                work.record(WorkCounters {
                    source_bytes_read: bytes as u64,
                    ..WorkCounters::UNCHARGED
                })
                .map_err(restore_demand_error)?;
                read += bytes;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    if &contents[..read] == expected {
        Ok(true)
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "restore artifact ownership witness is invalid",
        )
        .into())
    }
}

pub(super) fn remove_restore_witness(
    parent: &HostRoot,
    name: &Path,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<(), MaterializeError> {
    { #[cfg(unix)] { parent.remove(name, None, work) } #[cfg(windows)] { crate::native_host::with_wide_path(name, work, |path, work| parent.remove(path, None, work)) } }.map_err(restore_demand_error)?;
    crate::native_host::sync_directory_held(&parent.directory, work).map_err(restore_demand_error)
}

pub(super) fn cleanup_removed_restore(
    parent: &HostRoot,
    relative: &Path,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<(), MaterializeError> {
    with_restore_artifact(
        ".acyclic-restore-removed-",
        relative,
        work,
        |removed, work| {
            with_restore_artifact(
                ".acyclic-restore-remove-witness-",
                relative,
                work,
                |witness, work| {
                    match { #[cfg(unix)] { parent.stat(removed, work) } #[cfg(windows)] { crate::native_host::with_wide_path(removed, work, |path, work| parent.stat(path, work)) } } {
                        Ok(_) => {
                            if !validate_restore_witness(
                                parent,
                                witness,
                                REMOVE_RESTORE_WITNESS,
                                work,
                            )? {
                                return Err(MaterializeError::Engine(
                                    "unowned restore-removal artifact collision",
                                ));
                            }
                            { #[cfg(unix)] { parent.remove(removed, None, work) } #[cfg(windows)] { crate::native_host::with_wide_path(removed, work, |path, work| parent.remove(path, None, work)) } }.map_err(restore_demand_error)?;
                            crate::native_host::sync_directory_held(&parent.directory, work)
                                .map_err(restore_demand_error)?;
                            remove_restore_witness(parent, witness, work)?;
                        }
                        Err(crate::demand::DemandError::Io(error))
                            if error.kind() == std::io::ErrorKind::NotFound =>
                        {
                            if validate_restore_witness(
                                parent,
                                witness,
                                REMOVE_RESTORE_WITNESS,
                                work,
                            )? {
                                remove_restore_witness(parent, witness, work)?;
                            }
                        }
                        Err(error) => return Err(restore_demand_error(error)),
                    }
                    Ok(())
                },
            )
        },
    )
}

pub(super) fn remove_restored_path(
    parent: &HostRoot,
    destination_name: &Path,
    relative: &Path,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<(), MaterializeError> {
    with_restore_artifact(
        ".acyclic-restore-removed-",
        relative,
        work,
        |removed, work| {
            with_restore_artifact(
                ".acyclic-restore-remove-witness-",
                relative,
                work,
                |witness, work| {
                    create_restore_witness(parent, witness, REMOVE_RESTORE_WITNESS, work)?;
                    match { #[cfg(unix)] { parent.stat(destination_name, work) } #[cfg(windows)] { crate::native_host::with_wide_path(destination_name, work, |path, work| parent.stat(path, work)) } } {
                        Ok(_) => {
                            { #[cfg(unix)] { crate::native_host::rename_held(&(parent).directory, destination_name, &(parent).directory, removed, acyclic_native_runtime::RenameMode::Replace, None, work) } #[cfg(windows)] { crate::native_host::with_wide_path(destination_name, work, |name, work| crate::native_host::with_wide_path(removed, work, |target, work| crate::native_host::rename_held(&(parent).directory, name, &(parent).directory, target, acyclic_native_runtime::RenameMode::Replace, None, work))) } }
                                .map_err(restore_demand_error)?;
                            crate::native_host::sync_directory_held(&parent.directory, work)
                                .map_err(restore_demand_error)?;
                            { #[cfg(unix)] { parent.remove(removed, None, work) } #[cfg(windows)] { crate::native_host::with_wide_path(removed, work, |path, work| parent.remove(path, None, work)) } }.map_err(restore_demand_error)?;
                            crate::native_host::sync_directory_held(&parent.directory, work)
                                .map_err(restore_demand_error)?;
                        }
                        Err(crate::demand::DemandError::Io(error))
                            if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(restore_demand_error(error)),
                    }
                    remove_restore_witness(parent, witness, work)
                },
            )
        },
    )
}

pub(super) fn replace_live_mount(
    staged_parent: &HostRoot,
    staged_name: &Path,
    parent: &HostRoot,
    destination_name: &Path,
    relative: &Path,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<(), MaterializeError> {
    recover_live_mount_replacement(parent, destination_name, relative, work)?;
    with_restore_artifact(
        ".acyclic-restore-backup-",
        relative,
        work,
        |backup, work| {
            with_restore_artifact(
                ".acyclic-restore-live-witness-",
                relative,
                work,
                |witness, work| {
                    create_restore_witness(parent, witness, LIVE_RESTORE_WITNESS, work)?;
                    { #[cfg(unix)] { crate::native_host::rename_held(&(parent).directory, destination_name, &(parent).directory, backup, acyclic_native_runtime::RenameMode::Replace, None, work) } #[cfg(windows)] { crate::native_host::with_wide_path(destination_name, work, |name, work| crate::native_host::with_wide_path(backup, work, |target, work| crate::native_host::rename_held(&(parent).directory, name, &(parent).directory, target, acyclic_native_runtime::RenameMode::Replace, None, work))) } }
                        .map_err(restore_demand_error)?;
                    crate::native_host::sync_directory_held(&parent.directory, work)
                        .map_err(restore_demand_error)?;
                    if let Err(error) =
                        { #[cfg(unix)] { crate::native_host::rename_held(&(staged_parent).directory, staged_name, &(parent).directory, destination_name, acyclic_native_runtime::RenameMode::Replace, None, work) } #[cfg(windows)] { crate::native_host::with_wide_path(staged_name, work, |name, work| crate::native_host::with_wide_path(destination_name, work, |target, work| crate::native_host::rename_held(&(staged_parent).directory, name, &(parent).directory, target, acyclic_native_runtime::RenameMode::Replace, None, work))) } }
                    {
                        if let Err(rollback) =
                            { #[cfg(unix)] { crate::native_host::rename_held(&(parent).directory, backup, &(parent).directory, destination_name, acyclic_native_runtime::RenameMode::Replace, None, work) } #[cfg(windows)] { crate::native_host::with_wide_path(backup, work, |name, work| crate::native_host::with_wide_path(destination_name, work, |target, work| crate::native_host::rename_held(&(parent).directory, name, &(parent).directory, target, acyclic_native_runtime::RenameMode::Replace, None, work))) } }
                        {
                            return Err(MaterializeError::RestoreRollback {
                                primary: error,
                                rollback,
                            });
                        }
                        crate::native_host::sync_directory_held(&parent.directory, work)
                            .map_err(restore_demand_error)?;
                        return Err(restore_demand_error(error));
                    }
                    crate::native_host::sync_directory_held(&parent.directory, work)
                        .map_err(restore_demand_error)?;
                    crate::native_host::sync_directory_held(&staged_parent.directory, work)
                        .map_err(restore_demand_error)?;
                    { #[cfg(unix)] { parent.remove(backup, None, work) } #[cfg(windows)] { crate::native_host::with_wide_path(backup, work, |path, work| parent.remove(path, None, work)) } }.map_err(restore_demand_error)?;
                    crate::native_host::sync_directory_held(&parent.directory, work)
                        .map_err(restore_demand_error)?;
                    remove_restore_witness(parent, witness, work)
                },
            )
        },
    )
}

pub(super) fn recover_live_mount_replacement(
    parent: &HostRoot,
    destination_name: &Path,
    relative: &Path,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<(), MaterializeError> {
    with_restore_artifact(
        ".acyclic-restore-backup-",
        relative,
        work,
        |backup, work| {
            with_restore_artifact(
                ".acyclic-restore-live-witness-",
                relative,
                work,
                |witness, work| {
                    let backup_exists = match { #[cfg(unix)] { parent.stat(backup, work) } #[cfg(windows)] { crate::native_host::with_wide_path(backup, work, |path, work| parent.stat(path, work)) } } {
                        Ok(_) => true,
                        Err(crate::demand::DemandError::Io(error))
                            if error.kind() == std::io::ErrorKind::NotFound =>
                        {
                            false
                        }
                        Err(error) => return Err(restore_demand_error(error)),
                    };
                    if !backup_exists {
                        if validate_restore_witness(parent, witness, LIVE_RESTORE_WITNESS, work)? {
                            remove_restore_witness(parent, witness, work)?;
                        }
                        return Ok(());
                    }
                    if !validate_restore_witness(parent, witness, LIVE_RESTORE_WITNESS, work)? {
                        return Err(MaterializeError::Engine(
                            "unowned live-restore backup collision",
                        ));
                    }
                    match { #[cfg(unix)] { parent.stat(destination_name, work) } #[cfg(windows)] { crate::native_host::with_wide_path(destination_name, work, |path, work| parent.stat(path, work)) } } {
                        Ok(_) => { #[cfg(unix)] { parent.remove(backup, None, work) } #[cfg(windows)] { crate::native_host::with_wide_path(backup, work, |path, work| parent.remove(path, None, work)) } }.map_err(restore_demand_error)?,
                        Err(crate::demand::DemandError::Io(error))
                            if error.kind() == std::io::ErrorKind::NotFound =>
                        {
                            { #[cfg(unix)] { crate::native_host::rename_held(&(parent).directory, backup, &(parent).directory, destination_name, acyclic_native_runtime::RenameMode::Replace, None, work) } #[cfg(windows)] { crate::native_host::with_wide_path(backup, work, |name, work| crate::native_host::with_wide_path(destination_name, work, |target, work| crate::native_host::rename_held(&(parent).directory, name, &(parent).directory, target, acyclic_native_runtime::RenameMode::Replace, None, work))) } }
                                .map_err(restore_demand_error)?;
                        }
                        Err(error) => return Err(restore_demand_error(error)),
                    }
                    crate::native_host::sync_directory_held(&parent.directory, work)
                        .map_err(restore_demand_error)?;
                    remove_restore_witness(parent, witness, work)
                },
            )
        },
    )
}
