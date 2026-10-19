use super::*;

pub(super) async fn host_io<T: Send + 'static, E: Send + 'static>(
    lock: Option<File>,
    mut work: crate::NativeMaterializationWork,
    operation: impl FnOnce(&mut crate::NativeMaterializationWork) -> Result<T, E> + Send + 'static,
) -> Result<Result<T, E>, crate::NativeTreeMaterializationError> {
    let observer = work.runtime_meter();
    let original = work.clone();
    let operation = move || {
        // A cancelled waiter must not release the fence while host I/O continues.
        let _restore_lock = lock;
        operation(&mut work)
    };
    let task = acyclic_native_runtime::run_blocking_io(operation, Some((observer, false)))
        .map_err(|error| original.runtime_error(error))?;
    task.await.map_err(|error| original.runtime_error(error))
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

pub(super) async fn finish_restore(
    prepared: PreparedRestore,
    replacement: HostPathReplacement,
    materialized: Result<(), MaterializeError>,
    cleanup_slot: (Box<MaterializeError>, crate::NativeAllocation),
    work: crate::NativeMaterializationWork,
) -> Result<HostPathRestore, MaterializeError> {
    let PreparedRestore {
        root,
        destination_parent,
        stage_owner,
        stage_root,
        stage_name,
        stage_path,
        destination_root,
        relative,
        lock,
    } = prepared;
    host_io(Some(lock), work, move |work| {
        let mut cleanup_slot = cleanup_slot;
        let relative = relative;
        let stage_name = stage_name;
        // Every destination operation retains the originally bound
        // root and parent; publication never reopens an ambient path.
        let _root = root;
        let _destination_root = destination_root;
        let _stage_path = stage_path;
        let name = relative
            .0
            .file_name()
            .ok_or(MaterializeError::InvalidPath)?;
        let result = match materialized {
            Ok(_) => publish_restore(
                &destination_parent,
                &stage_root,
                &relative.0,
                replacement,
                work,
            )
            .map(|()| HostPathRestore::Restored),
            Err(MaterializeError::MissingPath) => {
                remove_restored_path(&destination_parent, Path::new(name), &relative.0, work)
                    .map(|()| HostPathRestore::Removed)
            }
            Err(error) => Err(error),
        };
        let stage_identity = stage_root.identity();
        drop(stage_root);
        let cleanup = {
            #[cfg(unix)]
            {
                stage_owner.remove(&stage_name.0, Some(stage_identity), work)
            }
            #[cfg(windows)]
            {
                crate::native_host::with_wide_path(&stage_name.0, work, |path, work| {
                    stage_owner.remove(path, Some(stage_identity), work)
                })
            }
        }
        .and_then(|()| crate::native_host::sync_directory_held(&stage_owner.directory, work));
        match (result, cleanup) {
            (Ok(value), Ok(())) => Ok(value),
            (Ok(_), Err(error)) => Err(MaterializeError::from(error)),
            (Err(error), Ok(())) => Err(error),
            (Err(primary), Err(cleanup)) => {
                *cleanup_slot.0 = primary;
                Err(MaterializeError::RestoreCleanup {
                    primary: cleanup_slot.0,
                    cleanup,
                    allocation: cleanup_slot.1,
                })
            }
        }
    })
    .await
    .map_err(MaterializeError::from)?
}

pub(super) fn prepare_restore(
    destination_root: (PathBuf, crate::NativeAllocation),
    relative: (PathBuf, crate::NativeAllocation),
    expected_root_identity: Option<crate::NativeRootIdentity>,
    replacement: HostPathReplacement,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<PreparedRestore, MaterializeError> {
    let allocation_work = destination_root.1.work().clone();
    let root = HostRoot::open(&destination_root.0, work).map_err(MaterializeError::from)?;
    let actual = root.identity();
    if let Some(expected) = expected_root_identity
        && actual != expected
    {
        return Err(MaterializeError::AllocationChanged { expected, actual });
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
        .map_err(MaterializeError::from)?,
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
    let stage_owner = match replacement {
        HostPathReplacement::Atomic => {
            HostRoot::open(&stage_parent_path.0, work).map_err(MaterializeError::from)?
        }
        HostPathReplacement::LiveMount => {
            #[cfg(unix)]
            {
                destination_parent.open_child(Path::new(""), work)
            }
            #[cfg(windows)]
            {
                crate::native_host::with_wide_path(Path::new(""), work, |path, work| {
                    destination_parent.open_child(path, work)
                })
            }
        }
        .map_err(MaterializeError::from)?,
    };
    let (stage_name, stage_root) = create_restore_stage(&stage_owner, &allocation_work, work)?;
    let stage_path = match allocation_work
        .path(&stage_parent_path.0, Some(&stage_name.0))
        .map_err(MaterializeError::from)
    {
        Ok(path) => path,
        Err(error) => {
            let identity = stage_root.identity();
            drop(stage_root);
            let _ = {
                #[cfg(unix)]
                {
                    stage_owner.remove(&stage_name.0, Some(identity), work)
                }
                #[cfg(windows)]
                {
                    crate::native_host::with_wide_path(&stage_name.0, work, |path, work| {
                        stage_owner.remove(path, Some(identity), work)
                    })
                }
            }
            .and_then(|()| crate::native_host::sync_directory_held(&stage_owner.directory, work));
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
    if crate::native_host::entry_identity(&destination_parent.directory, name, work)
        .map_err(MaterializeError::from)?
        .is_some()
    {
        match replacement {
            HostPathReplacement::Atomic => crate::native_exchange::exchange_native_entries(
                &destination_parent.directory,
                name,
                &stage_parent.directory,
                name,
                work,
            )
            .map_err(MaterializeError::from)?,
            HostPathReplacement::LiveMount => replace_live_mount(
                &stage_parent,
                name,
                destination_parent,
                name,
                relative,
                work,
            )?,
        }
    } else {
        {
            #[cfg(unix)]
            {
                crate::native_host::rename_held(
                    &(stage_parent).directory,
                    name,
                    &(destination_parent).directory,
                    name,
                    acyclic_native_runtime::RenameMode::Replace,
                    None,
                    work,
                )
            }
            #[cfg(windows)]
            {
                crate::native_host::with_wide_path(name, work, |name, work| {
                    crate::native_host::rename_held(
                        &stage_parent.directory,
                        name,
                        &destination_parent.directory,
                        name,
                        acyclic_native_runtime::RenameMode::Replace,
                        None,
                        work,
                    )
                })
            }
        }
        .map_err(MaterializeError::from)?;
        crate::native_host::sync_directory_held(&destination_parent.directory, work)
            .map_err(MaterializeError::from)?;
        crate::native_host::sync_directory_held(&stage_parent.directory, work)
            .map_err(MaterializeError::from)?;
    }
    Ok(())
}

pub(super) fn held_parent(
    root: &HostRoot,
    relative: &Path,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<HostRoot, MaterializeError> {
    {
        #[cfg(unix)]
        {
            root.create_dir_all_held(relative.parent().unwrap_or_else(|| Path::new("")), work)
        }
        #[cfg(windows)]
        {
            crate::native_host::with_wide_path(
                relative.parent().unwrap_or_else(|| Path::new("")),
                work,
                |path, work| root.create_dir_all_held(path, work),
            )
        }
    }
    .map_err(MaterializeError::from)
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
        work.admit(delta).map_err(MaterializeError::from)?;
        let allocation = allocation_work.claim(
            bytes as u64,
            u64::from((bytes as u64) != 0),
            crate::WorkCounters::UNCHARGED,
        )?;
        let mut name = String::with_capacity(bytes);
        write!(name, ".acyclic-restore-{process}-{sequence}")
            .map_err(|_| MaterializeError::InvalidOptions)?;
        work.record(delta).map_err(MaterializeError::from)?;
        let name = (PathBuf::from(name), allocation);
        #[cfg(unix)]
        let created = parent.create_dir(&name.0, work);
        #[cfg(windows)]
        let created = crate::native_host::with_wide_path(&name.0, work, |path, work| {
            parent.create_dir(path, work)
        });
        match created {
            Ok(()) => {
                #[cfg(unix)]
                let opened = parent.open_child(&name.0, work);
                #[cfg(windows)]
                let opened = crate::native_host::with_wide_path(&name.0, work, |path, work| {
                    parent.open_child(path, work)
                });
                let root = match opened {
                    Ok(root) => root,
                    // Without the stage capability no identity authorizes
                    // deleting this name, which may already have been swapped.
                    Err(error) => return Err(MaterializeError::from(error)),
                };
                if let Err(error) = crate::native_host::sync_directory_held(&parent.directory, work)
                    .map_err(MaterializeError::from)
                {
                    let identity = root.identity();
                    drop(root);
                    let _ = {
                        #[cfg(unix)]
                        {
                            parent.remove(&name.0, Some(identity), work)
                        }
                        #[cfg(windows)]
                        {
                            crate::native_host::with_wide_path(&name.0, work, |path, work| {
                                parent.remove(path, Some(identity), work)
                            })
                        }
                    }
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
            Err(error) => return Err(MaterializeError::from(error)),
        }
    }
    Err(MaterializeError::Io(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "restore stage names are exhausted",
    )))
}

mod artifacts;
pub(super) use artifacts::*;
mod removal;
pub(super) use removal::*;
mod live;
pub(super) use live::*;
mod host;
pub(super) use host::*;
