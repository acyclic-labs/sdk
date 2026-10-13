use super::*;

pub(in crate::native_mount::materialize) fn replace_live_mount(
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
                    {
                        #[cfg(unix)]
                        {
                            crate::native_host::rename_held(
                                &(parent).directory,
                                destination_name,
                                &(parent).directory,
                                backup,
                                acyclic_native_runtime::RenameMode::Replace,
                                None,
                                work,
                            )
                        }
                        #[cfg(windows)]
                        {
                            crate::native_host::with_wide_path(
                                destination_name,
                                work,
                                |name, work| {
                                    crate::native_host::with_wide_path(
                                        backup,
                                        work,
                                        |target, work| {
                                            crate::native_host::rename_held(
                                                &(parent).directory,
                                                name,
                                                &(parent).directory,
                                                target,
                                                acyclic_native_runtime::RenameMode::Replace,
                                                None,
                                                work,
                                            )
                                        },
                                    )
                                },
                            )
                        }
                    }
                    .map_err(restore_demand_error)?;
                    crate::native_host::sync_directory_held(&parent.directory, work)
                        .map_err(restore_demand_error)?;
                    if let Err(error) = {
                        #[cfg(unix)]
                        {
                            crate::native_host::rename_held(
                                &(staged_parent).directory,
                                staged_name,
                                &(parent).directory,
                                destination_name,
                                acyclic_native_runtime::RenameMode::Replace,
                                None,
                                work,
                            )
                        }
                        #[cfg(windows)]
                        {
                            crate::native_host::with_wide_path(staged_name, work, |name, work| {
                                crate::native_host::with_wide_path(
                                    destination_name,
                                    work,
                                    |target, work| {
                                        crate::native_host::rename_held(
                                            &(staged_parent).directory,
                                            name,
                                            &(parent).directory,
                                            target,
                                            acyclic_native_runtime::RenameMode::Replace,
                                            None,
                                            work,
                                        )
                                    },
                                )
                            })
                        }
                    } {
                        if let Err(rollback) = {
                            #[cfg(unix)]
                            {
                                crate::native_host::rename_held(
                                    &(parent).directory,
                                    backup,
                                    &(parent).directory,
                                    destination_name,
                                    acyclic_native_runtime::RenameMode::Replace,
                                    None,
                                    work,
                                )
                            }
                            #[cfg(windows)]
                            {
                                crate::native_host::with_wide_path(backup, work, |name, work| {
                                    crate::native_host::with_wide_path(
                                        destination_name,
                                        work,
                                        |target, work| {
                                            crate::native_host::rename_held(
                                                &(parent).directory,
                                                name,
                                                &(parent).directory,
                                                target,
                                                acyclic_native_runtime::RenameMode::Replace,
                                                None,
                                                work,
                                            )
                                        },
                                    )
                                })
                            }
                        } {
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
                    {
                        #[cfg(unix)]
                        {
                            parent.remove(backup, None, work)
                        }
                        #[cfg(windows)]
                        {
                            crate::native_host::with_wide_path(backup, work, |path, work| {
                                parent.remove(path, None, work)
                            })
                        }
                    }
                    .map_err(restore_demand_error)?;
                    crate::native_host::sync_directory_held(&parent.directory, work)
                        .map_err(restore_demand_error)?;
                    remove_restore_witness(parent, witness, work)
                },
            )
        },
    )
}

pub(in crate::native_mount::materialize) fn recover_live_mount_replacement(
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
                    let backup_exists = match {
                        #[cfg(unix)]
                        {
                            parent.stat(backup, work)
                        }
                        #[cfg(windows)]
                        {
                            crate::native_host::with_wide_path(backup, work, |path, work| {
                                parent.stat(path, work)
                            })
                        }
                    } {
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
                    match {
                        #[cfg(unix)]
                        {
                            parent.stat(destination_name, work)
                        }
                        #[cfg(windows)]
                        {
                            crate::native_host::with_wide_path(
                                destination_name,
                                work,
                                |path, work| parent.stat(path, work),
                            )
                        }
                    } {
                        Ok(_) => {
                            #[cfg(unix)]
                            {
                                parent.remove(backup, None, work)
                            }
                            #[cfg(windows)]
                            {
                                crate::native_host::with_wide_path(backup, work, |path, work| {
                                    parent.remove(path, None, work)
                                })
                            }
                        }
                        .map_err(restore_demand_error)?,
                        Err(crate::demand::DemandError::Io(error))
                            if error.kind() == std::io::ErrorKind::NotFound =>
                        {
                            {
                                #[cfg(unix)]
                                {
                                    crate::native_host::rename_held(
                                        &(parent).directory,
                                        backup,
                                        &(parent).directory,
                                        destination_name,
                                        acyclic_native_runtime::RenameMode::Replace,
                                        None,
                                        work,
                                    )
                                }
                                #[cfg(windows)]
                                {
                                    crate::native_host::with_wide_path(
                                        backup,
                                        work,
                                        |name, work| {
                                            crate::native_host::with_wide_path(
                                                destination_name,
                                                work,
                                                |target, work| {
                                                    crate::native_host::rename_held(
                                                        &(parent).directory,
                                                        name,
                                                        &(parent).directory,
                                                        target,
                                                        acyclic_native_runtime::RenameMode::Replace,
                                                        None,
                                                        work,
                                                    )
                                                },
                                            )
                                        },
                                    )
                                }
                            }
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
