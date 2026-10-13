use super::*;

pub(in crate::native_mount::materialize) fn cleanup_removed_restore(
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
                    match {
                        #[cfg(unix)]
                        {
                            parent.stat(removed, work)
                        }
                        #[cfg(windows)]
                        {
                            crate::native_host::with_wide_path(removed, work, |path, work| {
                                parent.stat(path, work)
                            })
                        }
                    } {
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
                            {
                                #[cfg(unix)]
                                {
                                    parent.remove(removed, None, work)
                                }
                                #[cfg(windows)]
                                {
                                    crate::native_host::with_wide_path(
                                        removed,
                                        work,
                                        |path, work| parent.remove(path, None, work),
                                    )
                                }
                            }
                            .map_err(restore_demand_error)?;
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

pub(in crate::native_mount::materialize) fn remove_restored_path(
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
                            {
                                #[cfg(unix)]
                                {
                                    crate::native_host::rename_held(
                                        &(parent).directory,
                                        destination_name,
                                        &(parent).directory,
                                        removed,
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
                                                removed,
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
                            {
                                #[cfg(unix)]
                                {
                                    parent.remove(removed, None, work)
                                }
                                #[cfg(windows)]
                                {
                                    crate::native_host::with_wide_path(
                                        removed,
                                        work,
                                        |path, work| parent.remove(path, None, work),
                                    )
                                }
                            }
                            .map_err(restore_demand_error)?;
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
