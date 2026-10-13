use super::*;

pub(in crate::native_mount::materialize) fn replace_live_mount(
    staged_parent: &HostRoot,
    staged_name: &Path,
    parent: &HostRoot,
    destination_name: &Path,
    relative: &Path,
    work: &mut impl SourceDemandReadWork,
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
                    rename_entry(parent, destination_name, parent, backup, work)
                        .map_err(restore_demand_error)?;
                    crate::native_host::sync_directory_held(&parent.directory, work)
                        .map_err(restore_demand_error)?;
                    if let Err(error) =
                        rename_entry(staged_parent, staged_name, parent, destination_name, work)
                    {
                        if let Err(rollback) =
                            rename_entry(parent, backup, parent, destination_name, work)
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
                    remove_entry(parent, backup, work).map_err(restore_demand_error)?;
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
    work: &mut impl SourceDemandReadWork,
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
                    let backup_exists =
                        crate::native_host::entry_identity(&parent.directory, backup, work)
                            .map_err(restore_demand_error)?
                            .is_some();
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
                    if crate::native_host::entry_identity(&parent.directory, destination_name, work)
                        .map_err(restore_demand_error)?
                        .is_some()
                    {
                        remove_entry(parent, backup, work).map_err(restore_demand_error)?;
                    } else {
                        rename_entry(parent, backup, parent, destination_name, work)
                            .map_err(restore_demand_error)?;
                    }
                    crate::native_host::sync_directory_held(&parent.directory, work)
                        .map_err(restore_demand_error)?;
                    remove_restore_witness(parent, witness, work)
                },
            )
        },
    )
}
