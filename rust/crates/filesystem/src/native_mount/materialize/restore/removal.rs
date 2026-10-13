use super::*;

pub(in crate::native_mount::materialize) fn cleanup_removed_restore(
    parent: &HostRoot,
    relative: &Path,
    work: &mut impl SourceDemandReadWork,
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
                    if crate::native_host::entry_identity(&parent.directory, removed, work)
                        .map_err(restore_demand_error)?
                        .is_some()
                    {
                        if !validate_restore_witness(parent, witness, REMOVE_RESTORE_WITNESS, work)?
                        {
                            return Err(MaterializeError::Engine(
                                "unowned restore-removal artifact collision",
                            ));
                        }
                        remove_entry(parent, removed, work).map_err(restore_demand_error)?;
                        crate::native_host::sync_directory_held(&parent.directory, work)
                            .map_err(restore_demand_error)?;
                        remove_restore_witness(parent, witness, work)?;
                    } else if validate_restore_witness(
                        parent,
                        witness,
                        REMOVE_RESTORE_WITNESS,
                        work,
                    )? {
                        remove_restore_witness(parent, witness, work)?;
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
    work: &mut impl SourceDemandReadWork,
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
                    if crate::native_host::entry_identity(&parent.directory, destination_name, work)
                        .map_err(restore_demand_error)?
                        .is_some()
                    {
                        rename_entry(parent, destination_name, parent, removed, work)
                            .map_err(restore_demand_error)?;
                        crate::native_host::sync_directory_held(&parent.directory, work)
                            .map_err(restore_demand_error)?;
                        remove_entry(parent, removed, work).map_err(restore_demand_error)?;
                        crate::native_host::sync_directory_held(&parent.directory, work)
                            .map_err(restore_demand_error)?;
                    }
                    remove_restore_witness(parent, witness, work)
                },
            )
        },
    )
}
