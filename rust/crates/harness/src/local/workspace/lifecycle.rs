use super::*;

// The filesystem's exclusive local owner is held throughout open. Recovery
// additionally acquires each original native destination fence before detach;
// a surviving provider therefore prevents recovery rather than losing custody.
pub(super) fn recover(project: &Path, directory: &Path) -> Result<()> {
    recover_mount(&project.join(volumes::SCRATCH_PATH))?;
    match std::fs::read_dir(directory.join("task-views")) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry.map_err(storage)?;
                if entry.file_type().map_err(storage)?.is_dir() {
                    recover_mount(&entry.path())?;
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(storage(error)),
    }
    Ok(())
}

fn recover_mount(path: &Path) -> Result<()> {
    let [fence, _] = acyclic_fs::native_mount_control_paths(path).map_err(storage)?;
    if fence.try_exists().map_err(storage)? {
        // Preserve underlying host residue; recovery never removes a directory.
        acyclic_fs::detach_native_mount_destination_after_crash(path).map_err(storage)?;
    }
    Ok(())
}

pub(in crate::local) async fn close(root: Arc<LocalWorkspace>) -> Result<()> {
    let mut pending = vec![root];
    let mut workspaces = Vec::new();
    let mut visited = std::collections::BTreeSet::new();
    while let Some(workspace) = pending.pop() {
        if !visited.insert(workspace.path.clone()) {
            continue;
        }
        // Never hold two view guards or a guard during native retirement.
        {
            let state = crate::sync::owned::lock_owned(StateArc::clone(&workspace.state)).await;
            pending.extend(state.workspaces());
        }
        workspaces.push(workspace);
    }
    let mut failed = None;
    for workspace in workspaces.iter().rev() {
        if let Some(native) = &workspace.mounted {
            if let Err(error) = native.unmount().await {
                failed.get_or_insert_with(|| storage(error));
            }
        }
    }
    // Task views link shared volume mounts. Retire those only after every
    // sparse task/branch projection has settled, once per physical destination.
    let mut retired = std::collections::BTreeSet::new();
    for workspace in &workspaces {
        for (_, mount) in workspace.mounts.mounts() {
            if let Some(source) = &mount.view.source {
                let native = source.native().await;
                let Some(native) = native.as_ref() else {
                    continue;
                };
                if retired.insert(native.path().to_owned()) {
                    if let Err(error) = native.unmount().await {
                        failed.get_or_insert_with(|| storage(error));
                    }
                }
            }
        }
    }
    failed.map_or(Ok(()), Err)
}
