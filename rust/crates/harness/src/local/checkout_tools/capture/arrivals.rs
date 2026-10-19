//! Register only arriving subtrees before capture reads their current contents.
use super::*;
use acyclic_fs::{
    WatchChange,
    native_capture::{host_path_to_namespace, namespace_to_host_path},
};

pub(super) fn register(
    watcher: &mut NativeWatch,
    batch: &WatchBatch,
    workspace: &LocalWorkspace,
    policy: &CapturePolicy,
) -> Result<()> {
    let WatchBatch::Changes { changes, .. } = batch else {
        return Ok(());
    };
    let config = workspace.view().workspace().volume_config();
    let mut pending = Vec::new();
    for change in changes {
        let path = match change {
            WatchChange::Created(path) | WatchChange::Arrived(path) => path,
            WatchChange::Renamed { to, .. } => to,
            _ => continue,
        };
        if policy
            .excluded_prefixes()
            .iter()
            .any(|excluded| path.is_within(excluded))
        {
            continue;
        }
        pending.push(namespace_to_host_path(path).map_err(storage)?);
    }
    let mut remaining = MAXIMUM_PATHS;
    while let Some(relative) = pending.pop() {
        let path = workspace.path.join(&relative);
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(storage(error)),
        };
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            continue;
        }
        let namespace =
            host_path_to_namespace(&relative, config.profile, config.limits).map_err(storage)?;
        if policy
            .excluded_prefixes()
            .iter()
            .any(|excluded| namespace.is_within(excluded))
        {
            continue;
        }
        // Register before reading children. Changes during this scan remain in
        // the native queue for the next capture; current contents are captured now.
        watcher.watch_directory(&namespace).map_err(storage)?;
        for entry in std::fs::read_dir(path).map_err(storage)? {
            remaining = remaining
                .checked_sub(1)
                .ok_or_else(|| Error::Invalid("arriving subtree exceeds capture bound".into()))?;
            let entry = entry.map_err(storage)?;
            let kind = entry.file_type().map_err(storage)?;
            if kind.is_dir() && !kind.is_symlink() {
                if pending.len() >= remaining as usize {
                    return Err(Error::Invalid(
                        "arriving subtree exceeds capture bound".into(),
                    ));
                }
                pending.push(relative.join(entry.file_name()));
            }
        }
    }
    Ok(())
}
