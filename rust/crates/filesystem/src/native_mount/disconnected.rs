use super::*;

#[cfg(all(test, acyclic_fs_loom))]
pub(in crate::native_mount) mod kernel;
mod linux;
#[cfg(all(test, acyclic_fs_loom))]
mod models;

// Detection, owner acquisition and detach all operate on the one pinned object.
fn recover(destination: &linux::Pinned) -> Result<bool, NativeMountError> {
    if !destination.disconnected()? {
        return Ok(false);
    }
    let fence = destination.acquire()?;
    if !destination.still_current()? || !destination.disconnected()? {
        return Ok(false);
    }
    destination.detach(fence)?;
    Ok(true)
}

/// Recovers a disconnected FUSE destination, rejecting symbolic-link aliases.
pub fn recover_disconnected_native_mount_destination(
    destination: &Path,
) -> Result<bool, NativeMountError> {
    let Some(destination) = linux::Pinned::open(destination)? else {
        return Ok(false);
    };
    recover(&destination)
}
