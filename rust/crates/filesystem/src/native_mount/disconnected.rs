use super::*;

mod linux;
#[cfg(all(test, acyclic_fs_loom))]
mod models;

// The kernel boundary owns a pinned mount and its existing destination fence.
// Both the Linux implementation and the instrumented kernel shim run this
// routine; detection never authorizes an unfenced pathname-based detach.
trait Destination {
    type Fence<'a>
    where
        Self: 'a;
    fn disconnected(&self) -> Result<bool, NativeMountError>;
    fn acquire(&self) -> Result<Self::Fence<'_>, NativeMountError>;
    fn still_current(&self) -> Result<bool, NativeMountError>;
    fn detach(&self, fence: Self::Fence<'_>) -> Result<(), NativeMountError>;
}

fn recover(destination: &impl Destination) -> Result<bool, NativeMountError> {
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
