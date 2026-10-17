//! Logical volume bindings acquire an OS projection only for native execution.
use super::*;
use crate::sync::owned::{OwnedMutexGuard, lock_owned};

pub(in crate::local) struct Native(
    pub(super) acyclic_fs::Mount<LocalAuthorityBackend, LocalObjectBackend>,
);

impl std::ops::Deref for Native {
    type Target = acyclic_fs::Mount<LocalAuthorityBackend, LocalObjectBackend>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for Native {
    fn drop(&mut self) {
        // Keep callbacks and the destination fence inside the last owner.
        if let Err(error) = self.0.abandon() {
            tracing::error!(?error, "workspace mount retirement failed");
        }
    }
}

// Lock order: checkout state -> one volume projection -> filesystem/provider.
// No callback reacquires either outer lock. Hold this guard through logical
// writes, so first projection cannot pin a head preceding an unpublished write.
pub(super) async fn initialize<T>(
    slot: &StateArc<AsyncMutex<Option<T>>>,
    create: impl AsyncFnOnce() -> Result<T>,
) -> Result<OwnedMutexGuard<Option<T>>> {
    let mut slot = lock_owned(StateArc::clone(slot)).await;
    if slot.is_none() {
        *slot = Some(create().await?);
    }
    Ok(slot)
}

impl MountedVolume {
    pub(in crate::local) async fn native(&self) -> OwnedMutexGuard<Option<Native>> {
        lock_owned(StateArc::clone(&self.native)).await
    }

    pub(super) async fn project(&self, root: &Path, relative: &str, read_only: bool) -> Result<()> {
        initialize(&self.native, async || {
            volumes::create_mount_directory(root, Path::new(relative))?;
            let options = if read_only {
                acyclic_fs::MountOptions::read_only()
            } else {
                acyclic_fs::MountOptions::read_write()
                    .publication(acyclic_fs::MountPublication::Manual)
            };
            self.workspace
                .workspace()
                .mount(&self.destination, options)
                .await
                .map(Native)
                .map_err(Error::from)
        })
        .await?;
        Ok(())
    }
}

#[cfg(all(test, acyclic_harness_loom))]
mod tests;
