//! Logical volume bindings acquire an OS projection only for native execution.
use super::*;
use crate::sync::SyncMutex;
use crate::sync::owned::{OwnedMutexGuard, lock_owned};

pub(in crate::local) struct Native {
    mount: acyclic_fs::Mount<LocalAuthorityBackend, LocalObjectBackend>,
    discard: bool,
}

impl std::ops::Deref for Native {
    type Target = acyclic_fs::Mount<LocalAuthorityBackend, LocalObjectBackend>;
    fn deref(&self) -> &Self::Target {
        &self.mount
    }
}

impl Drop for Native {
    fn drop(&mut self) {
        // Keep callbacks and the destination fence inside the last owner.
        if let Err(error) = self.mount.abandon() {
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

/// An owned execution claim, transferred to the physical command worker.
/// Only worker cleanup or successful return can release it.
pub(in crate::local) struct Entered(StateArc<SyncMutex<usize>>);

impl Drop for Entered {
    fn drop(&mut self) {
        let mut users = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *users = users.checked_sub(1).expect("projection claim underflow");
    }
}

// Claims are added only under the projection lock, and the last scope retires
// under that same lock, so a projection never retires under a running command
// and a new scope never adopts one that is being retired.
pub(in crate::local) async fn enter<T>(
    slot: &StateArc<AsyncMutex<Option<T>>>,
    users: &StateArc<SyncMutex<usize>>,
    create: impl AsyncFnOnce() -> Result<T>,
) -> Result<Entered> {
    let slot = initialize(slot, create).await?;
    *users
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
    drop(slot);
    Ok(Entered(StateArc::clone(users)))
}

pub(in crate::local) async fn leave<T>(
    slot: &StateArc<AsyncMutex<Option<T>>>,
    users: &SyncMutex<usize>,
    entered: Entered,
    publish: impl AsyncFnOnce(&mut Option<T>, bool) -> Result<()>,
) -> Result<()> {
    let mut slot = lock_owned(StateArc::clone(slot)).await;
    // Keep the completed worker's claim until its publication owns the slot.
    // Another completing command must not retire this projection first.
    drop(entered);
    let retire = *users
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        == 0;
    publish(&mut slot, retire).await
}

impl MountedVolume {
    pub(in crate::local) async fn native(&self) -> OwnedMutexGuard<Option<Native>> {
        lock_owned(StateArc::clone(&self.native)).await
    }

    pub(super) async fn enter(
        &self,
        root: &Path,
        relative: &str,
        read_only: bool,
    ) -> Result<Entered> {
        enter(&self.native, &self.users, async || {
            volumes::prepare_mount_directory(root, Path::new(relative))?;
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
                .map(|mount| Native {
                    mount,
                    discard: false,
                })
                .map_err(Error::from)
        })
        .await
    }

    pub(super) async fn leave(&self, entered: Entered) -> Result<()> {
        leave(&self.native, &self.users, entered, async |slot, retire| {
            self.publish_locked(slot, retire).await
        })
        .await
    }

    pub(super) async fn prepare_command(&self) -> Result<()> {
        let mut slot = self.native().await;
        if let Some(native) = slot.as_ref() {
            if native.discard {
                native.abandon_command()?;
            } else if native.is_retiring() {
                native.complete_command(true).await?;
            } else {
                return Ok(());
            }
            *slot = None;
        }
        Ok(())
    }

    pub(super) async fn abandon(&self, entered: Entered) -> Result<()> {
        leave(&self.native, &self.users, entered, async |slot, retire| {
            if !retire {
                return Err(Error::Conflict(
                    "unverified native scope shares a projection".into(),
                ));
            }
            if let Some(native) = slot.as_mut() {
                // Retain this decision if physical retirement fails. A later
                // entry must retry abandonment before exposing this projection.
                native.discard = true;
                native.abandon_command()?;
            }
            *slot = None;
            Ok(())
        })
        .await
    }

    pub(super) async fn retire(&self) -> Result<()> {
        let mut slot = self.native().await;
        if *self
            .users
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            != 0
        {
            return Err(Error::Conflict(
                "native workers still own the volume projection".into(),
            ));
        }
        self.publish_locked(&mut slot, true).await
    }

    // The single retirement path: publish, detach, then remove the residue
    // projection created. A failed detach keeps the owner for a later retry.
    async fn publish_locked(&self, slot: &mut Option<Native>, retire: bool) -> Result<()> {
        if let Some(native) = slot.as_ref() {
            if native.discard {
                native.abandon_command()?;
            } else {
                native.complete_command(retire).await?;
            }
        }
        if retire {
            *slot = None;
        }
        Ok(())
    }
}

#[cfg(test)]
#[cfg(acyclic_harness_loom)]
mod tests;
