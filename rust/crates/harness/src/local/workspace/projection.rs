//! Logical volume bindings acquire an OS projection only for native execution.
use super::*;
use crate::sync::SyncMutex;
use crate::sync::owned::{OwnedMutexGuard, lock_owned};

pub(in crate::local) struct Native {
    mount: acyclic_fs::Mount<LocalAuthorityBackend, LocalObjectBackend>,
    binding: Option<(PathBuf, PathBuf)>,
    // Owned native guards can outlive the logical MountedVolume. Keep the
    // original key occupied until this projection's physical fields retire.
    _binding_owner: Option<acyclic_fs::kernel::allocation::ErasedRetainedArc>,
}

impl std::ops::Deref for Native {
    type Target = acyclic_fs::Mount<LocalAuthorityBackend, LocalObjectBackend>;
    fn deref(&self) -> &Self::Target {
        &self.mount
    }
}

impl Drop for Native {
    fn drop(&mut self) {
        // The mount's session retains callbacks and its destination fence,
        // including physical retirement accepted by a cancelled caller.
        if let Some((destination, view)) = &self.binding
            && let Err(error) = volumes::unlink_binding(destination, view)
        {
            tracing::error!(?error, "volume binding retirement failed");
        }
    }
}

type PublicationGuard = OwnedMutexGuard<Option<Native>>;

/// One volume's distinct projections, retained in command lock order across
/// logical publication. Read-only aliases also observe the committed head.
pub(in crate::local) struct PublicationGroup {
    slots: Vec<PublicationGuard>,
    _storage: Option<acyclic_fs::NativeAllocation>,
}

impl PublicationGroup {
    pub(in crate::local) async fn acquire(
        mounts: &Mounts,
        volume: &VolumeRef,
        selected: acyclic_fs::WorkspaceId,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Self> {
        let ordered = volumes::ordered_mounts(mounts.volume_mounts(volume, io)?);
        let bytes = ordered
            .len()
            .checked_mul(std::mem::size_of::<PublicationGuard>())
            .ok_or_else(|| Error::Invalid("native publication storage overflows".into()))?;
        let allocation = io
            .as_ref()
            .and_then(crate::store::HistoryIo::original_native_work)
            .map(|work| work.claim(bytes as u64, 1, acyclic_fs::WorkCounters::UNCHARGED))
            .transpose()?;
        let mut slots = Vec::new();
        slots.try_reserve_exact(ordered.len()).map_err(storage)?;
        for (_, mount) in ordered {
            if let Some(source) = &mount.view.source
                && source.options.is_some()
            {
                if source.workspace.workspace().id() != selected {
                    return Err(Error::Conflict(
                        "volume projections select different workspaces".into(),
                    ));
                }
                slots.push(source.publication().await?);
            }
        }
        Ok(Self {
            slots,
            _storage: allocation,
        })
    }

    pub(in crate::local) async fn sync(&self) -> Result<()> {
        for slot in &self.slots {
            if let Some(native) = slot.as_ref() {
                native.sync().await?;
            }
        }
        Ok(())
    }

    pub(in crate::local) async fn advance_to_head(&self) -> Result<()> {
        for slot in &self.slots {
            if let Some(native) = slot.as_ref() {
                native.advance_to_head().await?;
            }
        }
        Ok(())
    }
}

// Lock order: checkout state -> projection -> filesystem/provider.
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

/// The host sessions sharing a volume's projection; retirement waits until
/// none remain. Native commands never share it: each runs in its own view.
#[derive(Default)]
pub(in crate::local) struct Users {
    count: SyncMutex<usize>,
    idle: tokio::sync::Notify,
}

impl std::ops::Deref for Users {
    type Target = SyncMutex<usize>;
    fn deref(&self) -> &Self::Target {
        &self.count
    }
}

pub(in crate::local) struct Entered(StateArc<Users>);

impl Drop for Entered {
    fn drop(&mut self) {
        let mut users = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *users = users.checked_sub(1).expect("projection claim underflow");
        let idle = *users == 0;
        drop(users);
        if idle {
            self.0.idle.notify_waiters();
        }
    }
}

// Users are added only under the projection lock, and retirement observes no
// users under that same lock, so a projection never retires under a host
// session and a new session never adopts one that is being retired.
pub(in crate::local) async fn enter<T>(
    slot: &StateArc<AsyncMutex<Option<T>>>,
    users: &StateArc<Users>,
    create: impl AsyncFnOnce() -> Result<T>,
) -> Result<Entered> {
    let slot = initialize(slot, create).await?;
    *users
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
    drop(slot);
    Ok(Entered(StateArc::clone(users)))
}

/// The one hand-back protocol for a command's own view. Each attempt plans
/// against the target's current head and publishes only while that head is
/// still current, or returns `None`. A moved head means another command
/// landed first; the next attempt plans on top of it. No publication is
/// overwritten, and no command waits on another command's view.
pub(in crate::local) async fn compare_and_swap<T, F: Future<Output = Result<Option<T>>>>(
    mut attempt: impl FnMut() -> F,
) -> Result<T> {
    loop {
        if let Some(published) = attempt().await? {
            return Ok(published);
        }
    }
}

impl MountedVolume {
    /// Logical publication holds the projection slot, so no host-session
    /// sync interleaves with it.
    pub(in crate::local) async fn publication(&self) -> Result<OwnedMutexGuard<Option<Native>>> {
        self.prepared_native().await
    }

    pub(in crate::local) async fn native(&self) -> OwnedMutexGuard<Option<Native>> {
        lock_owned(StateArc::clone(&self.native)).await
    }

    pub(in crate::local) async fn enter_with_options(
        &self,
        options: acyclic_fs::MountOptions,
    ) -> Result<Entered> {
        enter(&self.native, &self.users, async || {
            if self.destination.parent() != Some(self.binding_root.path()) {
                return Err(Error::Invalid(
                    "projection differs from its volume binding".into(),
                ));
            }
            let mount = self
                .workspace
                .workspace()
                .mount_view(super::task_views::view_options(
                    &self.task_view_admission,
                    options,
                ))
                .await
                .map_err(Error::from)?;
            let path = mount.path().to_path_buf();
            let mut native = Native {
                mount,
                binding: None,
                _binding_owner: self._binding.clone(),
            };
            // This alias lives in host state. On restart it can still name the preceding process's
            // retired root. Replace only the link itself; never follow it or
            // write its old target. A regular file/nonempty directory fails.
            let previous = volumes::alias_target(&self.destination)?;
            if let Some(previous) = &previous {
                let admission = self.task_view_admission.get().map(Arc::as_ref);
                if !acyclic_fs::shared_view_namespace_contains(previous, admission)?
                    || previous.try_exists().map_err(storage)?
                {
                    return Err(Error::Conflict(
                        "projection alias does not name a retired shared view".into(),
                    ));
                }
            }
            volumes::bind_alias(&self.destination, &path, previous.as_deref())?;
            native.binding = Some((self.destination.clone(), path));
            Ok(native)
        })
        .await
    }

    /// The live projection slot. A retirement that failed earlier finishes
    /// here, before anyone observes the projection again.
    pub(in crate::local) async fn prepared_native(
        &self,
    ) -> Result<OwnedMutexGuard<Option<Native>>> {
        let mut slot = self.native().await;
        if let Some(native) = slot.as_ref()
            && native.is_retiring()
        {
            native.complete_command(true).await?;
            *slot = None;
        }
        Ok(slot)
    }

    pub(super) async fn retire(&self) -> Result<()> {
        loop {
            let idle = self.users.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            let mut slot = self.native().await;
            if *self
                .users
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                == 0
            {
                return self.publish_locked(&mut slot, true).await;
            }
            drop(slot);
            idle.await;
        }
    }

    // The single retirement path: publish, detach, then remove the residue
    // projection created. A failed detach keeps the owner for a later retry.
    async fn publish_locked(&self, slot: &mut Option<Native>, retire: bool) -> Result<()> {
        if let Some(native) = slot.as_ref() {
            native.complete_command(retire).await?;
        }
        if retire {
            *slot = None;
        }
        Ok(())
    }
}

#[cfg(test)]
#[cfg(loom)]
mod tests;
