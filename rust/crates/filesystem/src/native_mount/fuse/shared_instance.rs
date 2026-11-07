//! The FUSE adapter's instance: one session per shared root directory.
//!
//! The shared core's [`Instances`] registry starts it for a root's first
//! view and drops it after the last; its drop unmounts the root, stops its
//! threads and releases its fence. A failed start unwinds through the same
//! teardown. An admitted owner-exit action retains the original fence and
//! detaches the root if its owner dies. The next start also recovers any
//! retained fence from an earlier failed retirement.

use super::shared::{SharedViewEntry, SharedViews, run_shared_invalidator};
use super::*;
use crate::NativeMaterializationWork;
use crate::native_mount::shared::instances::{Instances, Registration};
use crate::native_mount::{
    MountDestinationGuard, detach_native_mount_destination_after_crash, native_mount_control_paths,
};
use std::path::PathBuf;

static INSTANCES: Instances<SharedFuseInstance> = Instances::new();

pub(in crate::native_mount) struct SharedFuseInstance {
    pub(super) views: Arc<SharedViews>,
    invalidator: Option<std::thread::JoinHandle<()>>,
    #[cfg(test)]
    worker_lifetime: std::sync::Weak<()>,
    #[cfg(test)]
    exit_action_id: Option<u32>,
}

/// The existing invalidator owns physical root cleanup after accepting startup.
/// A partial constructor retains this same payload until that acceptance.
struct SharedFuseRoot {
    views: Arc<SharedViews>,
    root: PathBuf,
    path: std::ffi::CString,
    state: InstanceState,
    // The instance's own meter: it outlives the view that started it.
    work: NativeMaterializationWork,
    // The physical owner releases the root only after its native teardown.
    _registration: Registration<SharedFuseInstance>,
}

#[derive(Default)]
struct InstanceState {
    mount: super::mount::Mount,
    device: Option<Arc<super::host_io::HostIo>>,
    worker: Option<std::thread::JoinHandle<Result<(), NativeMountError>>>,
    transport: Option<Arc<super::host_io::HostIo>>,
    guard: Option<MountDestinationGuard>,
    #[cfg(test)]
    worker_lifetime: std::sync::Weak<()>,
}

impl InstanceState {
    fn detach(&mut self, path: &std::ffi::CStr, root: &Path) -> bool {
        let mut detached = !self.mount.needs_unmount();
        if !detached {
            let result = self.mount.stop(path);
            if let Some(device) = self.device.as_ref() {
                device.stop();
            }
            match result {
                Ok(()) => {
                    // Namespace removal is accepted even while a held file
                    // delays device acknowledgement. The transport stop and
                    // worker join below own that remaining physical lifetime.
                    detached = !self.mount.needs_unmount();
                }
                Err(error) => {
                    tracing::warn!(%error, root = %root.display(), "shared FUSE unmount failed");
                }
            }
        }
        if !detached {
            // Busy: detach the namespace. The physical owner still stops and
            // joins the transport, including while an old file remains open.
            match nix::mount::umount2(path, nix::mount::MntFlags::MNT_DETACH) {
                Ok(()) => {
                    detached = self.mount.cancel_exit_action().is_ok();
                }
                Err(error) => {
                    tracing::warn!(%error, root = %root.display(), "shared FUSE detach failed");
                }
            }
        }
        detached
    }

    fn join_worker(&mut self) {
        if let Some(transport) = self.transport.take() {
            transport.stop();
        }
        if let Some(worker) = self.worker.take() {
            match worker.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(%error, "shared FUSE session ended with an error");
                }
                Err(_) => tracing::warn!("shared FUSE session panicked"),
            }
        }
    }
}

/// One view of a shared instance. Dropping it retires the view, and the last
/// view's drop tears the instance down.
pub(in crate::native_mount) struct SharedFuseView {
    // Field order: the view retires before its instance reference drops.
    pub(super) entry: SharedViewEntry,
    pub(super) _instance: Arc<SharedFuseInstance>,
}

impl SharedFuseInstance {
    fn start(registration: Registration<Self>) -> Result<Self, NativeMountError> {
        let root = registration.root();
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| NativeMountError::from(cause))?;
        // A fence whose lock is free belongs to a dead owner: detach its
        // mount first, so a crash never leaves a root mounted for good.
        let [fence, ..] = native_mount_control_paths(root)?;
        if fence.try_exists()? {
            detach_native_mount_destination_after_crash(root)?;
        }
        let metadata = std::fs::metadata(root)?;
        let path = std::ffi::CString::new(root.as_os_str().as_bytes())
            .map_err(|_| NativeMountError::InvalidDestination)?;
        let mut instance = SharedFuseRoot {
            views: SharedViews::new(
                std::os::unix::fs::MetadataExt::uid(&metadata),
                std::os::unix::fs::MetadataExt::gid(&metadata),
            ),
            root: root.to_path_buf(),
            path,
            state: InstanceState::default(),
            work,
            _registration: registration,
        };
        let root = &instance.root;
        // Every failure below drops `instance`, whose teardown undoes the
        // steps already taken.
        let views = Arc::clone(&instance.views);
        let work = instance.work.clone();
        let state = &mut instance.state;
        MountDestinationGuard::acquire_with_work(root, true, &work, &mut state.guard, false)?;
        super::session::map_read_buffers();
        let mut received = None;
        let opened = state.mount.open_into(
            root,
            true,
            || super::lifecycle::fuse_recovery_helper(&work),
            &mut received,
            state
                .guard
                .as_ref()
                .and_then(|guard| guard.file.as_ref())
                .ok_or(NativeMountError::ControlPending)?,
            &work,
        );
        state.device = received
            .map(|descriptor| {
                super::host_io::HostIo::new(std::fs::File::from(descriptor), &work).map(Arc::new)
            })
            .transpose()?;
        opened?;
        let descriptor = Arc::clone(
            state
                .device
                .as_ref()
                .ok_or(NativeMountError::FuseShutdownFailed)?,
        );
        let session = super::host_session::HostSession::new(
            super::packet_router::PacketRouter::Shared(Arc::clone(&views)),
            descriptor,
            &work,
        )?;
        session.initialize()?;
        let notifier = session.notifier();
        state.transport = Some(session.transport());
        views.set_notifier(notifier.clone());
        #[cfg(test)]
        let session = {
            let lifetime = Arc::new(());
            state.worker_lifetime = Arc::downgrade(&lifetime);
            let mut session = session;
            session._worker_lifetime = Some(lifetime);
            session
        };
        state.worker =
            Some(session.start(std::thread::Builder::new().name("acyclic-fuse".to_owned()))?);
        #[cfg(test)]
        let worker_lifetime = state.worker_lifetime.clone();
        #[cfg(test)]
        let exit_action_id = state.mount.exit_action_id();
        let invalidator = std::thread::Builder::new()
            .name("acyclic-fuse-invalidator".to_owned())
            .spawn(move || {
                run_shared_invalidator(&instance.views, &notifier);
                // Native stop, worker join, fence and registration disposal
                // stay on this physical owner, including after a panic.
                drop(instance);
            })?;
        Ok(Self {
            views,
            invalidator: Some(invalidator),
            #[cfg(test)]
            worker_lifetime,
            #[cfg(test)]
            exit_action_id,
        })
    }

    #[cfg(test)]
    pub(super) fn physical_owner(&self) -> (std::sync::Weak<()>, std::thread::ThreadId) {
        (
            self.worker_lifetime.clone(),
            self.invalidator
                .as_ref()
                .expect("live physical owner")
                .thread()
                .id(),
        )
    }
}

impl SharedFuseRoot {
    fn teardown(&mut self) {
        #[cfg(test)]
        let _ = self
            .views
            .retirement_thread
            .set(std::thread::current().id());
        self.views.close();
        let state = &mut self.state;
        let detached = state.detach(&self.path, &self.root);
        state.join_worker();
        state.device.take();
        // A failed ordinary detach must still finish the already admitted
        // action before FileLock's explicit unlock can admit a successor.
        if let Err(error) = state.mount.finish_exit_action() {
            tracing::warn!(%error, "shared FUSE owner-exit cleanup failed");
        }
        if let Some(mut guard) = state.guard.take() {
            if detached && let Err(error) = guard.release_cleanup(None) {
                tracing::warn!(%error, "shared FUSE fence release failed");
            }
            // An undetached root keeps its fence file; the next start for the
            // root detaches it.
            drop(guard);
        }
    }
}

impl Drop for SharedFuseRoot {
    fn drop(&mut self) {
        self.teardown();
    }
}

impl Drop for SharedFuseInstance {
    fn drop(&mut self) {
        self.views.stop_invalidator();
        if let Some(invalidator) = self.invalidator.take()
            && invalidator.join().is_err()
        {
            tracing::warn!("shared FUSE invalidator panicked");
        }
    }
}

impl SharedFuseView {
    #[cfg(test)]
    pub(super) fn exit_action_id(&self) -> Option<u32> {
        self._instance.exit_action_id
    }

    /// Lists a view named after `destination`'s last component in the shared
    /// instance at its parent, starting the instance if none is live.
    pub(super) fn start(
        destination: &Path,
        build: impl FnOnce(u64) -> Result<Arc<ProjectionCore>, NativeMountError>,
    ) -> Result<Self, NativeMountError> {
        let (Some(root), Some(name)) = (destination.parent(), destination.file_name()) else {
            return Err(NativeMountError::InvalidDestination);
        };
        let instance = INSTANCES.acquire(root, SharedFuseInstance::start)?;
        let entry = instance.views.insert(name.as_bytes(), build)?;
        Ok(Self {
            entry,
            _instance: instance,
        })
    }

    pub(super) fn core(&self) -> &Arc<ProjectionCore> {
        self.entry.core()
    }

    /// Retires the view; the instance stays up while other views hold it.
    pub(super) fn retire(&mut self) -> bool {
        self.entry.retire()
    }
}
