//! The FUSE adapter's instance: one session per shared root directory.
//!
//! The shared core's [`Instances`] registry starts it for a root's first
//! view and drops it after the last; its drop unmounts the root, stops its
//! threads and releases its fence. A failed start unwinds through the same
//! teardown. A process that dies with live views leaves its root fenced by
//! a lock nobody holds; the next start for that root detaches the dead
//! owner's mount before mounting again.

use super::shared::{SharedViewEntry, SharedViews, run_shared_invalidator};
use super::*;
use crate::NativeMaterializationWork;
use crate::native_mount::shared::instances::{Instance, Instances};
use crate::native_mount::{
    MountDestinationGuard, detach_native_mount_destination_after_crash, native_mount_control_paths,
};
use std::path::PathBuf;

static INSTANCES: Instances<SharedFuseInstance> = Instances::new();

#[cfg(test)]
thread_local! {
    /// Negative control: starts without reaping a crash-left root.
    pub(super) static SKIP_REAP: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(in crate::native_mount) struct SharedFuseInstance {
    pub(super) views: Arc<SharedViews>,
    root: PathBuf,
    path: Vec<u8>,
    state: Mutex<InstanceState>,
    // The instance's own meter: it outlives the view that started it.
    work: NativeMaterializationWork,
}

#[derive(Default)]
struct InstanceState {
    mount: super::mount::Mount,
    device: Option<Arc<std::fs::File>>,
    worker: Option<std::thread::JoinHandle<Result<(), NativeMountError>>>,
    invalidator: Option<std::thread::JoinHandle<()>>,
    guard: Option<MountDestinationGuard>,
}

impl InstanceState {
    fn detach(&mut self, path: &std::ffi::CStr, root: &Path) -> bool {
        let mut detached = !self.mount.pending();
        if !detached {
            let device = self.device.as_ref().map(std::os::fd::AsFd::as_fd);
            match self.mount.stop(path, device) {
                Ok(()) => {
                    detached = device
                        .is_none_or(|device| self.mount.device_detached(device).unwrap_or(false))
                        || !self.mount.pending();
                }
                Err(error) => {
                    tracing::warn!(%error, root = %root.display(), "shared FUSE unmount failed");
                }
            }
        }
        if !detached {
            // Busy: detach the namespace now; the kernel ends the connection,
            // and with it the worker, once the last open file closes.
            match nix::mount::umount2(path, nix::mount::MntFlags::MNT_DETACH) {
                Ok(()) => detached = true,
                Err(error) => {
                    tracing::warn!(%error, root = %root.display(), "shared FUSE detach failed");
                }
            }
            drop(self.worker.take());
        }
        detached
    }

    fn join_worker(&mut self) {
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
    pub(super) _instance: Arc<Instance<SharedFuseInstance>>,
}

impl SharedFuseInstance {
    fn start(root: &Path) -> Result<Self, NativeMountError> {
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| NativeMountError::from(cause))?;
        // A fence whose lock is free belongs to a dead owner: detach its
        // mount first, so a crash never leaves a root mounted for good.
        let [fence, ..] = native_mount_control_paths(root)?;
        #[cfg(test)]
        let reap = !SKIP_REAP.with(std::cell::Cell::get);
        #[cfg(not(test))]
        let reap = true;
        if reap && fence.try_exists()? {
            detach_native_mount_destination_after_crash(root)?;
        }
        let metadata = std::fs::metadata(root)?;
        let mut path = root.as_os_str().as_bytes().to_vec();
        path.push(0);
        let mut instance = Self {
            views: SharedViews::new(
                std::os::unix::fs::MetadataExt::uid(&metadata),
                std::os::unix::fs::MetadataExt::gid(&metadata),
            ),
            root: root.to_path_buf(),
            path,
            state: Mutex::new(InstanceState::default()),
            work,
        };
        // Every failure below drops `instance`, whose teardown undoes the
        // steps already taken.
        let views = Arc::clone(&instance.views);
        let work = instance.work.clone();
        let state = instance
            .state
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner);
        MountDestinationGuard::acquire_with_work(root, true, &work, &mut state.guard, false)?;
        super::session::map_read_buffers();
        let mut received = None;
        let opened = state.mount.open_into(
            root,
            true,
            || super::lifecycle::fuse_recovery_helper(&work),
            &mut received,
        );
        state.device = received.map(|descriptor| Arc::new(std::fs::File::from(descriptor)));
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
        views.set_notifier(notifier.clone());
        state.worker = Some(
            std::thread::Builder::new()
                .name("acyclic-fuse".to_owned())
                .spawn(move || session.run())?,
        );
        state.invalidator = Some(
            std::thread::Builder::new()
                .name("acyclic-fuse-invalidator".to_owned())
                .spawn(move || run_shared_invalidator(&views, &notifier))?,
        );
        Ok(instance)
    }

    fn teardown(&mut self) {
        self.views.close();
        let state = self.state.get_mut().unwrap_or_else(PoisonError::into_inner);
        let Ok(path) = std::ffi::CStr::from_bytes_with_nul(&self.path) else {
            return;
        };
        let detached = state.detach(path, &self.root);
        state.join_worker();
        self.views.stop_invalidator();
        if let Some(invalidator) = state.invalidator.take()
            && invalidator.join().is_err()
        {
            tracing::warn!("shared FUSE invalidator panicked");
        }
        state.device.take();
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

impl Drop for SharedFuseInstance {
    fn drop(&mut self) {
        self.teardown();
    }
}

impl SharedFuseView {
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
