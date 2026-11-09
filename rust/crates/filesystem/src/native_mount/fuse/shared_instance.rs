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
use crate::native_mount::shared::instances::{Instances, Registration};
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
    // The mounted root's device, which every view's root shares.
    device: u64,
    invalidator: Option<std::thread::JoinHandle<()>>,
    #[cfg(test)]
    worker_lifetime: std::sync::Weak<()>,
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
    device: Option<Arc<std::fs::File>>,
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
            let device = self.device.as_ref().map(std::os::fd::AsFd::as_fd);
            match self.mount.stop(path, device) {
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
                Ok(()) => detached = true,
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
    pub(super) instance: Arc<SharedFuseInstance>,
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
        #[cfg(test)]
        let reap = !SKIP_REAP.with(std::cell::Cell::get);
        #[cfg(not(test))]
        let reap = true;
        if reap && fence.try_exists()? {
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
        state.transport = Some(session.transport());
        views.set_notifier(notifier.clone());
        #[cfg(test)]
        let lifetime = Arc::new(());
        #[cfg(test)]
        {
            state.worker_lifetime = Arc::downgrade(&lifetime);
        }
        state.worker = Some(
            std::thread::Builder::new()
                .name("acyclic-fuse".to_owned())
                .spawn(move || {
                    let result = session.run();
                    #[cfg(test)]
                    drop(lifetime);
                    result
                })?,
        );
        #[cfg(test)]
        let worker_lifetime = state.worker_lifetime.clone();
        // The router answers the root itself, from no view's state, so this
        // one query cannot wait on any view's owner.
        let device = std::os::unix::fs::MetadataExt::dev(&std::fs::metadata(root)?);
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
            device,
            invalidator: Some(invalidator),
            #[cfg(test)]
            worker_lifetime,
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
        Ok(Self { entry, instance })
    }

    pub(super) fn core(&self) -> &Arc<ProjectionCore> {
        self.entry.core()
    }

    /// The identity the kernel reports for this view's root, known without
    /// asking the kernel: a request into the view could wait on the very
    /// owner that serves it.
    pub(super) fn root_identity(&self) -> crate::NativeRootIdentity {
        crate::NativeRootIdentity {
            device: self.instance.device,
            object: self.entry.root_inode(),
        }
    }

    /// Retires the view; the instance stays up while other views hold it.
    pub(super) fn retire(&mut self) -> bool {
        self.entry.retire()
    }
}
