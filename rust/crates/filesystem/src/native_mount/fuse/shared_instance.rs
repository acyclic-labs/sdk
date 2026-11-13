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
    // The mounted root's device, which every view's root shares.
    device: u64,
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

    fn finish_detachment(&mut self) -> std::io::Result<bool> {
        let detached = if let Some(device) = self.device.as_ref() {
            device.stop();
            device.wait_detached(&mut self.mount)?
        } else {
            self.mount.observe_closed()?
        };
        if detached {
            self.device.take();
        }
        Ok(detached)
    }
}

/// One view of a shared instance. Dropping it retires the view, and the last
/// view's drop tears the instance down.
pub(in crate::native_mount) struct SharedFuseView {
    // Field order: the view retires before its instance reference drops.
    pub(super) entry: SharedViewEntry,
    pub(super) instance: Arc<SharedFuseInstance>,
}

/// Starts or joins `root`'s instance without a view of its own.
pub(in crate::native_mount) fn hold(
    root: &std::path::Path,
) -> Result<Arc<dyn std::any::Any + Send + Sync>, NativeMountError> {
    let operation = std::cell::Cell::new("acquire physical shared FUSE root");
    let instance: Arc<dyn std::any::Any + Send + Sync> = INSTANCES
        .acquire(root, |registration| {
            SharedFuseInstance::start(registration, &operation)
        })?;
    Ok(instance)
}

impl SharedFuseInstance {
    fn start(
        registration: Registration<Self>,
        operation: &std::cell::Cell<&'static str>,
    ) -> Result<Self, NativeMountError> {
        let root = registration.root();
        operation.set("admit shared FUSE root");
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| NativeMountError::from(cause))?;
        operation.set("recover disconnected shared FUSE root");
        super::super::recover_disconnected_native_mount_destination(root)
            .map_err(|error| error.during("probe/reclaim disconnected shared mountpoint", root))?;
        // A fence whose lock is free belongs to a dead owner: detach its
        // mount first, so a crash never leaves a root mounted for good.
        let [fence, ..] = native_mount_control_paths(root)?;
        if fence.try_exists()? {
            detach_native_mount_destination_after_crash(root)?;
        }
        operation.set("stat shared FUSE root before mount");
        let metadata = std::fs::metadata(root).map_err(|error| {
            NativeMountError::host_operation("stat shared mountpoint before mount", root, error)
        })?;
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
        operation.set("acquire shared FUSE destination fence");
        MountDestinationGuard::acquire_with_work(root, true, &work, &mut state.guard, false)?;
        super::session::map_read_buffers();
        let mut received = None;
        operation.set("open and mount shared FUSE device");
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
            .map(|descriptor| super::host_io::HostIo::host(descriptor, &work).map(Arc::new))
            .transpose()?;
        opened?;
        let descriptor = Arc::clone(
            state
                .device
                .as_ref()
                .ok_or(NativeMountError::FuseShutdownFailed)?,
        );
        operation.set("prepare shared FUSE device transport");
        let session = super::host_session::HostSession::new(
            super::packet_router::PacketRouter::Shared(Arc::clone(&views)),
            descriptor,
            &work,
        )
        .map_err(|error| error.during("prepare shared FUSE device transport", root))?;
        let notifier = session.notifier();
        state.transport = Some(session.transport());
        views.set_notifier(notifier.clone());
        #[cfg(test)]
        let worker_lifetime = session.worker_lifetime();
        session.start_mount(
            root,
            std::thread::Builder::new().name("acyclic-fuse".to_owned()),
            &mut state.worker,
            operation,
        )?;
        // The router answers the root itself, from no view's state, so this
        // one query cannot wait on any view's owner.
        operation.set("stat shared FUSE root after INIT");
        let device =
            std::os::unix::fs::MetadataExt::dev(&std::fs::metadata(root).map_err(|error| {
                NativeMountError::host_operation(
                    "stat shared mountpoint after FUSE INIT",
                    root,
                    error,
                )
            })?);
        #[cfg(test)]
        let exit_action_id = state.mount.exit_action_id();
        operation.set("spawn shared FUSE retirement owner");
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
        let namespace_detached = state.detach(&self.path, &self.root);
        state.join_worker();
        // Namespace removal is not a physical receipt. Keep the original
        // transport/custody on refusal, and leave its fence for recovery.
        let mut detached = match state.finish_detachment() {
            Ok(physical_detached) => namespace_detached && physical_detached,
            Err(error) => {
                tracing::warn!(%error, "shared FUSE physical retirement refused");
                false
            }
        };
        // A failed ordinary detach must still finish the already admitted
        // action before FileLock's explicit unlock can admit a successor.
        if let Err(error) = state.mount.finish_exit_action() {
            tracing::warn!(%error, "shared FUSE owner-exit cleanup failed");
            detached = false;
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
        self.instance.exit_action_id
    }

    /// Lists a view named after `destination`'s last component in the shared
    /// instance at its parent, starting the instance if none is live.
    pub(super) fn start(
        destination: &Path,
        operation: &std::cell::Cell<&'static str>,
        build: impl FnOnce(u64, (u32, u32)) -> Result<Arc<ProjectionCore>, NativeMountError>,
    ) -> Result<Self, NativeMountError> {
        let (Some(root), Some(name)) = (destination.parent(), destination.file_name()) else {
            return Err(NativeMountError::InvalidDestination);
        };
        operation.set("acquire physical shared FUSE root");
        let instance = INSTANCES.acquire(root, |registration| {
            SharedFuseInstance::start(registration, operation)
        })?;
        let owner = (instance.views.root_attr.uid, instance.views.root_attr.gid);
        operation.set("insert shared FUSE view");
        let entry = instance
            .views
            .insert(name.as_bytes(), |base| build(base, owner))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    #[test]
    fn missing_physical_receipt_refuses_shared_detachment() -> Result<(), Box<dyn std::error::Error>>
    {
        if acyclic_native_runtime::private_file::with_isolated_user_runtime(
            "native_mount::fuse::shared_instance::tests::missing_physical_receipt_refuses_shared_detachment",
        )? {
            return Ok(());
        }
        let work = super::super::tests::fuse_fixture_work();
        let funded = super::super::host_io::fund_device(&work)?;
        let held_bytes = acyclic_native_runtime::PacketDevice::storage_bytes(funded.capacity)?;
        let file =
            rustix::fs::memfd_create(c"shared-retirement-proof", rustix::fs::MemfdFlags::CLOEXEC)?;
        let device = acyclic_native_runtime::PacketDevice::prepare(
            std::fs::File::from(file),
            funded.capacity,
        )?;
        let io = Arc::new(super::super::host_io::HostIo::host(
            super::super::mount::Device::Hosted {
                device,
                allocation: funded.allocation,
                maximum_write: funded.maximum_write,
            },
            &work,
        )?);
        let host = io.host_id().ok_or("host absent")?;
        // Bind the original live host's lifetime before signalling it. This
        // isolated namespace has no other owner's host to disrupt.
        let pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, host, 0) };
        if pidfd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let pidfd = unsafe { OwnedFd::from_raw_fd(i32::try_from(pidfd)?) };
        if unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                pidfd.as_raw_fd(),
                libc::SIGKILL,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        } < 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        let mut state = InstanceState {
            device: Some(Arc::clone(&io)),
            ..InstanceState::default()
        };
        // The former namespace-only observation succeeds, while the real
        // host has died without publishing its physical closure receipt.
        assert!(state.mount.observe_closed()?);
        assert!(state.finish_detachment().is_err());
        assert!(
            state.device.is_some(),
            "a refused retirement must retain original custody"
        );
        assert!(
            work.live_allocation_bytes() >= held_bytes,
            "missing receipt must retain the original external storage credit"
        );
        Ok(())
    }
}
