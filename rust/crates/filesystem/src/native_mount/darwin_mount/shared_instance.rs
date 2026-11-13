//! One physical NFS owner per root; views own only their shared-core lease.
use super::*;
use crate::native_mount::shared::instances::{Instances, Registration};
use crate::native_mount::{MountDestinationGuard, NativeMountRetirement};

pub(super) type RootRegistration = Registration<SharedDarwinInstance>;

static INSTANCES: Instances<SharedDarwinInstance> = Instances::new();

pub(super) struct SharedDarwinInstance {
    views: Arc<super::shared::SharedViews>,
    physical: Mutex<DarwinMountSession>,
    identity: crate::NativeRootIdentity,
}

pub(super) struct SharedDarwinView {
    entry: Option<super::shared::SharedView>,
    pub(super) context: Arc<DarwinMountContext>,
    retirement: Option<NativeMountRetirement>,
    // Retain the root through callback drain and source retirement.
    _instance: Arc<SharedDarwinInstance>,
}

pub(in crate::native_mount) fn hold(
    root: &Path,
) -> Result<Arc<dyn std::any::Any + Send + Sync>, NativeMountError> {
    Ok(INSTANCES.acquire(root, SharedDarwinInstance::start)?)
}

impl SharedDarwinInstance {
    fn start(registration: Registration<Self>) -> Result<Self, NativeMountError> {
        let root = registration.root().to_path_buf();
        let work = Work::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .map_err(|(cause, _)| NativeMountError::from(cause))?;
        let [fence, ..] = crate::native_mount::native_mount_control_paths(&root)?;
        if fence.try_exists()? {
            crate::native_mount::detach_native_mount_destination_after_crash(&root)?;
        }
        let views = super::shared::SharedViews::new(&root, &work)?;
        let resources = DriverSessionResources::new_target(
            CallbackTarget::Shared(Arc::clone(&views)),
            &root,
            &work,
            true,
            Some(registration),
        )?;
        let mut packet = Some(NativeMountRetirement {
            retained_source: None,
            destination_guard: None,
            terminal_source_failure: None,
        });
        let mut physical = None;
        DarwinMountSession::start_resources(
            resources,
            Arc::clone(&views.handler_failure),
            &work,
            &mut physical,
            &mut packet,
            |packet| {
                MountDestinationGuard::acquire_with_work(
                    &root,
                    true,
                    &work,
                    &mut packet.destination_guard,
                    false,
                )
            },
        )?;
        let physical = physical.ok_or(NativeMountError::ControlPending)?;
        let identity = physical
            .native_root_identity()
            .ok_or(NativeMountError::ControlPending)?;
        Ok(Self {
            views,
            identity,
            physical: Mutex::new(physical),
        })
    }
}

impl Drop for SharedDarwinInstance {
    fn drop(&mut self) {
        let physical = self
            .physical
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner);
        if let Err(error) = physical.stop(&crate::native_mount::NativeSessionStopWork::AcceptedDrop)
        {
            // The accepted physical loop still retains its fence, registration
            // and native resources; session drop wakes that same owner.
            tracing::warn!(%error, "shared Darwin root retirement failed");
        }
    }
}

impl SharedDarwinView {
    pub(super) fn start_into(
        request: &NativeMountRequest,
        source: Arc<dyn MountFilesystem>,
        work: &Work,
        out: &mut Option<DarwinMountSession>,
        retirement: &mut Option<NativeMountRetirement>,
    ) -> Result<(), NativeMountError> {
        if !crate::native_mount::shared::claim_issued_destination(&request.destination) {
            return Err(NativeMountError::InvalidDestination);
        }
        let root = request
            .destination
            .parent()
            .ok_or(NativeMountError::InvalidDestination)?;
        let name = request
            .destination
            .file_name()
            .ok_or(NativeMountError::InvalidDestination)?;
        let instance = INSTANCES.acquire(root, SharedDarwinInstance::start)?;
        let entry =
            instance
                .views
                .insert(name.as_bytes().to_vec(), source, request.writable, work)?;
        let context = Arc::clone(&entry.context);
        let _active = NativeMountWorkScope::new(&context, work)?;
        let unmount = Arc::clone(
            &instance
                .physical
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .unmount,
        );
        *out = Some(DarwinMountSession {
            thread: None,
            loop_finished_before_teardown: None,
            loop_result: None,
            handler_failure: Arc::clone(&context.handler_failure),
            unmount,
            native_root_identity: Some(crate::NativeRootIdentity {
                device: instance.identity.device,
                object: context.root_inode,
            }),
            shared: Some(Self {
                entry: Some(entry),
                context,
                retirement: retirement.take(),
                _instance: instance,
            }),
        });
        Ok(())
    }

    pub(super) fn is_listed(&self) -> bool {
        self.entry.is_some()
    }

    pub(super) fn retirement_ownership(&self) -> (bool, bool) {
        self.retirement.as_ref().map_or((false, false), |packet| {
            (
                packet.destination_guard.is_some(),
                packet.retained_source.is_some(),
            )
        })
    }

    pub(super) fn stop(
        &mut self,
        wait: Option<&crate::CancellationToken>,
    ) -> Result<crate::native_mount::retirement::TerminalFailures, NativeMountError> {
        // Drop's one shared-core operation unlists, drains leases and forgets
        // this view's handles before its original producer can retire.
        drop(self.entry.take());
        let mut failure = crate::native_mount::retirement::TerminalFailures::default();
        if let Some(packet) = self.retirement.as_mut() {
            packet.retire_physical(
                wait,
                crate::native_mount::retirement::SourceRetirementWait::Caller,
            )?;
            if !packet.is_retired() {
                return Err(NativeMountError::ControlPending);
            }
            failure.source = packet.terminal_source_failure.take();
        }
        self.retirement = None;
        Ok(failure)
    }
}
