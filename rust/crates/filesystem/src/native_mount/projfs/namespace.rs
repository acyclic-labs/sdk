use super::*;

#[derive(Clone, Copy)]
pub(in crate::native_mount) struct ProjFsNamespaceQualification {
    pub(in crate::native_mount) root_identity: crate::NativeRootIdentity,
    pub(in crate::native_mount) instance_id: GUID,
}

pub(in crate::native_mount) struct ProjFsNamespaceOutcome {
    pub(in crate::native_mount) capture_failure: Option<NativeMountError>,
    pub(in crate::native_mount) cleanup_failure: Option<NativeMountError>,
    pub(in crate::native_mount) qualification: Option<ProjFsNamespaceQualification>,
    pub(in crate::native_mount) state: ProjectorState,
    pub(super) callbacks: Arc<Mutex<PostOperationFailures>>,
}

impl ProjFsNamespaceOutcome {
    pub(in crate::native_mount) fn qualification(&self) -> Option<ProjFsNamespaceQualification> {
        self.qualification
    }

    pub(in crate::native_mount) fn continuation_ready(&self) -> bool {
        let callbacks = lock_recover(&self.callbacks);
        self.qualification.is_some()
            && self.capture_failure.is_none()
            && self.cleanup_failure.is_none()
            && self.state.failure.is_none()
            && self.state.pending.is_empty()
            && callbacks.pending_captures.is_empty()
            && callbacks.unreplayable.is_none()
    }

    pub(in crate::native_mount) fn take_capture_failures(
        &mut self,
    ) -> Option<(NativeMountError, Option<NativeMountError>)> {
        match self.capture_failure.take() {
            Some(cause) => Some((cause, self.cleanup_failure.take())),
            None => self.cleanup_failure.take().map(|cause| (cause, None)),
        }
    }
}
