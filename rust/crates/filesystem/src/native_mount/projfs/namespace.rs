use super::*;

#[derive(Clone, Copy)]
pub(in crate::native_mount) struct ProjFsNamespaceQualification {
    pub(in crate::native_mount) root_identity: crate::NativeRootIdentity,
    pub(in crate::native_mount) instance_id: GUID,
}

pub(in crate::native_mount) struct ProjFsNamespaceOutcome {
    pub(in crate::native_mount) failures: crate::native_mount::retirement::TerminalFailures,
    pub(super) _state: ProjectorState,
    pub(super) callbacks: Arc<Mutex<PostOperationFailures>>,
}
