use super::*;
use std::sync::{Arc, Weak};

pub(crate) struct NativeInvocationView {
    pub(super) cancellation: crate::CancellationToken,
    pub(super) self_ref: Weak<Self>,
    pub(super) metadata: NativeAllocation,
}
impl NativeInvocationView {
    pub(crate) fn check(&self) -> Result<(), crate::DemandError> {
        self.cancellation
            .check()
            .map_err(|_| crate::DemandError::Cancelled)
    }
    pub(crate) fn cancellation(&self) -> &crate::CancellationToken {
        &self.cancellation
    }
    pub(crate) fn owns_resource(&self, resource: Option<&Arc<NativeResourceAllowance>>) -> bool {
        match (&self.metadata.work().resource, resource) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        }
    }
    pub(super) fn with_original_leaf<T>(
        &self,
        operation: impl FnOnce(&mut crate::demand::DemandWork) -> T,
    ) -> T {
        self.metadata
            .work()
            .state(|state| state.with_invocation_scope(Some(self.self_ref.clone()), operation))
    }
}
#[cfg(feature = "native-mount")]
pub(crate) struct NativeSourceExecutionView {
    pub(super) lease: crate::native_mount::NativeSourceIoLease,
    pub(super) metadata: NativeAllocation,
}
#[cfg(feature = "native-mount")]
impl NativeSourceExecutionView {
    pub(super) fn with_original_leaf<T>(
        &self,
        operation: impl FnOnce(&mut crate::demand::DemandWork) -> T,
    ) -> T {
        self.metadata.work().with_original_leaf(operation)
    }
}
