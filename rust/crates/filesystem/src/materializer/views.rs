use super::*;
use std::sync::{Arc, Weak};

pub(crate) struct NativeInvocationView {
    pub(super) cancellation: crate::CancellationToken,
    pub(super) self_ref: Weak<Self>,
    pub(super) _metadata: NativeAllocation,
}
impl std::fmt::Debug for NativeInvocationView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeInvocationView")
            .field("cancelled", &self.cancellation.is_cancelled())
            .finish_non_exhaustive()
    }
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
        match (&self._metadata.work().resource, resource) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        }
    }
    pub(super) fn with_original_leaf<T>(
        &self,
        operation: impl FnOnce(&mut crate::demand::DemandWork) -> T,
    ) -> T {
        self._metadata
            .work()
            .state(|demand| demand.with_invocation_scope(Some(self.self_ref.clone()), operation))
    }
}
impl_original_native_read_meter!(NativeInvocationView);

#[cfg(feature = "native-mount")]
pub(crate) struct NativeSourceExecutionView {
    pub(super) _lease: crate::native_mount::NativeSourceIoLease,
    pub(super) _metadata: NativeAllocation,
}
#[cfg(feature = "native-mount")]
impl std::fmt::Debug for NativeSourceExecutionView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeSourceExecutionView")
            .finish_non_exhaustive()
    }
}
#[cfg(feature = "native-mount")]
impl NativeSourceExecutionView {
    pub(super) fn with_original_leaf<T>(
        &self,
        operation: impl FnOnce(&mut crate::demand::DemandWork) -> T,
    ) -> T {
        self._metadata.work().with_original_leaf(operation)
    }
}
#[cfg(feature = "native-mount")]
impl_original_native_read_meter!(
    NativeSourceExecutionView,
    |view: &NativeSourceExecutionView| Some(view._metadata.work().runtime_meter())
);
