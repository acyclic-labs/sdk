use super::*;
use std::sync::Arc;

pub(crate) struct NativeCleanupWrite {
    pub(super) meter: NativeMaterializationWork,
    pub(super) additional_resource: Option<Arc<NativeResourceAllowance>>,
    pub(super) reservation: acyclic_native_runtime::NativeOperationReservation<
        Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    >,
}
impl NativeCleanupWrite {
    pub(crate) fn bind_current_work(
        &mut self,
        current: &NativeMaterializationWork,
    ) -> Result<(), NativeTreeMaterializationError> {
        if !self.meter.shares_ledger(current) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let Some(resource) = &current.original.resource else {
            return Ok(());
        };
        if self
            .additional_resource
            .as_ref()
            .is_some_and(|bound| Arc::ptr_eq(bound, resource))
        {
            return Ok(());
        }
        if self.additional_resource.is_some() {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        self.meter
            .state(|state| {
                state.check()?;
                resource.reserve(
                    NativeResourceCredit::WriteOperations.delta(1),
                    0,
                    self.meter.original.resource.as_deref(),
                    &mut || Ok(()),
                )
            })
            .map_err(native_materialization_work_error)?;
        self.additional_resource = Some(resource.clone());
        Ok(())
    }
    pub(crate) fn attempt<T>(
        mut self,
        operation: impl FnOnce() -> T,
    ) -> (T, Result<(), NativeTreeMaterializationError>) {
        let resource = self.additional_resource.take();
        let current = self
            .meter
            .state(|_| {
                resource.as_ref().map_or(Ok(()), |resource| {
                    resource.complete(
                        NativeResourceCredit::WriteOperations.delta(1),
                        NativeResourceCredit::WriteOperations.delta(1),
                        self.meter.original.resource.as_deref(),
                    )
                })
            })
            .map_err(native_materialization_work_error);
        let (result, original) = self.reservation.attempt(operation);
        (
            result,
            original
                .map_err(|error| self.meter.runtime_error(error))
                .and(current),
        )
    }
}
impl Drop for NativeCleanupWrite {
    fn drop(&mut self) {
        if let Some(resource) = self.additional_resource.take() {
            self.meter.state(|state| {
                if let Err(error) = resource.refund(
                    NativeResourceCredit::WriteOperations.delta(1),
                    self.meter.original.resource.as_deref(),
                ) {
                    state.record_native_io_failure(error);
                    state.poison_native_original_work();
                }
            });
        }
    }
}
impl std::fmt::Debug for NativeCleanupWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NativeCleanupWrite")
    }
}
