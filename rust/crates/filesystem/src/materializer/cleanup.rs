use super::*;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct NativeCleanupWrite {
    pub(super) meter: NativeMaterializationWork,
    pub(super) additional_resource: Option<Arc<NativeResourceAllowance>>,
    pub(super) reserved: bool,
}
impl NativeCleanupWrite {
    pub(crate) fn bind_current_work(
        &mut self,
        current: &NativeMaterializationWork,
    ) -> Result<(), NativeTreeMaterializationError> {
        if !self.reserved || !self.meter.shares_ledger(current) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let Some(resource) = &current.resource else {
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
                    self.meter.resource.as_deref(),
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
        self.reserved = false;
        let recorded = self
            .meter
            .state(|state| {
                let original = state.complete(NativeResourceCredit::WriteOperations.delta(1), NativeResourceCredit::WriteOperations.delta(1));
                let current = self
                    .additional_resource
                    .as_ref()
                    .map_or(Ok(()), |resource| {
                        resource.complete(
                            NativeResourceCredit::WriteOperations.delta(1),
                            NativeResourceCredit::WriteOperations.delta(1),
                            self.meter.resource.as_deref(),
                        )
                    });
                original.and(current)
            })
            .map_err(native_materialization_work_error);
        (operation(), recorded)
    }
}
impl Drop for NativeCleanupWrite {
    fn drop(&mut self) {
        if self.reserved {
            self.meter.state(|state| {
                let original = state.refund(NativeResourceCredit::WriteOperations.delta(1));
                let current = self
                    .additional_resource
                    .as_ref()
                    .map_or(Ok(()), |resource| {
                        resource.refund(
                            NativeResourceCredit::WriteOperations.delta(1),
                            self.meter.resource.as_deref(),
                        )
                    });
                if let Err(error) = original.and(current) {
                    state.record_native_io_failure(error);
                    state.poison_native_original_work();
                }
            });
        }
    }
}
