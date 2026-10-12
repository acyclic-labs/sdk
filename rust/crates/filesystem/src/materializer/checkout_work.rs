use super::*;
use std::sync::Arc;

impl NativeMaterializationWork {
    pub(crate) fn owns_native_checkout_token(
        &self,
        token: &Arc<crate::kernel::ProofDataCustody>,
    ) -> bool {
        self.state(|state| state.owns_native_checkout_token(token))
    }
    pub(crate) fn checkout_control_sink(
        &self,
        producer: &Arc<crate::kernel::ProofDataCustody>,
    ) -> Result<crate::demand::NativeCheckoutControlWitness, NativeTreeMaterializationError> {
        self.state
            .checkout_control_witness(producer)
            .ok_or(NativeTreeMaterializationError::InvalidLayout)
    }
    pub(crate) fn register_native_checkout_control(
        &self,
        producer: &Arc<crate::kernel::ProofDataCustody>,
        header: &mut NativeAllocation,
    ) -> Result<crate::demand::NativeCheckoutControlWitness, NativeTreeMaterializationError> {
        self.state
            .register_native_checkout_control(self, producer, header)
    }
    pub(crate) fn transfer_capture_allocation(
        &self,
        bytes: u64,
        custody: &Arc<crate::demand::CaptureTransientCustody>,
    ) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        if !custody.owns_native_work(self) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        custody.transfer_retained_bytes(bytes)?;
        Ok(NativeAllocation {
            meter: self.allocation_work(),
            bytes,
            capture: None,
        })
    }
    pub(crate) fn allocations<T, E: From<NativeTreeMaterializationError>>(
        &self,
        operation: impl FnOnce(
            &mut crate::kernel::allocation::AllocationLedger,
            &mut crate::WorkCounters,
            crate::WorkBudget,
            &crate::CancellationToken,
        ) -> Result<T, E>,
    ) -> Result<(T, NativeAllocation), E> {
        let (result, bytes, cleanup) = self
            .state(|state| {
                state
                    .reconcile_native_controls()
                    .map_err(NativeTreeMaterializationError::Work)?;
                let (allocations, counters, budget, cancellation) = state.allocation_parts();
                let before = allocations.live_bytes();
                let result = operation(allocations, counters, budget, cancellation);
                let bytes = allocations.live_bytes().checked_sub(before);
                let cleanup = bytes
                    .filter(|_| result.is_err())
                    .map(|bytes| allocations.release(bytes));
                Ok::<_, NativeTreeMaterializationError>((result, bytes, cleanup))
            })
            .map_err(E::from)?;
        let bytes = bytes.ok_or_else(|| {
            E::from(NativeTreeMaterializationError::Work(
                crate::WorkError::Overflow,
            ))
        })?;
        if let Some(cleanup) = cleanup {
            cleanup.map_err(|error| {
                E::from(error.into_work_or(NativeTreeMaterializationError::Work(
                    crate::WorkError::Overflow,
                )))
            })?;
        }
        result.map(|value| {
            (
                value,
                NativeAllocation {
                    meter: self.allocation_work(),
                    bytes,
                    capture: None,
                },
            )
        })
    }
}
