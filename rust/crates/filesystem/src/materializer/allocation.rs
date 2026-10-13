use super::*;
use std::sync::Arc;

/// Capacity retained on the original SDK demand ledger.
pub struct NativeAllocation {
    pub(super) meter: NativeMaterializationWork,
    pub(super) bytes: u64,
}
impl std::fmt::Debug for NativeAllocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeAllocation")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}
impl NativeAllocation {
    /// Retained capacity.
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }
    /// Original accounting context.
    pub const fn work(&self) -> &NativeMaterializationWork {
        &self.meter
    }
    /// Whether allocations share a ledger.
    pub fn shares_ledger(&self, other: &Self) -> bool {
        self.meter.shares_ledger(&other.meter)
    }
    /// Takes paid runtime capacity without allocating again.
    pub fn from_runtime_allocation(
        work: &NativeMaterializationWork,
        allocation: acyclic_native_runtime::ReadAllocation,
    ) -> Result<
        Self,
        (
            NativeTreeMaterializationError,
            acyclic_native_runtime::ReadAllocation,
        ),
    > {
        let meter = work.runtime_meter();
        let owner = meter.allocation_owner().unwrap_or(meter);
        allocation
            .consume_same_meter_owned_bytes(&owner)
            .map(|bytes| Self {
                meter: work.allocation_work(),
                bytes,
            })
            .map_err(|allocation| (NativeTreeMaterializationError::InvalidLayout, allocation))
    }
    /// Combines capacity with identical accounting attribution.
    pub fn absorb(&mut self, other: &mut Self) -> Result<(), NativeTreeMaterializationError> {
        if !self.meter.shares_allocation_context(&other.meter) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let bytes = self
            .bytes
            .checked_add(other.bytes)
            .ok_or(crate::WorkError::Overflow)?;
        self.bytes = bytes;
        other.bytes = 0;
        Ok(())
    }
    /// Splits admitted capacity.
    pub fn split_off(&mut self, bytes: u64) -> Result<Self, NativeTreeMaterializationError> {
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        Ok(Self {
            meter: self.meter.clone(),
            bytes,
        })
    }
    /// Admits physical growth before allocation.
    pub fn grow(&mut self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        self.bytes
            .checked_add(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        let mut growth =
            self.meter
                .claim(bytes, u64::from(bytes != 0), crate::WorkCounters::UNCHARGED)?;
        self.absorb(&mut growth)
    }
    /// Releases destroyed backing capacity.
    pub fn release(&mut self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        self.meter
            .state(|state| state.release(bytes))
            .map_err(native_materialization_work_error)
    }
    pub(crate) fn release_deferred(&mut self, bytes: u64) {
        if let Err(error) = self.release(bytes) {
            self.meter.poison(error);
        }
    }
    /// Changes logical retained capacity without a physical allocation.
    pub fn resize(&mut self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        if bytes <= self.bytes {
            return self.release(self.bytes - bytes);
        }
        self.meter
            .state(|state| state.retain_native_owned(bytes - self.bytes, &None))
            .map_err(native_materialization_work_error)?;
        self.bytes = bytes;
        Ok(())
    }
    /// Records actual allocator excess before returning a limit failure.
    pub fn reconcile_capacity(&mut self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        if bytes < self.bytes {
            return self.release(self.bytes - bytes);
        }
        let (mut growth, recorded) = self.meter.record_completed_allocation(bytes - self.bytes);
        self.absorb(&mut growth)?;
        recorded
    }
    fn root_metadata_for(&self, meter: &Arc<crate::demand::NativeDemandReadMeter>) -> bool {
        Arc::ptr_eq(&self.meter.state, meter) && self.meter.resource.is_none()
    }
    pub(crate) fn is_checkout_header_for(
        &self,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> bool {
        self.root_metadata_for(meter)
            && self.bytes == std::mem::size_of::<crate::kernel::ProofDataCustody>() as u64
    }
    pub(crate) fn transfer_checkout_metadata(
        &mut self,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
        expected: u64,
    ) -> Result<u64, NativeTreeMaterializationError> {
        if !self.root_metadata_for(meter) || self.bytes != expected {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        Ok(std::mem::take(&mut self.bytes))
    }
    pub(crate) fn transfer_resource_retirement(
        &mut self,
        bytes: u64,
    ) -> Result<NativeResourceRetirement, NativeTreeMaterializationError> {
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        Ok(NativeResourceRetirement {
            original: self.meter.state.clone(),
            resource: self.meter.resource.clone(),
            bytes,
        })
    }
    pub(crate) fn transfer_checkout_control(
        &mut self,
        owner: &Arc<crate::kernel::ProofDataCustody>,
        scope: &std::sync::Arc<crate::kernel::ProofDataCustody>,
    ) -> Result<crate::kernel::ProofDataClaim, NativeTreeMaterializationError> {
        if !std::sync::Arc::ptr_eq(scope, owner) || !owner.is_native_original(&self.meter.state) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        Ok(crate::kernel::ProofDataClaim::native_control(
            owner,
            self.transfer_resource_retirement(self.bytes)?,
        ))
    }
    pub(crate) fn transfer_checkout_data_bytes(
        &mut self,
        bytes: u64,
        owner: &Arc<crate::kernel::ProofDataCustody>,
        scope: &std::sync::Arc<crate::kernel::ProofDataCustody>,
    ) -> Result<crate::kernel::ProofDataClaim, NativeTreeMaterializationError> {
        if !std::sync::Arc::ptr_eq(scope, owner) || !owner.is_native_original(&self.meter.state) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        Ok(crate::kernel::ProofDataClaim::native_data(
            owner,
            self.transfer_resource_retirement(bytes)?,
        ))
    }
}
impl Drop for NativeAllocation {
    fn drop(&mut self) {
        self.release_deferred(self.bytes);
    }
}
pub(crate) struct NativeResourceRetirement {
    original: Arc<crate::demand::NativeDemandReadMeter>,
    resource: Option<Arc<NativeResourceAllowance>>,
    bytes: u64,
}
impl std::fmt::Debug for NativeResourceRetirement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeResourceRetirement")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}
impl NativeResourceRetirement {
    pub(crate) const fn bytes(&self) -> u64 {
        self.bytes
    }
    pub(crate) fn original_meter(&self) -> &Arc<crate::demand::NativeDemandReadMeter> {
        &self.original
    }
}
impl Drop for NativeResourceRetirement {
    fn drop(&mut self) {
        if let Some(resource) = &self.resource {
            if let Err(error) = resource.retire(self.bytes) {
                let mut state = self.original.lock();
                state.record_native_io_failure(error);
                state.poison_native_original_work();
            }
        }
    }
}
