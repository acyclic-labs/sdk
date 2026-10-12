use super::*;
use std::sync::Arc;

/// Bytes retained on the original demand ledger.
pub struct NativeAllocation {
    pub(super) meter: NativeMaterializationWork,
    pub(super) bytes: u64,
    pub(super) capture: Option<Arc<crate::demand::CaptureTransientCustody>>,
}

impl std::fmt::Debug for NativeAllocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeAllocation")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl NativeAllocation {
    /// Current retained capacity.
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }
    /// Original accounting context.
    pub const fn work(&self) -> &NativeMaterializationWork {
        &self.meter
    }
    /// Whether both allocations use the same ledger.
    pub fn shares_ledger(&self, other: &Self) -> bool {
        self.meter.shares_ledger(&other.meter)
    }
    /// Transfers an already admitted runtime allocation.
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
                capture: None,
            })
            .map_err(|allocation| (NativeTreeMaterializationError::InvalidLayout, allocation))
    }
    /// Combines capacities with identical ledger and capture attribution.
    pub fn absorb(&mut self, other: &mut Self) -> Result<(), NativeTreeMaterializationError> {
        let captures_match = match (&self.capture, &other.capture) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        };
        if !self.meter.shares_allocation_context(&other.meter)
            || !captures_match && self.bytes != 0 && other.bytes != 0
        {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let bytes = self
            .bytes
            .checked_add(other.bytes)
            .ok_or(crate::WorkError::Overflow)?;
        if self.bytes == 0 {
            self.capture = other.capture.clone();
        }
        self.bytes = bytes;
        other.bytes = 0;
        Ok(())
    }
    /// Splits paid capacity without allocating again.
    pub fn split_off(&mut self, bytes: u64) -> Result<Self, NativeTreeMaterializationError> {
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        Ok(Self {
            meter: self.meter.clone(),
            bytes,
            capture: self.capture.clone(),
        })
    }
    /// Admits a physical buffer growth before attempting it.
    pub fn grow(&mut self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        self.bytes
            .checked_add(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        let mut added =
            self.meter
                .claim(bytes, u64::from(bytes != 0), crate::WorkCounters::UNCHARGED)?;
        added.capture = self.capture.clone();
        self.absorb(&mut added)
    }
    /// Releases capacity after destroying its backing storage.
    pub fn release(&mut self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        self.meter
            .state(|state| state.release_native_owned(bytes, &None))
            .map_err(native_materialization_work_error)
    }
    pub(crate) fn release_deferred(&mut self, bytes: u64) {
        if let Err(error) = self.release(bytes) {
            self.meter.poison(error);
        }
    }
    /// Changes logical retained capacity without another physical allocation.
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
    /// Records actual allocation excess even when it exceeds the limit.
    pub fn reconcile_capacity(&mut self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        if bytes < self.bytes {
            return self.release(self.bytes - bytes);
        }
        let (mut added, result) = self
            .meter
            .record_completed_allocation(bytes - self.bytes, self.capture.clone());
        self.absorb(&mut added)?;
        result
    }
    fn root_metadata_for(&self, meter: &Arc<crate::demand::NativeDemandReadMeter>) -> bool {
        Arc::ptr_eq(&self.meter.state, meter)
            && self.meter.resource.is_none()
            && self.capture.is_none()
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
            _capture: self.capture.clone(),
            bytes,
        })
    }
    pub(crate) fn transfer_checkout_control(
        &mut self,
        owner: &Arc<crate::kernel::ProofDataCustody>,
        scope: &crate::demand::NativeCheckoutControlWitness,
    ) -> Result<crate::kernel::ProofDataClaim, NativeTreeMaterializationError> {
        if !scope.owns(&self.meter.state, owner) {
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
        scope: &crate::demand::NativeCheckoutControlWitness,
    ) -> Result<crate::kernel::ProofDataClaim, NativeTreeMaterializationError> {
        if !scope.owns(&self.meter.state, owner) {
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
    _capture: Option<Arc<crate::demand::CaptureTransientCustody>>,
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
