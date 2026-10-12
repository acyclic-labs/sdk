use super::*;
use std::sync::Arc;

/// Retained bytes on the existing demand ledger.
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
    /// Transfers an existing runtime allocation without readmission.
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
                meter: work.clone(),
                bytes,
                capture: None,
            })
            .map_err(|allocation| (NativeTreeMaterializationError::InvalidLayout, allocation))
    }
    /// Current owned capacity.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
    /// Original ledger and resource attribution.
    pub const fn work(&self) -> &NativeMaterializationWork {
        &self.meter
    }
    /// Same original ledger.
    pub fn shares_ledger(&self, other: &Self) -> bool {
        self.meter.shares_ledger(&other.meter)
    }
    /// Admits actual buffer growth before allocation.
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
    /// Combines paid capacities with identical attribution.
    pub fn absorb(&mut self, other: &mut Self) -> Result<(), NativeTreeMaterializationError> {
        if !self.meter.shares_allocation_context(&other.meter) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let same_capture = match (&self.capture, &other.capture) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        };
        if !same_capture && self.bytes != 0 && other.bytes != 0 {
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
    /// Splits existing paid capacity.
    pub fn split_off(&mut self, bytes: u64) -> Result<Self, NativeTreeMaterializationError> {
        let remaining = self
            .bytes
            .checked_sub(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        let split = Self {
            meter: self.meter.clone(),
            capture: self.capture.clone(),
            bytes,
        };
        self.bytes = remaining;
        Ok(split)
    }
    pub(super) fn release_original(&mut self, bytes: u64) -> Result<(), crate::DemandError> {
        let remaining = self
            .bytes
            .checked_sub(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        let result = self
            .meter
            .state(|demand| demand.release_native_owned(bytes, &None));
        self.bytes = remaining;
        result
    }
    /// Releases capacity after destroying its backing storage.
    pub fn release(&mut self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        self.release_original(bytes)
            .map_err(native_materialization_work_error)
    }
    pub(crate) fn release_deferred(&mut self, bytes: u64) {
        if let Err(error) = self.release_original(bytes) {
            self.meter.state(|demand| {
                demand.record_native_io_failure(error);
                demand.poison_native_original_work();
            });
        }
    }
    /// Shrinks an admitted quota to its actual capacity.
    pub fn substitute(&mut self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        self.release(
            self.bytes
                .checked_sub(bytes)
                .ok_or(crate::WorkError::Overflow)?,
        )
    }
    /// Retains logical capacity on the existing ledger.
    pub fn resize_retained(&mut self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        if bytes < self.bytes {
            return self.release(self.bytes - bytes);
        }
        self.meter
            .state(|demand| demand.retain_native_owned(bytes - self.bytes, &None))
            .map_err(native_materialization_work_error)?;
        self.bytes = bytes;
        Ok(())
    }
    /// Records completed allocation excess before returning a limit failure.
    pub fn reconcile_capacity(
        &mut self,
        actual: u64,
    ) -> Result<(), NativeTreeMaterializationError> {
        if actual < self.bytes {
            self.release(self.bytes - actual)?;
        }
        let (mut allocation, result) = self
            .meter
            .record_completed_allocation(actual - self.bytes, self.capture.clone());
        self.absorb(&mut allocation)?;
        result
    }
    pub(super) fn is_root_metadata_for(
        &self,
        original: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> bool {
        Arc::ptr_eq(&self.meter.state, original)
            && self.meter.resource.is_none()
            && self.capture.is_none()
    }
    pub(crate) fn is_checkout_header_for(
        &self,
        original: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> bool {
        self.is_root_metadata_for(original)
            && self.bytes == std::mem::size_of::<crate::kernel::ProofDataCustody>() as u64
    }
    pub(crate) fn transfer_checkout_metadata(
        &mut self,
        original: &Arc<crate::demand::NativeDemandReadMeter>,
        expected: u64,
    ) -> Result<u64, NativeTreeMaterializationError> {
        if !self.is_root_metadata_for(original) || self.bytes != expected {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        Ok(std::mem::take(&mut self.bytes))
    }
    pub(crate) fn transfer_resource_retirement(
        &mut self,
        bytes: u64,
    ) -> Result<NativeResourceRetirement, NativeTreeMaterializationError> {
        let remaining = self
            .bytes
            .checked_sub(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        let retirement = NativeResourceRetirement {
            original: self.meter.state.clone(),
            resource: self.meter.resource.clone(),
            _capture: self.capture.clone(),
            bytes,
        };
        self.bytes = remaining;
        Ok(retirement)
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
        if self.bytes != 0 {
            self.release_deferred(self.bytes);
        }
    }
}

/// Keeps resource attribution until the existing physical proof retires.
pub(crate) struct NativeResourceRetirement {
    original: Arc<crate::demand::NativeDemandReadMeter>,
    resource: Option<Arc<NativeResourceAllowance>>,
    _capture: Option<Arc<crate::demand::CaptureTransientCustody>>,
    bytes: u64,
}
impl NativeResourceRetirement {
    pub(crate) const fn bytes(&self) -> u64 {
        self.bytes
    }
    pub(crate) fn original_meter(&self) -> &Arc<crate::demand::NativeDemandReadMeter> {
        &self.original
    }
}
impl std::fmt::Debug for NativeResourceRetirement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeResourceRetirement")
            .field("bytes", &self.bytes)
            .finish()
    }
}
impl Drop for NativeResourceRetirement {
    fn drop(&mut self) {
        if let Some(resource) = &self.resource {
            if let Err(error) = resource.retire(self.bytes) {
                let mut demand = self.original.lock();
                demand.record_native_io_failure(error);
                demand.poison_native_original_work();
            }
        }
    }
}
