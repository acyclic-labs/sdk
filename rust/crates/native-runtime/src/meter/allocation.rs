use crate::*;

/// One inline, original-meter allocation credit prepared before accepted IO.
#[must_use]
pub struct ReadAllocationReservation {
    meter: Arc<dyn NativeReadMeter>,
    bytes: Option<u64>,
}

impl ReadAllocationReservation {
    /// Withholds one known positive allocation without allocating this token.
    pub fn new(meter: &Arc<dyn NativeReadMeter>, bytes: u64) -> io::Result<Self> {
        meter.reserve_allocation(bytes)?;
        Ok(Self {
            meter: Arc::clone(meter),
            bytes: Some(bytes),
        })
    }

    /// Records the accepted allocation and transfers its exact original custody.
    pub fn allocate(mut self) -> io::Result<ReadAllocation> {
        let bytes = self.bytes.expect("allocation credit is still held");
        self.meter.record_reserved_allocation(bytes)?;
        self.bytes = None;
        let mut allocation = ReadAllocation::empty(&self.meter);
        allocation.bytes = bytes;
        Ok(allocation)
    }
}

impl Drop for ReadAllocationReservation {
    fn drop(&mut self) {
        if let Some(bytes) = self.bytes {
            self.meter.cancel_reserved_allocation(bytes);
        }
    }
}

/// Custody of logical SDK-owned payload on the original invocation ledger.
#[must_use]
pub struct ReadAllocation {
    meter: Arc<dyn NativeReadMeter>,
    bytes: u64,
}

impl ReadAllocation {
    /// Admits one genuine positive allocation attempt before its allocator call.
    pub fn new(meter: &Arc<dyn NativeReadMeter>, bytes: usize) -> io::Result<Self> {
        let bytes = bytes as u64;
        if bytes == 0 {
            meter.check()?;
        } else {
            meter.allocate(bytes)?;
        }
        let mut allocation = Self::empty(meter);
        allocation.bytes = bytes;
        Ok(allocation)
    }

    /// Borrows the same original meter without an allocation or resource grant.
    pub fn empty(meter: &Arc<dyn NativeReadMeter>) -> Self {
        Self {
            meter: meter
                .allocation_owner()
                .unwrap_or_else(|| Arc::clone(meter)),
            bytes: 0,
        }
    }

    /// Admits retention of an existing transferred payload, with zero allocation operations.
    pub fn adopt(meter: &Arc<dyn NativeReadMeter>, bytes: usize) -> io::Result<Self> {
        let bytes = bytes as u64;
        if bytes == 0 {
            meter.check()?;
        } else {
            meter.adopt(bytes)?;
        }
        let mut allocation = Self::empty(meter);
        allocation.bytes = bytes;
        Ok(allocation)
    }

    /// Admits another genuine positive allocation attempt before it occurs.
    pub fn grow(&mut self, additional: usize) -> io::Result<()> {
        let additional = additional as u64;
        let bytes = self
            .bytes
            .checked_add(additional)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        if additional != 0 {
            self.meter.allocate(additional)?;
        }
        self.bytes = bytes;
        Ok(())
    }

    /// Admits logical retention before an allocation/transfer, without another allocation operation.
    pub fn resize_retained(&mut self, actual: usize) -> io::Result<()> {
        let actual = actual as u64;
        if actual > self.bytes {
            self.meter.adopt(actual - self.bytes)?;
        } else if actual < self.bytes {
            self.meter.release(self.bytes - actual);
        }
        self.bytes = actual;
        Ok(())
    }

    /// Records real capacity discovered after a controlled allocation.
    pub fn reconcile_capacity(&mut self, actual: usize) -> io::Result<()> {
        Self::reconcile_capacity_recorded(&mut self.bytes, &self.meter, actual, self.meter.as_ref())
    }

    /// Reconciles an already allocated buffer through its accepted completion meter.
    pub fn reconcile_capacity_with_meter(
        &mut self,
        actual: usize,
        completion: &dyn NativeReadMeter,
    ) -> io::Result<()> {
        let owner = completion
            .allocation_owner()
            .ok_or(io::ErrorKind::InvalidInput)?;
        if !Arc::ptr_eq(&owner, &self.meter) {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        Self::reconcile_capacity_recorded(&mut self.bytes, &self.meter, actual, completion)
    }

    fn reconcile_capacity_recorded(
        bytes: &mut u64,
        original: &Arc<dyn NativeReadMeter>,
        actual: usize,
        completion: &dyn NativeReadMeter,
    ) -> io::Result<()> {
        let actual = actual as u64;
        if actual > *bytes {
            let additional = actual - *bytes;
            let (retained, result) = completion.record_completed_adoption(additional);
            assert!(
                retained <= additional,
                "completed adoption retains only actual excess"
            );
            *bytes += retained;
            result?;
            assert_eq!(
                retained, additional,
                "successful adoption retains all actual excess"
            );
        } else if actual < *bytes {
            original.release(*bytes - actual);
            *bytes = actual;
        }
        Ok(())
    }

    /// Logical capacity currently owned by this guard.
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }

    /// The original shared invocation, not a newly constructed ledger.
    pub const fn meter(&self) -> &Arc<dyn NativeReadMeter> {
        &self.meter
    }

    /// Transfers an already-owned claim to its exact original meter owner.
    pub fn consume_same_meter_owned_bytes(
        mut self,
        expected: &Arc<dyn NativeReadMeter>,
    ) -> Result<u64, Self> {
        if !Arc::ptr_eq(&self.meter, expected) {
            return Err(self);
        }
        let bytes = self.bytes;
        self.bytes = 0;
        Ok(bytes)
    }

    /// Moves another same-ledger claim without allocating or releasing it.
    pub fn absorb(&mut self, other: &mut Self) -> io::Result<()> {
        if !Arc::ptr_eq(&self.meter, &other.meter) {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let bytes = self
            .bytes
            .checked_add(other.bytes)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        self.bytes = bytes;
        other.bytes = 0;
        Ok(())
    }

    /// Splits custody already owned here, with no new quota grant or allocation.
    pub fn split(&mut self, bytes: usize) -> io::Result<Self> {
        let bytes = bytes as u64;
        let remaining = self
            .bytes
            .checked_sub(bytes)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        self.bytes = remaining;
        Ok(Self {
            meter: Arc::clone(&self.meter),
            bytes,
        })
    }

    /// Releases only physically retired payload owned by this guard.
    pub fn release(&mut self, bytes: usize) -> io::Result<()> {
        let bytes = bytes as u64;
        let remaining = self
            .bytes
            .checked_sub(bytes)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        self.meter.release(bytes);
        self.bytes = remaining;
        Ok(())
    }
}

impl std::fmt::Debug for ReadAllocation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReadAllocation")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl Drop for ReadAllocation {
    fn drop(&mut self) {
        self.meter.release(self.bytes);
    }
}
