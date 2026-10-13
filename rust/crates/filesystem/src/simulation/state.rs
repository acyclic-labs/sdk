use super::*;

pub(super) struct State {
    pub(super) options: SimulationOptions,
    pub(super) occurrences: [u64; SIMULATION_OPERATION_COUNT],
    pub(super) faults: BTreeMap<(SimulationOperation, u64), SimulationFault>,
    pub(super) trace: Vec<SimulationTrace>,
    pub(super) trace_allocation: KernelOwnedAllocation,
    pub(super) pending_objects: BTreeMap<ObjectId, OwnedBytes>,
    pub(super) pending_object_bytes: u64,
}

impl State {
    pub(super) fn prepare_trace(
        &mut self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        accounting.check()?;
        if self.trace.len() == self.options.maximum_trace_entries {
            return Err(ObjectStoreError::Simulation("simulation trace bound exhausted").into());
        }
        if self.trace.len() < self.trace.capacity() {
            return Ok(());
        }
        let capacity = self
            .trace
            .capacity()
            .checked_mul(2)
            .ok_or(crate::WorkError::Overflow)?
            .max(1)
            .min(self.options.maximum_trace_entries);
        let bytes = u64::try_from(
            capacity
                .checked_mul(std::mem::size_of::<SimulationTrace>())
                .ok_or(crate::WorkError::Overflow)?,
        )
        .map_err(|_| crate::WorkError::Overflow)?;
        let copied = u64::try_from(
            self.trace
                .len()
                .checked_mul(std::mem::size_of::<SimulationTrace>())
                .ok_or(crate::WorkError::Overflow)?,
        )
        .map_err(|_| crate::WorkError::Overflow)?;
        let delta = WorkCounters {
            bytes_copied: copied,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(delta)?;
        let allocation = accounting.claim_owned(bytes, 1)?;
        let mut replacement = (Vec::new(), allocation);
        replacement
            .0
            .try_reserve_exact(capacity)
            .map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
        let actual = u64::try_from(
            replacement
                .0
                .capacity()
                .checked_mul(std::mem::size_of::<SimulationTrace>())
                .ok_or(crate::WorkError::Overflow)?,
        )
        .map_err(|_| crate::WorkError::Overflow)?;
        accounting.reconcile_owned(&mut replacement.1, bytes, actual)?;
        accounting.admit(delta)?;
        replacement.0.extend_from_slice(&self.trace);
        accounting.record_completed(delta)?;
        let (trace, allocation) = replacement;
        self.trace = trace;
        self.trace_allocation = allocation;
        Ok(())
    }

    pub(super) fn intercept(
        &mut self,
        operation: SimulationOperation,
    ) -> Result<Option<SimulationFault>, &'static str> {
        if self.trace.len() == self.options.maximum_trace_entries {
            return Err("simulation trace bound exhausted");
        }
        let occurrence = self.occurrences[operation as usize]
            .checked_add(1)
            .ok_or("simulation operation occurrence overflow")?;
        self.occurrences[operation as usize] = occurrence;
        let fault = self.faults.remove(&(operation, occurrence));
        let replaces_trace = self.trace.len() == self.trace.capacity();
        self.trace
            .try_reserve(1)
            .map_err(|_| "simulation trace allocation failed")?;
        if replaces_trace {
            self.trace_allocation = KernelOwnedAllocation::Standalone;
        }
        self.trace.push(SimulationTrace {
            operation,
            occurrence,
            fault,
        });
        Ok(fault)
    }
}
