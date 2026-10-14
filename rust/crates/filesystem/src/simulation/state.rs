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
        let copied = self
            .trace
            .len()
            .checked_mul(std::mem::size_of::<SimulationTrace>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        let delta = WorkCounters {
            bytes_copied: copied,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(delta)?;
        let mut replacement =
            crate::kernel::Owned::<Vec<SimulationTrace>>::allocate(capacity, accounting)?;
        replacement.value.extend_from_slice(&self.trace);
        accounting.record_completed(delta)?;
        self.trace = replacement.value;
        self.trace_allocation = replacement.allocation;
        Ok(())
    }

    pub(super) fn intercept(
        &mut self,
        operation: SimulationOperation,
    ) -> Result<Option<SimulationFault>, &'static str> {
        if self.trace.len() == self.options.maximum_trace_entries {
            return Err("simulation trace bound exhausted");
        }
        let counter = self
            .occurrences
            .get_mut(operation as usize)
            .ok_or("invalid simulation operation")?;
        let occurrence = counter
            .checked_add(1)
            .ok_or("simulation operation occurrence overflow")?;
        *counter = occurrence;
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
