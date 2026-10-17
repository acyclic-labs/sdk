use super::{DemandWork, WorkCounters};

// Per original ledger, under its existing leaf lock; no global fault state.
// Charge, completion credit, and pre-funded I/O all pass through this probe.
pub(crate) struct WriteProbe {
    pub(crate) writes: u64,
    pub(crate) stop_before: Option<u64>,
}

impl DemandWork {
    pub(super) fn probe_write(&mut self, delta: WorkCounters) {
        let Some(probe) = &mut self.write_probe else {
            return;
        };
        let writes = delta.backend_write_operations + delta.durability_operations;
        assert!(
            writes <= 1,
            "write sweep must see individual I/O, not batches"
        );
        if writes == 0 {
            return;
        }
        probe.writes += 1;
        if probe.stop_before == Some(probe.writes) {
            // A child test process exits without unwinding or running cleanup.
            std::process::exit(86);
        }
    }
}
