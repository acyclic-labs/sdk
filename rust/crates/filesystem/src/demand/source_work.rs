use super::*;
use crate::NativeMaterializationWork;
use crate::native_host::{DemandWorkContext, NativeHostWork};

pub(crate) trait SourceDemandReadWork:
    DemandWorkContext + NativeHostWork<Error = DemandError, Allocation = u64, CleanupWrite = u64>
{
    #[cfg(windows)]
    fn with_native_read_meter<T>(
        &mut self,
        operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
    ) -> T;
    fn with_original_leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
        self.with_demand_work(operation)
    }
    #[cfg(any(test, feature = "native-watch"))]
    fn native_source_work(&self) -> Option<NativeMaterializationWork> {
        None
    }
    #[cfg(any(test, feature = "native-watch"))]
    fn with_ordinary_work<T>(&mut self, _: impl FnOnce(&mut DemandWork) -> T) -> Option<T> {
        None
    }
    #[cfg(any(test, feature = "native-watch"))]
    fn is_native_original(&self) -> bool {
        self.native_source_work().is_some()
    }
    #[cfg(all(feature = "native-watch", target_os = "macos"))]
    fn reserve_source_copy(
        &mut self,
        bytes: u64,
    ) -> Result<Option<crate::materializer::NativeWorkReservation>, DemandError> {
        let delta = WorkCounters {
            bytes_copied: bytes,
            ..WorkCounters::UNCHARGED
        };
        self.check()?;
        if let Some(native) = self.native_source_work() {
            return native
                .reserve_work(delta)
                .map(Some)
                .map_err(crate::native_name::original_name_error);
        }
        self.admit(delta)?;
        Ok(None)
    }
    fn claim_allocation(
        &mut self,
        bytes: u64,
        operations: u64,
        delta: WorkCounters,
    ) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.claim_allocation(bytes, operations, delta))
    }
    #[cfg(any(test, feature = "native-watch"))]
    fn remaining(&mut self) -> Result<WorkBudget, DemandError> {
        self.with_original_leaf(|work| work.remaining())
    }
    #[cfg(any(test, feature = "native-watch"))]
    fn merge(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.merge(delta, work.live_allocation_bytes()))
    }
}

impl SourceDemandReadWork for DemandWork {
    #[cfg(any(test, feature = "native-watch"))]
    fn with_ordinary_work<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> Option<T> {
        Some(operation(self))
    }
    #[cfg(windows)]
    fn with_native_read_meter<T>(
        &mut self,
        operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
    ) -> T {
        let meter = NativeDemandReadMeter::new(self.take_for_native_queue());
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| operation(&meter)));
        *self = meter.0.into_inner().unwrap_or_else(|poisoned| {
            let mut work = poisoned.into_inner();
            work.poison_native_original_work();
            work
        });
        result.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    }
}

impl SourceDemandReadWork for NativeMaterializationWork {
    #[cfg(any(test, feature = "native-watch"))]
    fn native_source_work(&self) -> Option<NativeMaterializationWork> {
        Some(self.clone())
    }
    #[cfg(windows)]
    fn with_native_read_meter<T>(
        &mut self,
        operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
    ) -> T {
        operation(self.runtime_meter().as_ref())
    }
}

impl DemandWorkContext for NativeMaterializationWork {
    fn check_work(&self) -> Result<(), DemandError> {
        NativeMaterializationWork::with_original_leaf(self, |work| work.check())
    }
    fn with_demand_work<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
        NativeMaterializationWork::with_original_leaf(self, operation)
    }
}
