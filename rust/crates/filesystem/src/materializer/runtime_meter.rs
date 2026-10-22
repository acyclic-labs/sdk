use super::*;
use crate::demand::DemandWork;

macro_rules! demand_meter {
    ($owner:ty $(, $allocation_owner:expr)?) => {
        demand_meter!(@impl $owner, |_: &$owner, kind| DemandWork::byte_credit(kind) $(, $allocation_owner)?);
    };
    (@impl $owner:ty, $byte_credit:expr $(, $allocation_owner:expr)?) => {
        impl $owner {
            fn byte_credit(&self, kind: acyclic_native_runtime::NativeOperationKind) -> std::io::Result<NativeResourceCredit> {
                ($byte_credit)(self, kind)
            }
            fn io<T>(&self, operation: impl FnOnce(&mut DemandWork) -> Result<T, crate::DemandError>) -> std::io::Result<T> {
                self.with_original_leaf(|state| operation(state).map_err(|error| state.record_native_io_failure(error)))
            }
            fn cancel(&self, operation: impl FnOnce(&mut DemandWork) -> Result<(), crate::DemandError>) {
                self.with_original_leaf(|state| { if let Err(error) = operation(state) { state.record_native_io_failure(error); state.poison_native_original_work(); } });
            }
        }
        impl acyclic_native_runtime::NativeReadMeter for $owner {
            $(fn allocation_owner(&self) -> Option<std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>> { ($allocation_owner)(self) })?
            fn check(&self) -> std::io::Result<()> { self.io(|state| state.check()) }
            fn reserve_cleanup_write(&self) -> std::io::Result<u64> {
                self.io(|state| crate::native_host::NativeHostWork::reserve_cleanup_write(state))
            }
            fn cancel_cleanup_write(&self, reservation: u64) {
                self.cancel(|state| state.refund(NativeResourceCredit::WriteOperations.delta(reservation)));
            }
            fn record_cleanup_write(&self, reservation: u64) -> std::io::Result<()> {
                self.io(|state| crate::native_host::NativeHostWork::attempt_cleanup_write(state, reservation, || ()).1)
            }
            fn reserve_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) -> std::io::Result<()> {
                self.io(|state| state.reserve(crate::demand::DemandWork::operation_credit(kind).delta(count)))
            }
            fn cancel_reserved_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) {
                self.cancel(|state| state.refund(crate::demand::DemandWork::operation_credit(kind).delta(count)));
            }
            fn record_reserved_operation(&self, kind: acyclic_native_runtime::NativeOperationKind) -> std::io::Result<()> {
                self.io(|state| state.complete(crate::demand::DemandWork::operation_credit(kind).delta(1), crate::demand::DemandWork::operation_credit(kind).delta(1)))
            }
            fn reserve_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, bytes: u64) -> std::io::Result<()> {
                self.io(|state| state.reserve(self.byte_credit(kind)?.delta(bytes)))
            }
            fn cancel_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, bytes: u64) {
                self.cancel(|state| state.refund(self.byte_credit(kind)?.delta(bytes)));
            }
            fn record_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64, actual: u64) -> std::io::Result<()> {
                self.io(|state| state.complete(self.byte_credit(kind)?.delta(maximum), self.byte_credit(kind)?.delta(actual)))
            }
            fn allocate(&self, bytes: u64) -> std::io::Result<()> {
                self.io(|state| state.claim_allocation(bytes, u64::from(bytes != 0), crate::WorkCounters::UNCHARGED))
            }
            fn adopt(&self, bytes: u64) -> std::io::Result<()> {
                self.io(|state| state.claim_allocation(bytes, 0, crate::WorkCounters::UNCHARGED))
            }
            fn allocate_refcount_header(&self) -> std::io::Result<()> {
                self.io(|state| state.charge(crate::WorkCounters { allocation_operations: 1, ..crate::WorkCounters::UNCHARGED }))
            }
            fn reserve_allocation(&self, bytes: u64) -> std::io::Result<()> { self.io(|state| state.reserve_native_allocation(bytes)) }
            fn cancel_reserved_allocation(&self, bytes: u64) { self.cancel(|state| state.settle_native_allocation(bytes, false)); }
            fn record_reserved_allocation(&self, bytes: u64) -> std::io::Result<()> { self.io(|state| state.settle_native_allocation(bytes, true)) }
            fn allocation_failed(&self, error: std::collections::TryReserveError) -> std::io::Error {
                self.with_original_leaf(|state| state.record_native_io_failure(crate::DemandError::Allocation(error)))
            }
            fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
                self.with_original_leaf(|state| {
                    let (bytes, result) = state.record_native_owned_completed(bytes, &None);
                    (bytes, result.map_err(|error| state.record_native_io_failure(error)))
                })
            }
            fn release(&self, bytes: u64) { self.cancel(|state| state.release(bytes)); }
            fn admit_write_bytes(&self, bytes: usize) -> std::io::Result<()> {
                self.io(|state| state.admit(NativeResourceCredit::WriteBytes.delta(bytes as u64)))
            }
            fn admit_bytes(&self, bytes: usize) -> std::io::Result<()> {
                self.io(|state| state.admit(self.byte_credit(acyclic_native_runtime::NativeOperationKind::Read)?.delta(bytes as u64)))
            }
            fn record_read(&self, bytes: usize) -> std::io::Result<()> {
                self.io(|state| state.record(self.byte_credit(acyclic_native_runtime::NativeOperationKind::Read)?.delta(bytes as u64)))
            }
            fn record_write(&self, bytes: usize) -> std::io::Result<()> {
                self.io(|state| state.record(NativeResourceCredit::WriteBytes.delta(bytes as u64)))
            }
            fn admit_read(&self, bytes: usize) -> std::io::Result<()> {
                self.io(|state| { state.admit(self.byte_credit(acyclic_native_runtime::NativeOperationKind::Read)?.delta(bytes as u64))?; state.charge(NativeResourceCredit::ReadOperations.delta(1)) })
            }
            fn admit_write(&self, bytes: usize) -> std::io::Result<()> {
                self.io(|state| { state.admit(NativeResourceCredit::WriteBytes.delta(bytes as u64))?; state.charge(NativeResourceCredit::WriteOperations.delta(1)) })
            }
            fn admit_durability(&self) -> std::io::Result<()> {
                self.io(|state| state.charge(NativeResourceCredit::DurabilityOperations.delta(1)))
            }
            fn admit_examination(&self, items: u64, copied: u64) -> std::io::Result<()> {
                self.io(|state| state.admit(crate::WorkCounters { items_examined: items, bytes_copied: copied, ..crate::WorkCounters::UNCHARGED }))
            }
            fn record_examination(&self, items: u64, copied: u64) -> std::io::Result<()> {
                self.io(|state| state.record(crate::WorkCounters { items_examined: items, bytes_copied: copied, ..crate::WorkCounters::UNCHARGED }))
            }
            fn reserve_examination(&self, items: u64) -> std::io::Result<()> {
                self.io(|state| state.reserve(crate::WorkCounters { items_examined: items, ..crate::WorkCounters::UNCHARGED }))
            }
            fn cancel_reserved_examination(&self, items: u64) {
                self.cancel(|state| state.refund(crate::WorkCounters { items_examined: items, ..crate::WorkCounters::UNCHARGED }));
            }
            fn record_reserved_examination(&self, items: u64) -> std::io::Result<()> {
                self.io(|state| {
                    let probe = crate::WorkCounters { items_examined: items, ..crate::WorkCounters::UNCHARGED };
                    state.complete(probe, probe)
                })
            }
            fn examine(&self, items: u64, copied: u64) -> std::io::Result<()> {
                let result = self.io(|state| state.charge(crate::WorkCounters { items_examined: items, bytes_copied: copied, ..crate::WorkCounters::UNCHARGED }));
                #[cfg(all(test, any(target_os = "linux", windows)))]
                if result.is_ok() {
                    crate::native_host::stat_probe::examined(items);
                }
                result
            }
            fn admit_codec(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
                self.io(|state| state.admit(crate::WorkCounters { bytes_encoded: encoded, bytes_copied: copied, bytes_hashed: hashed, ..crate::WorkCounters::UNCHARGED }))
            }
            fn record_codec(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
                self.io(|state| state.record(crate::WorkCounters { bytes_encoded: encoded, bytes_copied: copied, bytes_hashed: hashed, ..crate::WorkCounters::UNCHARGED }))
            }
        }
    };
}

demand_meter!(crate::demand::NativeDemandReadMeter);
demand_meter!(NativeResourceAllowance);
demand_meter!(@impl NativeWorkWindow,
    |window: &NativeWorkWindow, kind| window.read_credit(kind),
    |window: &NativeWorkWindow| window.allocation_owner()
);
demand_meter!(@impl NativeInvocationView,
    |view: &NativeInvocationView, kind| match kind {
        acyclic_native_runtime::NativeOperationKind::Read => Ok(view.read_credit),
        _ => DemandWork::byte_credit(kind),
    },
    |view: &NativeInvocationView| match view.read_credit {
        #[cfg(any(test, feature = "distributed"))]
        NativeResourceCredit::AuthorityReadBytes => Some(view.metadata.work().runtime_meter()),
        _ => None,
    }
);
#[cfg(feature = "native-mount")]
demand_meter!(
    NativeSourceExecutionView,
    |view: &NativeSourceExecutionView| Some(view.metadata.work().runtime_meter())
);
