use super::*;

macro_rules! demand_meter {
    ($owner:ty $(, $allocation_owner:expr)?) => {
        impl $owner {
            fn io<T>(&self, operation: impl FnOnce(&mut crate::demand::DemandWork) -> Result<T, crate::DemandError>) -> std::io::Result<T> {
                self.with_original_leaf(|state| operation(state).map_err(|error| state.record_native_io_failure(error)))
            }
            fn cancel(&self, operation: impl FnOnce(&mut crate::demand::DemandWork) -> Result<(), crate::DemandError>) {
                self.with_original_leaf(|state| {
                    if let Err(error) = operation(state) {
                        state.record_native_io_failure(error);
                        state.poison_native_original_work();
                    }
                });
            }
        }
        impl acyclic_native_runtime::NativeReadMeter for $owner {
            $(fn allocation_owner(&self) -> Option<std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>> { ($allocation_owner)(self) })?
            fn check(&self) -> std::io::Result<()> { self.io(|state| state.check()) }
            fn reserve_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) -> std::io::Result<()> {
                self.io(|state| state.reserve(crate::demand::DemandWork::operation_credit(kind), count))
            }
            fn cancel_reserved_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) {
                self.cancel(|state| state.refund(crate::demand::DemandWork::operation_credit(kind), count));
            }
            fn record_reserved_operation(&self, kind: acyclic_native_runtime::NativeOperationKind) -> std::io::Result<()> {
                self.io(|state| state.complete(crate::demand::DemandWork::operation_credit(kind), 1, 1))
            }
            fn reserve_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, bytes: u64) -> std::io::Result<()> {
                self.io(|state| state.reserve(crate::demand::DemandWork::byte_credit(kind)?, bytes))
            }
            fn cancel_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, bytes: u64) {
                self.cancel(|state| state.refund(crate::demand::DemandWork::byte_credit(kind)?, bytes));
            }
            fn record_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64, actual: u64) -> std::io::Result<()> {
                self.io(|state| state.complete(crate::demand::DemandWork::byte_credit(kind)?, maximum, actual))
            }
            fn allocate(&self, bytes: u64) -> std::io::Result<()> {
                self.io(|state| state.claim_native_owned(bytes, u64::from(bytes != 0), crate::WorkCounters::UNCHARGED, &None))
            }
            fn adopt(&self, bytes: u64) -> std::io::Result<()> {
                self.io(|state| state.claim_native_owned(bytes, 0, crate::WorkCounters::UNCHARGED, &None))
            }
            fn allocate_refcount_header(&self) -> std::io::Result<()> {
                self.io(|state| state.charge(crate::WorkCounters { allocation_operations: 1, ..crate::WorkCounters::UNCHARGED }))
            }
            fn reserve_allocation(&self, bytes: u64) -> std::io::Result<()> { self.io(|state| state.reserve_native_allocation(bytes)) }
            fn cancel_reserved_allocation(&self, bytes: u64) { self.cancel(|state| state.cancel_reserved_native_allocation(bytes)); }
            fn record_reserved_allocation(&self, bytes: u64) -> std::io::Result<()> { self.io(|state| state.record_reserved_native_allocation(bytes)) }
            fn allocation_failed(&self, error: std::collections::TryReserveError) -> std::io::Error {
                self.with_original_leaf(|state| state.record_native_io_failure(crate::DemandError::Allocation(error)))
            }
            fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
                self.with_original_leaf(|state| {
                    let (bytes, result) = state.record_native_owned_completed(bytes, &None);
                    (bytes, result.map_err(|error| state.record_native_io_failure(error)))
                })
            }
            fn release(&self, bytes: u64) { self.cancel(|state| state.release_native_owned(bytes, &None)); }
            fn admit_write_bytes(&self, bytes: usize) -> std::io::Result<()> {
                self.io(|state| state.admit(NativeResourceCredit::WriteBytes.delta(bytes as u64)))
            }
            fn admit_bytes(&self, bytes: usize) -> std::io::Result<()> {
                self.io(|state| state.admit(NativeResourceCredit::ReadBytes.delta(bytes as u64)))
            }
            fn record_read(&self, bytes: usize) -> std::io::Result<()> {
                self.io(|state| state.record(NativeResourceCredit::ReadBytes.delta(bytes as u64)))
            }
            fn record_write(&self, bytes: usize) -> std::io::Result<()> {
                self.io(|state| state.record(NativeResourceCredit::WriteBytes.delta(bytes as u64)))
            }
            fn admit_read(&self, bytes: usize) -> std::io::Result<()> {
                self.io(|state| { state.admit(NativeResourceCredit::ReadBytes.delta(bytes as u64))?; state.charge(NativeResourceCredit::ReadOperations.delta(1)) })
            }
            fn admit_write(&self, bytes: usize) -> std::io::Result<()> {
                self.io(|state| { state.admit(NativeResourceCredit::WriteBytes.delta(bytes as u64))?; state.charge(NativeResourceCredit::WriteOperations.delta(1)) })
            }
            fn admit_durability(&self) -> std::io::Result<()> {
                self.io(|state| state.charge(NativeResourceCredit::DurabilityOperations.delta(1)))
            }
            fn admit_codec(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
                self.io(|state| state.admit(crate::WorkCounters { bytes_encoded: encoded, bytes_copied: copied, bytes_hashed: hashed, ..crate::WorkCounters::UNCHARGED }))
            }
            fn record_codec(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
                self.io(|state| state.record(crate::WorkCounters { bytes_encoded: encoded, bytes_copied: copied, bytes_hashed: hashed, ..crate::WorkCounters::UNCHARGED }))
            }
            fn examine(&self, items: u64, copied: u64) -> std::io::Result<()> {
                self.io(|state| state.charge(crate::WorkCounters { items_examined: items, bytes_copied: copied, ..crate::WorkCounters::UNCHARGED }))
            }
            fn admit_examination(&self, items: u64, copied: u64) -> std::io::Result<()> {
                self.io(|state| state.admit(crate::WorkCounters { items_examined: items, bytes_copied: copied, ..crate::WorkCounters::UNCHARGED }))
            }
            fn record_examination(&self, items: u64, copied: u64) -> std::io::Result<()> {
                self.io(|state| state.record(crate::WorkCounters { items_examined: items, bytes_copied: copied, ..crate::WorkCounters::UNCHARGED }))
            }
            fn reserve_cleanup_write(&self) -> std::io::Result<u64> { self.io(|state| crate::native_host::NativeHostWork::reserve_cleanup_write(state)) }
            fn cancel_cleanup_write(&self, reservation: u64) { self.with_original_leaf(|state| crate::native_host::NativeHostWork::cancel_cleanup_write(state, reservation)); }
            fn record_cleanup_write(&self, reservation: u64) -> std::io::Result<()> { self.io(|state| crate::native_host::NativeHostWork::attempt_cleanup_write(state, reservation, || ()).1) }
        }
    };
}

demand_meter!(NativeResourceAllowance);
demand_meter!(NativeInvocationView);
#[cfg(feature = "native-mount")]
demand_meter!(
    NativeSourceExecutionView,
    |view: &NativeSourceExecutionView| Some(view.metadata.work().runtime_meter())
);
