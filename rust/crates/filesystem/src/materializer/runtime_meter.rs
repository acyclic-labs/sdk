macro_rules! impl_original_native_read_meter {
    ($owner:ty $(, $allocation_owner:expr)?) => {
        impl $owner {
            fn with_io<T>(
                &self,
                operation: impl FnOnce(&mut crate::demand::DemandWork) -> Result<T, crate::DemandError>,
            ) -> std::io::Result<T> {
                self.with_original_leaf(|demand| {
                    operation(demand).map_err(|error| demand.record_native_io_failure(error))
                })
            }
        }
        impl acyclic_native_runtime::NativeReadMeter for $owner {
            $(fn allocation_owner(&self) -> Option<std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>> { ($allocation_owner)(self) })?
            fn allocate_refcount_header(&self) -> std::io::Result<()> { self.with_io(|demand| demand.charge(crate::WorkCounters { allocation_operations: 1, ..crate::WorkCounters::UNCHARGED })) }
            fn admit_write_bytes(&self, maximum: usize) -> std::io::Result<()> { self.with_io(|demand| demand.admit(crate::WorkCounters { output_bytes: maximum as u64, ..crate::WorkCounters::UNCHARGED })) }
            fn admit_codec(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> { self.with_io(|demand| demand.admit(crate::WorkCounters { bytes_encoded: encoded, bytes_copied: copied, bytes_hashed: hashed, ..crate::WorkCounters::UNCHARGED })) }
            fn record_codec(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> { self.with_io(|demand| demand.record(crate::WorkCounters { bytes_encoded: encoded, bytes_copied: copied, bytes_hashed: hashed, ..crate::WorkCounters::UNCHARGED })) }
            fn examine(&self, items: u64, copied: u64) -> std::io::Result<()> { self.with_io(|demand| demand.charge(crate::WorkCounters { items_examined: items, bytes_copied: copied, ..crate::WorkCounters::UNCHARGED })) }
            fn admit_examination(&self, items: u64, copied: u64) -> std::io::Result<()> { self.with_io(|demand| demand.admit(crate::WorkCounters { items_examined: items, bytes_copied: copied, ..crate::WorkCounters::UNCHARGED })) }
            fn record_examination(&self, items: u64, copied: u64) -> std::io::Result<()> { self.with_io(|demand| demand.record(crate::WorkCounters { items_examined: items, bytes_copied: copied, ..crate::WorkCounters::UNCHARGED })) }
            fn admit_bytes(&self, maximum: usize) -> std::io::Result<()> { self.with_io(|demand| demand.admit(crate::WorkCounters { source_bytes_read: maximum as u64, ..crate::WorkCounters::UNCHARGED })) }
            fn admit_operations(&self, count: u64) -> std::io::Result<()> { self.with_io(|demand| demand.admit(crate::WorkCounters { backend_read_operations: count, ..crate::WorkCounters::UNCHARGED })) }
            fn record_operations(&self, count: u64) -> std::io::Result<()> { self.with_io(|demand| demand.record(crate::WorkCounters { backend_read_operations: count, ..crate::WorkCounters::UNCHARGED })) }
            fn record_read(&self, bytes: usize) -> std::io::Result<()> { self.with_io(|demand| demand.record(crate::WorkCounters { source_bytes_read: bytes as u64, ..crate::WorkCounters::UNCHARGED })) }
            fn record_write(&self, bytes: usize) -> std::io::Result<()> { self.with_io(|demand| demand.record(crate::WorkCounters { output_bytes: bytes as u64, ..crate::WorkCounters::UNCHARGED })) }
            fn admit_write_operations(&self, count: u64) -> std::io::Result<()> { self.with_io(|demand| demand.admit(crate::WorkCounters { backend_write_operations: count, ..crate::WorkCounters::UNCHARGED })) }
            fn record_write_operations(&self, count: u64) -> std::io::Result<()> { self.with_io(|demand| demand.record(crate::WorkCounters { backend_write_operations: count, ..crate::WorkCounters::UNCHARGED })) }
            fn admit_durability(&self) -> std::io::Result<()> { self.with_io(|demand| demand.charge(crate::WorkCounters { durability_operations: 1, ..crate::WorkCounters::UNCHARGED })) }
            fn check(&self) -> std::io::Result<()> { self.with_io(|demand| demand.check()) }
            fn reserve_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) -> std::io::Result<()> {
                self.with_io(|demand| demand.reserve(crate::demand::DemandWork::operation_credit(kind), count))
            }
            fn cancel_reserved_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) {
                self.with_original_leaf(|demand| demand.refund(crate::demand::DemandWork::operation_credit(kind), count).expect("balanced original credit"));
            }
            fn record_reserved_operation(&self, kind: acyclic_native_runtime::NativeOperationKind) -> std::io::Result<()> {
                self.with_io(|demand| demand.complete(crate::demand::DemandWork::operation_credit(kind), 1, 1))
            }
            fn reserve_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64) -> std::io::Result<()> {
                self.with_original_leaf(|demand| demand.reserve(crate::demand::DemandWork::byte_credit(kind)?, maximum).map_err(|error| demand.record_native_io_failure(error)))
            }
            fn cancel_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64) {
                self.with_original_leaf(|demand| demand.refund(crate::demand::DemandWork::byte_credit(kind).expect("byte operation"), maximum).expect("balanced original credit"));
            }
            fn record_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64, actual: u64) -> std::io::Result<()> {
                self.with_original_leaf(|demand| demand.complete(crate::demand::DemandWork::byte_credit(kind)?, maximum, actual).map_err(|error| demand.record_native_io_failure(error)))
            }
            fn allocate(&self, bytes: u64) -> std::io::Result<()> { self.with_io(|demand| demand.claim_native_owned(bytes, u64::from(bytes != 0), crate::WorkCounters::UNCHARGED, &None)) }
            fn reserve_allocation(&self, bytes: u64) -> std::io::Result<()> { self.with_io(|demand| demand.reserve_native_allocation(bytes)) }
            fn cancel_reserved_allocation(&self, bytes: u64) {
                self.with_original_leaf(|demand| { if let Err(error) = demand.cancel_reserved_native_allocation(bytes) {
                    demand.record_native_io_failure(error); demand.poison_native_original_work();
                } });
            }
            fn record_reserved_allocation(&self, bytes: u64) -> std::io::Result<()> { self.with_io(|demand| demand.record_reserved_native_allocation(bytes)) }
            fn allocation_failed(&self, error: std::collections::TryReserveError) -> std::io::Error {
                self.with_original_leaf(|demand| demand.record_native_io_failure(crate::DemandError::Allocation(error)))
            }
            fn adopt(&self, bytes: u64) -> std::io::Result<()> { self.with_io(|demand| demand.claim_native_owned(bytes, 0, crate::WorkCounters::UNCHARGED, &None)) }
            fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
                self.with_original_leaf(|demand| { let (bytes, result) = demand.record_native_owned_completed(bytes, &None);
                    (bytes, result.map_err(|error| demand.record_native_io_failure(error))) })
            }
            fn release(&self, bytes: u64) {
                self.with_original_leaf(|demand| { if let Err(error) = demand.release_native_owned(bytes, &None) {
                    demand.record_native_io_failure(error); demand.poison_native_original_work();
                } });
            }
            fn admit_read(&self, maximum: usize) -> std::io::Result<()> {
                self.with_io(|demand| { demand.admit(crate::WorkCounters { source_bytes_read: maximum as u64, ..crate::WorkCounters::UNCHARGED })?;
                    demand.charge(crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED }) })
            }
            fn admit_write(&self, maximum: usize) -> std::io::Result<()> {
                self.with_io(|demand| { demand.admit(crate::WorkCounters { output_bytes: maximum as u64, ..crate::WorkCounters::UNCHARGED })?;
                    demand.charge(crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED }) })
            }
            fn reserve_cleanup_write(&self) -> std::io::Result<u64> { self.with_io(|demand| crate::native_host::NativeHostWork::reserve_cleanup_write(demand)) }
            fn cancel_cleanup_write(&self, reservation: u64) { self.with_original_leaf(|demand| crate::native_host::NativeHostWork::cancel_cleanup_write(demand, reservation)); }
            fn record_cleanup_write(&self, reservation: u64) -> std::io::Result<()> { self.with_io(|demand| crate::native_host::NativeHostWork::attempt_cleanup_write(demand, reservation, || ()).1) }
        }
    };
}
