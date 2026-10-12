use super::*;

impl<T: AsMut<crate::demand::DemandWork>> NativeDemandReadMeter<T> {
    pub(in crate::demand) fn with_io<R>(
            &self, operation: impl FnOnce(&mut T) -> Result<R, crate::DemandError>,
        ) -> std::io::Result<R> {
            let mut held = self.lock();
            operation(&mut held).map_err(|error| held.as_mut().record_native_io_failure(error))
        }

    pub(crate) fn relay_io_failure(&self, error: crate::DemandError) -> std::io::Error {
            self.lock().as_mut().record_native_io_failure(error)
        }

    pub(crate) fn take_native_io_failure(&self) -> Option<crate::DemandError> {
            self.lock().as_mut().take_native_io_failure()
        }

    pub(in crate::demand) fn admit(&self, delta: WorkCounters) -> std::io::Result<()> {
            self.with_io(|held| held.as_mut().admit(delta))
        }

    pub(in crate::demand) fn record(&self, delta: WorkCounters) -> std::io::Result<()> {
            self.with_io(|held| held.as_mut().record(delta))
        }

    pub(in crate::demand) fn charge(&self, delta: WorkCounters) -> std::io::Result<()> {
            self.with_io(|held| held.as_mut().charge(delta))
        }
}

#[cfg(all(feature = "native-watch", target_os = "macos"))]
pub(crate) fn native_demand_read_error(error: crate::DemandError) -> std::io::Error {
    if matches!(error, crate::DemandError::Cancelled) {
        std::io::Error::new(std::io::ErrorKind::Interrupted, error)
    } else {
        std::io::Error::other(error)
    }
}

macro_rules! meter_counters {
    ($($name:ident($($arg:ident: $ty:ty),*) => $mode:ident { $($field:ident: $value:expr),* };)*) => {
        $(fn $name(&self, $($arg: $ty),*) -> std::io::Result<()> {
            self.$mode(WorkCounters { $($field: $value),*, ..WorkCounters::UNCHARGED })
        })*
    };
}

impl<T: AsMut<crate::demand::DemandWork> + Send> acyclic_native_runtime::NativeReadMeter for NativeDemandReadMeter<T> {
    meter_counters! {
        allocate_refcount_header() => charge { allocation_operations: 1 };
        admit_examination(items: u64, bytes_copied: u64) => admit { items_examined: items, bytes_copied: bytes_copied };
        record_examination(items: u64, bytes_copied: u64) => record { items_examined: items, bytes_copied: bytes_copied };
        examine(items: u64, bytes_copied: u64) => charge { items_examined: items, bytes_copied: bytes_copied };
        admit_bytes(maximum: usize) => admit { source_bytes_read: maximum as u64 };
        admit_operations(additional: u64) => admit { backend_read_operations: additional };
        record_operations(completed: u64) => record { backend_read_operations: completed };
        record_read(bytes: usize) => record { source_bytes_read: bytes as u64 };
        record_write(bytes: usize) => record { output_bytes: bytes as u64 };
        admit_write_operations(additional: u64) => admit { backend_write_operations: additional };
        record_write_operations(completed: u64) => record { backend_write_operations: completed };
        admit_durability() => charge { durability_operations: 1 };
    }
    fn reserve_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) -> std::io::Result<()> {
        self.with_io(|held| held.as_mut().reserve(crate::demand::DemandWork::operation_credit(kind), count))
    }
    fn cancel_reserved_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) {
        self.lock().as_mut().refund(crate::demand::DemandWork::operation_credit(kind), count).expect("original reservation is balanced");
    }
    fn record_reserved_operation(&self, kind: acyclic_native_runtime::NativeOperationKind) -> std::io::Result<()> {
        self.with_io(|held| held.as_mut().complete(crate::demand::DemandWork::operation_credit(kind), 1, 1))
    }
    fn admit_write_bytes(&self, maximum: usize) -> std::io::Result<()> {
        self.admit(WorkCounters { output_bytes: maximum as u64, ..WorkCounters::UNCHARGED })
    }
    fn reserve_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64) -> std::io::Result<()> {
        let credit = crate::demand::DemandWork::byte_credit(kind)?;
        self.with_io(|held| held.as_mut().reserve(credit, maximum))
    }
    fn cancel_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64) {
        self.lock().as_mut().refund(crate::demand::DemandWork::byte_credit(kind).expect("durability has no bytes"), maximum).expect("original reservation is balanced");
    }
    fn record_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64, actual: u64) -> std::io::Result<()> {
        let credit = crate::demand::DemandWork::byte_credit(kind)?;
        self.with_io(|held| held.as_mut().complete(credit, maximum, actual))
    }

    fn check(&self) -> std::io::Result<()> {
        self.with_io(|held| held.as_mut().check())
    }

    fn allocation_failed(&self, error: std::collections::TryReserveError) -> std::io::Error {
        self.relay_io_failure(crate::DemandError::Allocation(error))
    }

    fn finish_read(&self, _requested: usize, _completed: usize) -> std::io::Result<()> {
        self.with_io(|held| held.as_mut().check())
    }

    fn allocate(&self, bytes: u64) -> std::io::Result<()> {
        self.with_io(|held| held.as_mut().claim_native_owned(bytes, u64::from(bytes != 0), crate::WorkCounters::UNCHARGED, &None))
    }

    fn reserve_allocation(&self, bytes: u64) -> std::io::Result<()> {
        self.with_io(|held| held.as_mut().reserve_native_allocation(bytes))
    }

    fn cancel_reserved_allocation(&self, bytes: u64) {
        let mut held = self.lock();
        let work = held.as_mut();
        if let Err(error) = work.cancel_reserved_native_allocation(bytes) {
            work.record_native_io_failure(error);
            work.poison_native_original_work();
        }
    }

    fn record_reserved_allocation(&self, bytes: u64) -> std::io::Result<()> {
        self.with_io(|held| held.as_mut().record_reserved_native_allocation(bytes))
    }

    fn adopt(&self, bytes: u64) -> std::io::Result<()> {
        self.with_io(|held| held.as_mut().claim_native_owned(bytes, 0, crate::WorkCounters::UNCHARGED, &None))
    }

    fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
        let mut held = self.lock();
        let work = held.as_mut();
        let (claimed, result) = work.record_native_owned_completed(bytes, &None);
        (claimed, result.map_err(|error| work.record_native_io_failure(error)))
    }

    fn release(&self, bytes: u64) {
        let mut held = self.lock();
        let work = held.as_mut();
        if let Err(error) = work.release_native_owned(bytes, &None) {
            work.record_native_io_failure(error);
            work.poison_native_original_work();
        }
    }

    fn admit_read(&self, maximum: usize) -> std::io::Result<()> {
        self.admit_bytes(maximum)?;
        self.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })
    }

    fn admit_write(&self, maximum: usize) -> std::io::Result<()> {
        self.admit(WorkCounters { output_bytes: maximum as u64, ..WorkCounters::UNCHARGED })?;
        self.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED })
    }

    fn reserve_cleanup_write(&self) -> std::io::Result<u64> {
        self.with_io(|held| crate::native_host::NativeHostWork::reserve_cleanup_write(held.as_mut()))
    }

    fn cancel_cleanup_write(&self, reservation: u64) {
        crate::native_host::NativeHostWork::cancel_cleanup_write(self.lock().as_mut(), reservation);
    }

    fn record_cleanup_write(&self, reservation: u64) -> std::io::Result<()> {
        self.with_io(|held| crate::native_host::NativeHostWork::attempt_cleanup_write(
            held.as_mut(), reservation, || (),
        ).1)
    }
}

pub(crate) fn native_demand_error(error: std::io::Error) -> crate::DemandError {
    if error.get_ref().is_some_and(|error| error.is::<crate::DemandError>()) {
        *error.into_inner().expect("meter error contains its cause")
            .downcast::<crate::DemandError>().expect("meter cause retains its type")
    } else {
        crate::DemandError::Io(error)
    }
}
