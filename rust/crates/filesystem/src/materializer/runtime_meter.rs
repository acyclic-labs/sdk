use super::*;
use crate::demand::{DemandWork, NativeDemandReadMeter};
use acyclic_native_runtime::{NativeOperationKind, NativeReadMeter};

macro_rules! sdk_calls {
    ($($name:ident($($argument:ident: $kind:ty),*) -> $result:ty;)*) => {$(
        fn $name(&self, $($argument: $kind),*) -> $result {
            self.with_original_leaf(|state| NativeReadMeter::$name(&NativeDemandReadMeter(std::sync::Mutex::new(state)), $($argument),*))
        }
    )*};
}
macro_rules! scoped_meter {
    ($owner:ty $(, $allocation_owner:expr)?) => {
        impl $owner {
            fn io<T>(&self, operation: impl FnOnce(&mut DemandWork) -> Result<T, crate::DemandError>) -> std::io::Result<T> {
                self.with_original_leaf(|state| operation(state).map_err(|error| state.record_native_io_failure(error)))
            }
            fn cancel(&self, operation: impl FnOnce(&mut DemandWork) -> Result<(), crate::DemandError>) {
                self.with_original_leaf(|state| { if let Err(error) = operation(state) { state.record_native_io_failure(error); state.poison_native_original_work(); } });
            }
        }
        impl NativeReadMeter for $owner {
            $(fn allocation_owner(&self) -> Option<std::sync::Arc<dyn NativeReadMeter>> { ($allocation_owner)(self) })?
            sdk_calls! {
                check() -> std::io::Result<()>;
                reserve_operations(kind: NativeOperationKind, count: u64) -> std::io::Result<()>;
                record_reserved_operation(kind: NativeOperationKind) -> std::io::Result<()>;
                reserve_bytes(kind: NativeOperationKind, maximum: u64) -> std::io::Result<()>;
                record_reserved_bytes(kind: NativeOperationKind, maximum: u64, actual: u64) -> std::io::Result<()>;
                allocate(bytes: u64) -> std::io::Result<()>;
                reserve_allocation(bytes: u64) -> std::io::Result<()>;
                cancel_reserved_allocation(bytes: u64) -> ();
                record_reserved_allocation(bytes: u64) -> std::io::Result<()>;
                allocation_failed(error: std::collections::TryReserveError) -> std::io::Error;
                allocate_refcount_header() -> std::io::Result<()>;
                adopt(bytes: u64) -> std::io::Result<()>;
                record_completed_adoption(bytes: u64) -> (u64, std::io::Result<()>);
                release(bytes: u64) -> ();
                admit_bytes(maximum: usize) -> std::io::Result<()>;
                admit_write_bytes(maximum: usize) -> std::io::Result<()>;
                admit_read(maximum: usize) -> std::io::Result<()>;
                admit_write(maximum: usize) -> std::io::Result<()>;
                record_read(bytes: usize) -> std::io::Result<()>;
                record_write(bytes: usize) -> std::io::Result<()>;
                admit_durability() -> std::io::Result<()>;
                admit_examination(items: u64, copied: u64) -> std::io::Result<()>;
                record_examination(items: u64, copied: u64) -> std::io::Result<()>;
                examine(items: u64, copied: u64) -> std::io::Result<()>;
                reserve_cleanup_write() -> std::io::Result<u64>;
                cancel_cleanup_write(reservation: u64) -> ();
                record_cleanup_write(reservation: u64) -> std::io::Result<()>;
            }
            fn cancel_reserved_operations(&self, kind: NativeOperationKind, count: u64) {
                self.cancel(|state| state.refund(DemandWork::operation_credit(kind), count));
            }
            fn cancel_reserved_bytes(&self, kind: NativeOperationKind, bytes: u64) {
                self.cancel(|state| state.refund(DemandWork::byte_credit(kind)?, bytes));
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
scoped_meter!(NativeResourceAllowance);
scoped_meter!(NativeInvocationView);
#[cfg(feature = "native-mount")]
scoped_meter!(
    NativeSourceExecutionView,
    |view: &NativeSourceExecutionView| Some(view.metadata.work().runtime_meter())
);
