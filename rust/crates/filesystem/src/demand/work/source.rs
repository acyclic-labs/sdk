use super::*;
use std::io;

/// Borrow of the original native invocation. Construction is restricted to the
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Copy)]
pub struct NativeSourceWork<'a> {
    pub(crate) original: &'a std::sync::Arc<NativeDemandReadMeter>,
    pub(in crate::demand) context: Option<&'a crate::NativeMaterializationWork>,
    pub(in crate::demand) native_original: bool,
    pub(in crate::demand) operation_stop: Option<&'a std::sync::atomic::AtomicBool>,
}

#[cfg(not(target_arch = "wasm32"))]
impl<'a> NativeSourceWork<'a> {
    pub(crate) fn new(original: &'a std::sync::Arc<NativeDemandReadMeter>) -> Self {
            Self {
                original, context: None,
                native_original: true, operation_stop: None,
            }
        }

    pub(crate) fn ordinary(original: &'a std::sync::Arc<NativeDemandReadMeter>) -> Self {
            Self { native_original: false, ..Self::new(original) }
        }

    /// Borrows the caller's original native accounting context.
        pub fn from_native_materialization_work(work: &'a crate::materializer::NativeMaterializationWork) -> Self {
            Self { original: work.native_read_meter(), context: Some(work),
                native_original: true, operation_stop: None }
        }

    pub(crate) fn owned(&self) -> crate::NativeMaterializationWork {
        self.context.map_or_else(|| crate::NativeMaterializationWork::from_meter(Arc::clone(self.original)), Clone::clone)
    }

    pub(crate) fn materialization_work(&self) -> Result<crate::materializer::NativeMaterializationWork, DemandError> {
            crate::native_host::NativeHostWork::check(self)?;
            drop(self.original.0.lock().map_err(|_| DemandError::OriginalWorkPoisoned)?);
            Ok(self.owned())
        }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) trait SourceDemandReadWork:
    crate::native_host::NativeHostWork<Error = DemandError, Allocation = u64, CleanupWrite = u64>
{
    fn with_native_read_meter<T>(
        &mut self, operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
    ) -> T;
    fn with_original_leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T;
    fn native_source_work(&self) -> Option<NativeSourceWork<'_>> { None }
    fn with_ordinary_work<T>(&mut self, _operation: impl FnOnce(&mut DemandWork) -> T) -> Option<T> { None }
    fn original_meter(&self) -> Option<&Arc<NativeDemandReadMeter>> { self.native_source_work().map(|work| work.original) }
    fn reserve_source_copy(
        &mut self, bytes: u64,
    ) -> Result<Option<crate::materializer::NativeWorkReservation>, DemandError> {
        self.check()?;
        if let Some(source) = self.native_source_work() {
            return source.materialization_work()?.reserve_work(crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED })
                .map(Some).map_err(crate::native_name::original_name_error);
        }
        self.admit(WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED })?;
        Ok(None)
    }
    fn charge(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.check()?;
        self.with_original_leaf(|work| work.charge(delta))
    }
    fn claim_allocation(&mut self, bytes: u64, operations: u64) -> Result<(), DemandError> {
        self.check()?;
        self.with_original_leaf(|work| work.claim_allocation(bytes, operations))
    }
    fn allocate(&mut self, bytes: u64) -> Result<(), DemandError> {
        self.check()?;
        self.with_original_leaf(|work| work.allocate(bytes))
    }
    fn allocate_for(&mut self, bytes: u64, delta: WorkCounters) -> Result<(), DemandError> {
        self.check()?;
        self.with_original_leaf(|work| work.allocate_for(bytes, delta))
    }
    fn remaining(&mut self) -> Result<WorkBudget, DemandError> {
        self.with_original_leaf(|work| work.remaining())
    }
    fn adopt(&mut self, bytes: u64) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.adopt(bytes))
    }
    fn merge(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.merge(delta))
    }
    fn is_native_original(&self) -> bool { self.native_source_work().is_some() }
}

#[cfg(not(target_arch = "wasm32"))]
impl SourceDemandReadWork for DemandWork {
    fn with_ordinary_work<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> Option<T> { Some(operation(self)) }
    fn with_native_read_meter<T>(
        &mut self, operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
    ) -> T {
        let meter = NativeDemandReadMeter(std::sync::Mutex::new(self));
        operation(&meter)
    }
    fn with_original_leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
        operation(self)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl crate::native_host::NativeHostWork for NativeSourceWork<'_> {
    type Error = DemandError;
    type Allocation = u64;
    type CleanupWrite = u64;
    fn io_error_kind(error: &Self::Error) -> Option<std::io::ErrorKind> {
        <DemandWork as crate::native_host::NativeHostWork>::io_error_kind(error)
    }
    fn invalid_request() -> Self::Error { DemandError::InvalidRequest }
    fn check(&self) -> Result<(), Self::Error> {
        self.original.with_original_leaf(|work| work.check())?;
        if let Some(context) = self.context {
            if let Some(invocation) = &context.invocation { invocation.check()?; }
            if context.capture_origin.as_ref().is_some_and(|origin| origin.strong_count() == 0) {
                return Err(DemandError::NativeWorkUnavailable);
            }
            if let Some(resource) = &context.resource { resource.admit(WorkCounters::UNCHARGED, 0)?; }
        }
        if self.operation_stop.is_some_and(|stop| stop.load(std::sync::atomic::Ordering::Acquire)) {
            return Err(DemandError::Cancelled);
        }
        Ok(())
    }
    fn admit(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        self.check()?;
        self.with_original_leaf(|work| work.admit(delta))
    }
    fn record(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        self.with_original_leaf(|work| work.record(delta))
    }
    fn started_io(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        self.check()?;
        self.with_original_leaf(|work| work.charge(delta))
    }
    fn claim(&mut self, bytes: u64) -> Result<u64, Self::Error> {
        self.check()?;
        self.with_original_leaf(|work| { work.allocate(bytes)?; Ok(bytes) })
    }
    fn reconcile_allocation(&mut self, allocation: &mut u64, actual_bytes: u64) -> Result<(), Self::Error> {
        self.with_original_leaf(|work| work.reconcile_native_allocation(allocation, actual_bytes))
    }
    fn reserve_opaque_decode(&mut self, maximum: u64) -> Result<u64, Self::Error> {
        self.check()?;
        self.with_original_leaf(|work| crate::native_host::NativeHostWork::reserve_opaque_decode(work, maximum))
    }
    fn allocation_failed(error: std::collections::TryReserveError) -> Self::Error {
        DemandError::Allocation(error)
    }
    fn release(&mut self, bytes: u64) -> Result<(), Self::Error> {
        self.with_original_leaf(|work| work.release(bytes))
    }
    fn reserve_cleanup_write(&mut self) -> Result<u64, Self::Error> {
        self.with_original_leaf(|work| crate::native_host::NativeHostWork::reserve_cleanup_write(work))
    }
    fn cancel_cleanup_write(&mut self, reservation: u64) {
        self.with_original_leaf(|work| crate::native_host::NativeHostWork::cancel_cleanup_write(work, reservation));
    }
    fn attempt_cleanup_write<T>(&mut self, reservation: u64, operation: impl FnOnce() -> T)
        -> (T, Result<(), Self::Error>) {
        let recorded = self.with_original_leaf(|work| {
            let (_, recorded) = crate::native_host::NativeHostWork::attempt_cleanup_write(work, reservation, || ());
            recorded
        });
        (operation(), recorded)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl SourceDemandReadWork for NativeSourceWork<'_> {
    fn native_source_work(&self) -> Option<NativeSourceWork<'_>> {
        self.native_original.then_some(*self)
    }
    fn original_meter(&self) -> Option<&Arc<NativeDemandReadMeter>> { Some(self.original) }
    fn with_native_read_meter<T>(
        &mut self, operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
    ) -> T {
        let original: &dyn acyclic_native_runtime::NativeReadMeter = {
            #[cfg(feature = "native-mount")]
            if let Some(execution) = self.context.and_then(|context| context.execution.as_ref()) {
                if let Some(stop) = self.operation_stop {
                    return operation(&ScopedSourceReadMeter {
                        original: execution.as_ref(), failure_root: self.original.as_ref(), stop,
                    });
                }
                return operation(execution.as_ref());
            }
            let resource = self.context.and_then(|context| context.resource.as_ref());
            match (self.context.and_then(|context| context.invocation.as_ref()), resource) {
                (Some(invocation), _) if invocation.owns_resource(resource) => invocation.as_ref(),
                (_, Some(resource)) => resource.as_ref(),
                (_, None) => self.original.as_ref(),
            }
        };
        if let Some(stop) = self.operation_stop {
            operation(&ScopedSourceReadMeter { original, failure_root: self.original.as_ref(), stop })
        } else { operation(original) }
    }
    fn with_original_leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
        if let Some(context) = self.context { return context.with_original_leaf(operation); }
        self.original.with_original_leaf(|work| work.with_resource_scope(None, |work|
            work.with_capture(None, |work| work.with_invocation_scope(None, operation))))
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::demand) struct ScopedSourceReadMeter<'a> {
    pub(in crate::demand) original: &'a dyn acyclic_native_runtime::NativeReadMeter,
    pub(in crate::demand) failure_root: &'a NativeDemandReadMeter,
    pub(in crate::demand) stop: &'a std::sync::atomic::AtomicBool,
}

#[cfg(not(target_arch = "wasm32"))]
macro_rules! forward_source_reads {
    ($($mode:ident $name:ident($($arg:ident: $ty:ty),*) -> $result:ty;)*) => {
        $(fn $name(&self, $($arg: $ty),*) -> $result {
            forward_source_reads!(@ $mode self);
            self.original.$name($($arg),*)
        })*
    };
    (@ admit $meter:ident) => { $meter.check()?; };
    (@ complete $meter:ident) => {};
}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_native_runtime::NativeReadMeter for ScopedSourceReadMeter<'_> {
    fn check(&self) -> io::Result<()> {
        if self.stop.load(std::sync::atomic::Ordering::Acquire) {
            return Err(self.failure_root.with_original_leaf(|work| work.record_native_io_failure(DemandError::Cancelled)));
        }
        self.original.check()
    }

    forward_source_reads! {
        admit reserve_operations(kind: NativeOperationKind, count: u64) -> io::Result<()>;
        complete cancel_reserved_operations(kind: NativeOperationKind, count: u64) -> ();
        complete record_reserved_operation(kind: NativeOperationKind) -> io::Result<()>;
        admit admit_write_bytes(maximum: usize) -> io::Result<()>;
        admit reserve_bytes(kind: NativeOperationKind, maximum: u64) -> io::Result<()>;
        complete cancel_reserved_bytes(kind: NativeOperationKind, maximum: u64) -> ();
        complete record_reserved_bytes(kind: NativeOperationKind, maximum: u64, actual: u64) -> io::Result<()>;
        admit allocate(bytes: u64) -> io::Result<()>;
        complete allocation_failed(error: std::collections::TryReserveError) -> io::Error;
        admit reserve_allocation(bytes: u64) -> io::Result<()>;
        complete cancel_reserved_allocation(bytes: u64) -> ();
        complete record_reserved_allocation(bytes: u64) -> io::Result<()>;
        admit allocate_refcount_header() -> io::Result<()>;
        admit adopt(bytes: u64) -> io::Result<()>;
        complete record_completed_adoption(bytes: u64) -> (u64, io::Result<()>);
        admit admit_codec(encoded: u64, copied: u64, hashed: u64) -> io::Result<()>;
        complete record_codec(encoded: u64, copied: u64, hashed: u64) -> io::Result<()>;
        admit examine(items: u64, bytes_copied: u64) -> io::Result<()>;
        admit admit_examination(items: u64, bytes_copied: u64) -> io::Result<()>;
        complete record_examination(items: u64, bytes_copied: u64) -> io::Result<()>;
        complete release(bytes: u64) -> ();
        admit admit_bytes(maximum: usize) -> io::Result<()>;
        admit admit_read(maximum: usize) -> io::Result<()>;
        admit admit_operations(additional: u64) -> io::Result<()>;
        complete record_operations(completed: u64) -> io::Result<()>;
        complete record_read(bytes: usize) -> io::Result<()>;
        admit admit_write(maximum: usize) -> io::Result<()>;
        complete record_write(bytes: usize) -> io::Result<()>;
        admit admit_write_operations(additional: u64) -> io::Result<()>;
        complete record_write_operations(completed: u64) -> io::Result<()>;
        admit admit_durability() -> io::Result<()>;
        admit reserve_cleanup_write() -> io::Result<u64>;
        complete cancel_cleanup_write(reservation: u64) -> ();
        complete record_cleanup_write(reservation: u64) -> io::Result<()>;
    }

    fn finish_read(&self, _requested: usize, _completed: usize) -> io::Result<()> {
        let completed = self.original.finish_read(_requested, _completed);
        completed.and(self.check())
    }

}
