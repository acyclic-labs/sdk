use super::*;

pub(super) const READ: WorkCounters = WorkCounters {
    backend_read_operations: 1,
    ..WorkCounters::UNCHARGED
};
pub(super) const WRITE: WorkCounters = WorkCounters {
    backend_write_operations: 1,
    ..WorkCounters::UNCHARGED
};
pub(super) const ITEM: WorkCounters = WorkCounters {
    items_examined: 1,
    ..WorkCounters::UNCHARGED
};

pub(super) fn buffer<T, W: NativeHostWork>(
    capacity: usize,
    work: &mut W,
) -> Result<(Vec<T>, W::Allocation), W::Error> {
    let bytes = capacity
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(crate::WorkError::Overflow)?;
    let mut allocation = work.claim(bytes as u64)?;
    let mut values = Vec::new();
    let reserved = values
        .try_reserve_exact(capacity)
        .map_err(W::allocation_failed);
    let actual = values
        .capacity()
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(crate::WorkError::Overflow)?;
    let reconciled = work.reconcile_allocation(&mut allocation, actual as u64);
    if let Err(error) = reserved.and(reconciled) {
        drop(values);
        work.release(allocation)?;
        return Err(error);
    }
    Ok((values, allocation))
}
pub(crate) trait NativeHostWork {
    type Error: std::error::Error + Send + Sync + 'static + From<io::Error> + From<crate::WorkError>;
    type Allocation;
    type CleanupWrite;
    fn io_error_kind(error: &Self::Error) -> Option<io::ErrorKind>;
    fn allocation_failed(error: std::collections::TryReserveError) -> Self::Error;
    fn invalid_request() -> Self::Error {
        io::Error::new(io::ErrorKind::InvalidInput, "invalid native source request").into()
    }
    fn check(&self) -> Result<(), Self::Error>;
    fn admit(&mut self, delta: WorkCounters) -> Result<(), Self::Error>;
    fn record(&mut self, delta: WorkCounters) -> Result<(), Self::Error>;
    fn started_io(&mut self, delta: WorkCounters) -> Result<(), Self::Error>;
    fn claim(&mut self, bytes: u64) -> Result<Self::Allocation, Self::Error>;
    fn reserve_opaque_decode(&mut self, logical_max: u64) -> Result<Self::Allocation, Self::Error>;
    fn reconcile_allocation(
        &mut self,
        allocation: &mut Self::Allocation,
        actual: u64,
    ) -> Result<(), Self::Error>;
    fn release(&mut self, allocation: Self::Allocation) -> Result<(), Self::Error>;
    fn attempt_io<T>(
        &mut self,
        delta: WorkCounters,
        operation: impl FnOnce() -> io::Result<T>,
    ) -> Result<T, Self::Error> {
        self.started_io(delta)?;
        operation().map_err(Into::into)
    }
    fn reserve_cleanup_write(&mut self) -> Result<Self::CleanupWrite, Self::Error>;
    fn cancel_cleanup_write(&mut self, reservation: Self::CleanupWrite);
    fn attempt_cleanup_write<T>(
        &mut self,
        reservation: Self::CleanupWrite,
        operation: impl FnOnce() -> T,
    ) -> (T, Result<(), Self::Error>);
}

impl NativeHostWork for DemandWork {
    type Error = DemandError;
    type Allocation = u64;
    type CleanupWrite = u64;
    fn io_error_kind(error: &Self::Error) -> Option<io::ErrorKind> {
        match error {
            DemandError::Io(error) => Some(error.kind()),
            _ => None,
        }
    }
    fn invalid_request() -> Self::Error {
        DemandError::InvalidRequest
    }
    fn allocation_failed(error: std::collections::TryReserveError) -> Self::Error {
        DemandError::Allocation(error)
    }
    fn check(&self) -> Result<(), Self::Error> {
        DemandWork::check(self)
    }
    fn admit(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        self.check()?;
        Ok(self.work.admit(&delta, &self.budget)?)
    }
    fn record(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        DemandWork::record(self, delta)
    }
    fn started_io(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        self.charge(delta)
    }
    fn claim(&mut self, bytes: u64) -> Result<u64, Self::Error> {
        self.allocate(bytes)?;
        Ok(bytes)
    }
    fn reserve_opaque_decode(&mut self, logical_max: u64) -> Result<Self::Allocation, Self::Error> {
        self.adopt(logical_max)?;
        Ok(logical_max)
    }
    fn reconcile_allocation(
        &mut self,
        allocation: &mut Self::Allocation,
        actual: u64,
    ) -> Result<(), Self::Error> {
        self.reconcile_native_allocation(allocation, actual)
    }
    fn release(&mut self, bytes: u64) -> Result<(), Self::Error> {
        DemandWork::release(self, bytes)
    }
    fn reserve_cleanup_write(&mut self) -> Result<u64, Self::Error> {
        self.reserve(
            crate::materializer::NativeResourceCredit::WriteOperations,
            1,
        )
        .map(|()| 1)
    }
    fn cancel_cleanup_write(&mut self, reservation: u64) {
        self.refund(
            crate::materializer::NativeResourceCredit::WriteOperations,
            reservation,
        )
        .expect("original reservation is balanced");
    }
    fn attempt_cleanup_write<T>(
        &mut self,
        reservation: u64,
        operation: impl FnOnce() -> T,
    ) -> (T, Result<(), Self::Error>) {
        let recorded = self.complete(
            crate::materializer::NativeResourceCredit::WriteOperations,
            reservation,
            1,
        );
        (operation(), recorded)
    }
}

impl NativeHostWork for &NativeMaterializationWork {
    type Error = NativeTreeMaterializationError;
    type Allocation = NativeAllocation;
    type CleanupWrite = NativeCleanupWrite;
    fn io_error_kind(error: &Self::Error) -> Option<io::ErrorKind> {
        match error {
            NativeTreeMaterializationError::Io(error) => Some(error.kind()),
            _ => None,
        }
    }
    fn allocation_failed(error: std::collections::TryReserveError) -> Self::Error {
        NativeTreeMaterializationError::AllocationFailed(error)
    }
    fn invalid_request() -> Self::Error {
        NativeTreeMaterializationError::InvalidLayout
    }
    fn check(&self) -> Result<(), Self::Error> {
        NativeMaterializationWork::admit(self, WorkCounters::UNCHARGED)
    }
    fn admit(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        NativeMaterializationWork::admit(self, delta)
    }
    fn record(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        self.record_admitted(delta)
    }
    fn started_io(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        self.charge(delta)
    }
    fn claim(&mut self, bytes: u64) -> Result<Self::Allocation, Self::Error> {
        NativeMaterializationWork::claim(
            self,
            bytes,
            u64::from(bytes != 0),
            WorkCounters::UNCHARGED,
        )
    }
    fn reserve_opaque_decode(&mut self, logical_max: u64) -> Result<Self::Allocation, Self::Error> {
        NativeMaterializationWork::claim(self, logical_max, 0, WorkCounters::UNCHARGED)
    }
    fn reconcile_allocation(
        &mut self,
        allocation: &mut Self::Allocation,
        actual: u64,
    ) -> Result<(), Self::Error> {
        allocation.reconcile_capacity(actual)
    }
    fn release(&mut self, allocation: Self::Allocation) -> Result<(), Self::Error> {
        drop(allocation);
        Ok(())
    }
    fn reserve_cleanup_write(&mut self) -> Result<Self::CleanupWrite, Self::Error> {
        NativeMaterializationWork::reserve_cleanup_write(self)
    }
    fn cancel_cleanup_write(&mut self, reservation: Self::CleanupWrite) {
        drop(reservation);
    }
    fn attempt_cleanup_write<T>(
        &mut self,
        reservation: Self::CleanupWrite,
        operation: impl FnOnce() -> T,
    ) -> (T, Result<(), Self::Error>) {
        reservation.attempt(operation)
    }
}

pub(crate) struct NativeOwned<T> {
    pub(crate) data: T,
    pub(crate) _allocation: NativeAllocation,
}

impl<T> std::ops::Deref for NativeOwned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.data
    }
}

pub(crate) fn native_buffer<T>(
    capacity: usize,
    mut work: &NativeMaterializationWork,
) -> Result<NativeOwned<Vec<T>>, NativeTreeMaterializationError> {
    let (data, allocation) = buffer(capacity, &mut work)?;
    Ok(NativeOwned {
        data,
        _allocation: allocation,
    })
}

pub(crate) fn clone_path<W: NativeHostWork>(
    path: &Path,
    work: &mut W,
) -> Result<(std::path::PathBuf, W::Allocation), W::Error> {
    let bytes = path.as_os_str().len();
    let delta = WorkCounters {
        bytes_copied: bytes as u64,
        ..WorkCounters::UNCHARGED
    };
    let mut allocation = work.claim(bytes as u64)?;
    let mut name = OsString::new();
    let result = (|| {
        let reserved = name.try_reserve_exact(bytes);
        let reconciled = work.reconcile_allocation(&mut allocation, name.capacity() as u64);
        reserved.map_err(W::allocation_failed)?;
        reconciled?;
        work.started_io(delta)?;
        name.push(path.as_os_str());
        Ok(())
    })();
    if let Err(error) = result {
        drop(name);
        work.release(allocation)?;
        return Err(error);
    }
    Ok((name.into(), allocation))
}

/// Creates unbounded native work.
pub(crate) fn ordinary_work() -> DemandWork {
    static CANCELLATION: std::sync::LazyLock<crate::CancellationToken> =
        std::sync::LazyLock::new(crate::CancellationToken::new);
    DemandWork::new(WorkBudget::UNBOUNDED, &CANCELLATION)
}

/// Maps native demand failures to I/O errors.
pub(crate) fn ordinary_error(error: DemandError) -> io::Error {
    match error {
        DemandError::Io(error) => error,
        error => io::Error::other(error),
    }
}
