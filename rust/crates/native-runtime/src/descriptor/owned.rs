use super::FileLease;
use std::{future::Future, io};

// Fields drop in order, including while unwinding a failed worker.
struct Owned<C> {
    lease: FileLease,
    custody: C,
}

impl FileLease {
    /// Acquires a lease and runs owned I/O on the shared persistence service.
    ///
    /// Both closures retain the caller's original buffers and admitted work.
    /// No descriptor, admission, write or flush is added. Once submitted, the
    /// service retains the lease and custody through terminal I/O even if the
    /// caller drops the result. Acquisition and operation errors are returned
    /// unchanged; loss of the completion producer is an outer I/O error.
    pub fn submit<C, T, E>(
        acquire: impl FnOnce() -> Result<(Self, C), E> + Send + 'static,
        operation: impl FnOnce(&Self, &mut C) -> Result<T, E> + Send + 'static,
    ) -> impl Future<Output = io::Result<Result<T, E>>> + Send
    where
        C: Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        crate::spawn_persistence(move || {
            let (lease, custody) = acquire()?;
            let mut owned = Owned { lease, custody };
            operation(&owned.lease, &mut owned.custody)
        })
    }
}
