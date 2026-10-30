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
    pub fn submit<C, T, E, A, F>(
        medium: &'static str,
        kind: &'static str,
        key: crate::PersistenceKey,
        acquire: A,
        operation: F,
    ) -> impl Future<Output = io::Result<Result<T, E>>> + Send + use<C, T, E, A, F>
    where
        A: FnOnce() -> Result<(Self, C), E> + Send + 'static,
        F: FnOnce(&Self, &mut C) -> Result<T, E> + Send + 'static,
        C: Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        crate::spawn_persistence(medium, kind, key, move || {
            let (lease, custody) = acquire()?;
            let mut owned = Owned { lease, custody };
            operation(&owned.lease, &mut owned.custody)
        })
    }
}
