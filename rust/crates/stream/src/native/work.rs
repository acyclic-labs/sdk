//! Original native provider work, shared by memory and durable adapters.
use crate::StreamError;
use acyclic_native_runtime::{
    NativeByteReservation, NativeOperationKind, NativeOperationReservation, NativeReadMeter,
    ReadAllocation,
};
use std::sync::Arc;

type WriteReservation = (
    NativeOperationReservation<Arc<dyn NativeReadMeter>>,
    NativeByteReservation<Arc<dyn NativeReadMeter>>,
);

pub(crate) struct Work {
    pub(crate) meter: Arc<dyn NativeReadMeter>,
    error: std::sync::Mutex<Option<std::io::Error>>,
    write: std::sync::Mutex<Option<WriteReservation>>,
    #[cfg(feature = "local")]
    durability: std::sync::Mutex<Option<NativeOperationReservation<Arc<dyn NativeReadMeter>>>>,
    _allocation: ReadAllocation,
}

impl Work {
    pub(crate) fn start<'a, T, E, F>(
        meter: Arc<dyn NativeReadMeter>,
        operation: impl FnOnce(Arc<Self>) -> F,
    ) -> crate::NativeProviderFuture<'a, T, E>
    where
        F: std::future::Future<Output = crate::NativeStreamResult<T, E>> + Send + 'a,
    {
        let work = match Self::new(meter) {
            Ok(work) => work,
            Err(error) => return crate::NativeProviderFuture::ready(Err(error)),
        };
        crate::NativeProviderFuture::new(&Arc::clone(&work.meter), operation(work))
    }

    pub(crate) fn new(meter: Arc<dyn NativeReadMeter>) -> std::io::Result<Arc<Self>> {
        let allocation = ReadAllocation::new(
            &meter,
            acyclic_native_runtime::arc_allocation_bytes::<Self>(),
        )?;
        Ok(Arc::new(Self {
            meter,
            error: std::sync::Mutex::new(None),
            write: std::sync::Mutex::new(None),
            #[cfg(feature = "local")]
            durability: std::sync::Mutex::new(None),
            _allocation: allocation,
        }))
    }

    pub(crate) fn record<T>(&self, result: std::io::Result<T>) -> Result<T, StreamError> {
        result.map_err(|error| {
            self.error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_or_insert(error);
            StreamError::Unavailable
        })
    }

    pub(crate) fn reserve_write(&self, bytes: usize) -> Result<(), StreamError> {
        let operation = self.record(NativeOperationReservation::new(
            Arc::clone(&self.meter),
            NativeOperationKind::Write,
            1,
        ))?;
        let bytes = self.record(NativeByteReservation::new(
            Arc::clone(&self.meter),
            NativeOperationKind::Write,
            bytes,
        ))?;
        *self
            .write
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((operation, bytes));
        Ok(())
    }

    /// Releases an unspent write reservation without publishing.
    pub(crate) fn release_write(&self) {
        self.write
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    pub(crate) fn publish<T>(
        &self,
        bytes: usize,
        operation: impl FnOnce() -> Result<T, StreamError>,
    ) -> Result<T, StreamError> {
        let mut allowance = self
            .write
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let (result, observed) = match &mut allowance {
            Some((write, _)) => write.attempt(operation),
            None => (operation(), Ok(())),
        };
        let transferred = match &mut allowance {
            Some((_, transfer)) if result.is_ok() && bytes != 0 => transfer.record(bytes),
            _ => Ok(()),
        };
        self.record(observed)?;
        self.record(transferred)?;
        result
    }

    #[cfg(feature = "local")]
    pub(crate) fn reserve_durability(&self, count: u64) -> Result<(), StreamError> {
        self.record(self.prepare_durability(count))
    }
    #[cfg(feature = "local")]
    pub(crate) fn prepare_durability(&self, count: u64) -> std::io::Result<()> {
        let ticket = NativeOperationReservation::new(
            Arc::clone(&self.meter),
            NativeOperationKind::Durability,
            count,
        )?;
        *self
            .durability
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ticket);
        Ok(())
    }
    #[cfg(feature = "local")]
    pub(crate) fn sync<T>(
        &self,
        operation: impl FnOnce() -> std::io::Result<T>,
    ) -> std::io::Result<T> {
        let mut ticket = self
            .durability
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (physical, observed) = ticket
            .as_mut()
            .ok_or(std::io::ErrorKind::InvalidInput)?
            .attempt(operation);
        let _ = self.record(observed);
        match physical {
            Ok(value) => Ok(value),
            Err(error) => {
                let _ = self.record::<()>(Err(error));
                Err(std::io::ErrorKind::Other.into())
            }
        }
    }

    pub(crate) fn finish<T, E>(
        &self,
        result: Result<(T, Option<ReadAllocation>), E>,
    ) -> crate::NativeStreamResult<T, E> {
        if let Some(error) = self
            .error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            return Err(error);
        }
        Ok(result.map(|(value, allocation)| {
            crate::NativeStreamOutput::new(
                value,
                allocation.unwrap_or_else(|| ReadAllocation::empty(&self.meter)),
            )
        }))
    }
}
