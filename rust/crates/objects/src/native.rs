//! Original-meter producers over the same engine paths as ordinary calls.
use crate::{
    Account, Blobs, ByteRange, Digest, Medium, MediumError, NativeObjectsCause, NativeObjectsError,
    Object, ObjectsBatch, ObjectsNativeFuture, ObjectsNativeRequest, ObjectsPutReceipt,
    Observation, Observed, Retained, RootId, native_future, native_input_meter,
};
use acyclic_native_runtime::{NativeOperationKind, NativeReadMeter, ReadAllocation};
use acyclic_stream::OwnedBytes;
use std::ops::Range;
use std::sync::{Arc, Mutex, PoisonError};

type Meter = Arc<dyn NativeReadMeter>;

/// The caller's meter over every medium operation of one native call, batch
/// members included: each physical operation is admitted before dispatch, and
/// returned payload backing keeps its charge through the final byte alias.
/// Temporary engine allocations remain charged for the call.
/// Canonical payload bytes belong to the caller's logical-object accounting;
/// medium envelopes, index keys and listings are neither source nor output bytes.
pub(crate) struct Metered<'a, M> {
    inner: &'a M,
    meter: Meter,
    held: Mutex<ReadAllocation>,
    refusal: Mutex<Option<std::io::Error>>,
    /// Writes withheld for this call.
    reserved: Mutex<u64>,
}

impl<'a, M> Metered<'a, M> {
    fn new(inner: &'a M, meter: Meter) -> Self {
        Self {
            held: Mutex::new(ReadAllocation::empty(&meter)),
            inner,
            meter,
            refusal: Mutex::new(None),
            reserved: Mutex::new(0),
        }
    }

    /// Admits one write: from the reservation while it lasts, else from the budget. A reserved
    /// write still honors cancellation, except a removal: one is only ever
    /// the cleanup of a write that did not commit, so it always proceeds.
    fn admit_write(&self, cleanup: bool) -> Result<(), MediumError> {
        let mut reserved = self.reserved.lock().unwrap_or_else(PoisonError::into_inner);
        if *reserved == 0 {
            drop(reserved);
            return self.charge(self.meter.admit_write(0));
        }
        if !cleanup {
            self.charge(self.meter.check())?;
        }
        *reserved -= 1;
        drop(reserved);
        self.charge(
            self.meter
                .record_reserved_operation(NativeOperationKind::Write),
        )
    }

    /// Keeps the meter's own refusal for the caller; the engine sees a failure.
    fn charge<T>(&self, result: std::io::Result<T>) -> Result<T, MediumError> {
        result.map_err(|error| {
            let relay = std::io::Error::from(error.kind());
            self.refusal
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get_or_insert(error);
            MediumError::Failed(relay)
        })
    }

    fn hold(&self, bytes: usize) -> Result<(), MediumError> {
        let grown = self
            .held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .grow(bytes);
        self.charge(grown)
    }

    fn retain_read(&self, bytes: OwnedBytes) -> Result<OwnedBytes, MediumError>
    where
        M: Medium,
    {
        let allocated = self.inner.read_allocation_bytes(&bytes);
        if allocated == 0 {
            return Ok(bytes);
        }
        let allocation = self.charge(ReadAllocation::new(&self.meter, allocated))?;
        self.charge(bytes.with_guard(allocation, Arc::clone(&self.meter)))
    }

    /// The meter's refusal behind `error`, else the engine's error.
    fn cause(&self, error: crate::Error) -> NativeObjectsError {
        match self
            .refusal
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            Some(refusal) => refusal.into(),
            None => NativeObjectsError::from(NativeObjectsCause::Service(error)),
        }
    }
}

#[async_trait::async_trait]
impl<M: Medium> Medium for Metered<'_, M> {
    async fn create(&self, key: &str, bytes: OwnedBytes) -> Result<bool, MediumError> {
        self.admit_write(false)?;
        self.inner.create(key, bytes).await
    }

    async fn read(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<OwnedBytes>, MediumError> {
        self.charge(self.meter.admit_read(0))?;
        let read = self.inner.read(key, range).await?;
        read.map(|bytes| self.retain_read(bytes)).transpose()
    }

    async fn remove(&self, key: &str) -> Result<bool, MediumError> {
        self.admit_write(true)?;
        self.inner.remove(key).await
    }

    async fn observe(&self, key: String) -> Result<Option<Observed<'_>>, MediumError> {
        self.charge(self.meter.admit_read(0))?;
        let mut observed = self.inner.observe(key).await?;
        if let Some(observed) = &mut observed {
            observed.bytes = self.retain_read(std::mem::take(&mut observed.bytes))?;
            self.hold(observed.observation.version_bytes())?;
        }
        Ok(observed)
    }

    async fn replace<'a>(
        &'a self,
        observed: Observation<'a>,
        bytes: OwnedBytes,
    ) -> Result<Option<Observation<'a>>, MediumError> {
        self.admit_write(false)?;
        self.inner.replace(observed, bytes).await
    }

    async fn flush(&self, keys: &[String]) -> Result<(), MediumError> {
        self.admit_write(false)?;
        self.inner.flush(keys).await
    }

    fn volatile(&self) -> bool {
        self.inner.volatile()
    }

    fn recovered(&self) -> bool {
        self.inner.recovered()
    }

    fn settle(&self) {
        self.inner.settle();
    }

    async fn remove_if(&self, observed: Observation<'_>) -> Result<bool, MediumError> {
        self.admit_write(false)?;
        self.inner.remove_if(observed).await
    }

    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, u64)>, MediumError> {
        self.charge(self.meter.admit_read(0))?;
        let listed = self.inner.list(prefix, after, limit).await?;
        let bytes = listed
            .iter()
            .map(|(key, _)| key.len() + std::mem::size_of::<(String, u64)>())
            .sum();
        self.hold(bytes)?;
        Ok(listed)
    }

    fn allocate(&self, bytes: u64) -> Result<(), MediumError> {
        self.hold(usize::try_from(bytes).unwrap_or(usize::MAX))
    }

    fn finish_allocation(&self, bytes: Vec<u8>) -> Result<OwnedBytes, MediumError> {
        let mut allocation = self.charge(
            self.held
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .split(bytes.len()),
        )?;
        if let Err(error) = allocation.reconcile_capacity(bytes.capacity()) {
            drop(bytes);
            drop(allocation);
            return self.charge(Err(error));
        }
        self.charge(
            OwnedBytes::from_vec_with_guard(bytes, allocation, Arc::clone(&self.meter)).map_err(
                |failure| match failure.into_error() {
                    acyclic_stream::PreparedOwnedBytesError::Admission(error) => error,
                    acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                        std::io::ErrorKind::InvalidData.into()
                    }
                },
            ),
        )
    }

    fn reserve_writes(&self, operations: u64) -> Result<(), MediumError> {
        self.charge(
            self.meter
                .reserve_operations(NativeOperationKind::Write, operations),
        )?;
        let mut reserved = self.reserved.lock().unwrap_or_else(PoisonError::into_inner);
        *reserved += operations;
        Ok(())
    }
}

impl<M> Drop for Metered<'_, M> {
    /// Returns what the call's reservation did not spend.
    fn drop(&mut self) {
        let operations = *self
            .reserved
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner);
        if operations > 0 {
            self.meter
                .cancel_reserved_operations(NativeOperationKind::Write, operations);
        }
    }
}

/// `account` over the caller's meter.
fn metered<'a, M: Medium>(account: &'a Account<M>, meter: &Meter) -> Account<Metered<'a, M>> {
    let medium = Arc::new(Metered::new(account.borrowed_medium(), Arc::clone(meter)));
    account.over(medium)
}

/// Reserves the exact result vector a genuine batch returns.
fn results<T>(
    count: usize,
    meter: &Meter,
) -> Result<(Vec<Result<T, NativeObjectsError>>, ReadAllocation), NativeObjectsError> {
    let bytes = count
        .checked_mul(std::mem::size_of::<Result<T, NativeObjectsError>>())
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
    let mut allocation = ReadAllocation::new(meter, bytes)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|error| meter.allocation_failed(error))?;
    allocation.reconcile_capacity(
        values.capacity() * std::mem::size_of::<Result<T, NativeObjectsError>>(),
    )?;
    Ok((values, allocation))
}

fn checked<T, M>(
    account: &Account<Metered<'_, M>>,
    value: Result<T, crate::Error>,
) -> Result<T, NativeObjectsError> {
    let medium = account.borrowed_medium();
    medium.meter.check()?;
    value.map_err(|error| medium.cause(error))
}

pub(crate) fn retain<'a, M: Medium>(
    account: &'a Account<M>,
    request: ObjectsNativeRequest<(RootId, Vec<Digest>)>,
    meter: Meter,
) -> Result<ObjectsNativeFuture<'a, Retained>, NativeObjectsError> {
    native_input_meter(&meter, &request)?;
    let account = metered(account, &meter);
    native_future(&meter, async move {
        // The account borrows the root. Keep it in the original request so
        // its backing retires before the request's funding on every exit.
        let mut request = request;
        let result = account
            .retain(&request.data.0, std::mem::take(&mut request.data.1))
            .await;
        checked(&account, result)
    })
}

pub(crate) fn get<'a, M: Medium>(
    account: &'a Account<M>,
    request: ObjectsNativeRequest<(Digest, ByteRange, u64)>,
    meter: Meter,
) -> Result<ObjectsNativeFuture<'a, Object>, NativeObjectsError> {
    native_input_meter(&meter, &request)?;
    let ((digest, range, maximum), custody) = request.split();
    let account = metered(account, &meter);
    native_future(&meter, async move {
        let _custody = custody;
        checked(&account, account.get_inner(digest, range, maximum).await)
    })
}

pub(crate) fn get_batch<'a, M: Medium>(
    account: &'a Account<M>,
    requests: ObjectsNativeRequest<Vec<(Digest, ByteRange, u64)>>,
    meter: Meter,
) -> Result<ObjectsNativeFuture<'a, ObjectsBatch<Object>>, NativeObjectsError> {
    native_input_meter(&meter, &requests)?;
    let (requests, custody) = requests.split();
    let future_meter = Arc::clone(&meter);
    let account = metered(account, &meter);
    native_future(&meter, async move {
        let _custody = custody;
        let (mut values, allocation) = results(requests.len(), &future_meter)?;
        crate::batch::drive(
            requests.into_iter(),
            crate::BATCH_WINDOW,
            |bytes| ReadAllocation::new(&future_meter, bytes),
            |(digest, range, maximum)| account.get_inner(digest, range, maximum),
            |got| values.push(checked(&account, got)),
        )
        .await?;
        ObjectsBatch::new(values, allocation)
    })
}

pub(crate) fn put<'a, M: Medium>(
    account: &'a Account<M>,
    request: ObjectsNativeRequest<(crate::Sealed, Vec<Digest>)>,
    meter: Meter,
) -> Result<ObjectsNativeFuture<'a, ObjectsPutReceipt>, NativeObjectsError> {
    native_input_meter(&meter, &request)?;
    let ((body, references), custody) = request.split();
    let account = metered(account, &meter);
    native_future(&meter, async move {
        let _custody = custody;
        checked(&account, account.put_inner(body, references).await)
            .map(|put| ObjectsPutReceipt::new(put, None))
    })
}

pub(crate) fn put_batch<'a, M: Medium>(
    account: &'a Account<M>,
    requests: ObjectsNativeRequest<Vec<(crate::Sealed, Vec<Digest>)>>,
    meter: Meter,
) -> Result<ObjectsNativeFuture<'a, ObjectsBatch<ObjectsPutReceipt>>, NativeObjectsError> {
    put_window(account, requests, meter, crate::BATCH_WINDOW)
}

/// [`put_batch`] with `window` members in flight.
pub(crate) fn put_window<'a, M: Medium>(
    account: &'a Account<M>,
    requests: ObjectsNativeRequest<Vec<(crate::Sealed, Vec<Digest>)>>,
    meter: Meter,
    window: usize,
) -> Result<ObjectsNativeFuture<'a, ObjectsBatch<ObjectsPutReceipt>>, NativeObjectsError> {
    native_input_meter(&meter, &requests)?;
    let (requests, custody) = requests.split();
    let future_meter = Arc::clone(&meter);
    let account = metered(account, &meter);
    native_future(&meter, async move {
        let _custody = custody;
        let (mut values, allocation) = results(requests.len(), &future_meter)?;
        crate::batch::drive(
            requests.into_iter(),
            window,
            |bytes| ReadAllocation::new(&future_meter, bytes),
            |(body, references)| account.put_inner(body, references),
            |put| values.push(checked(&account, put).map(|put| ObjectsPutReceipt::new(put, None))),
        )
        .await?;
        ObjectsBatch::new(values, allocation)
    })
}
