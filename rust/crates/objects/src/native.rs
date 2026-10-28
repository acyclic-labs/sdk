//! Original-meter producers over the same engine paths as ordinary calls.
use crate::{
    Account, ByteRange, Digest, Medium, MediumError, NativeObjectsCause, NativeObjectsError, Object,
    ObjectsBatch, ObjectsNativeFuture, ObjectsNativeRequest, ObjectsPutReceipt, native_future,
    native_input_meter,
};
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation};
use acyclic_stream::OwnedBytes;
use std::ops::Range;
use std::sync::{Arc, Mutex, PoisonError};

type Meter = Arc<dyn NativeReadMeter>;

/// The caller's meter over every medium operation of one native call, batch
/// members included: each read, write, removal and listing is admitted before
/// dispatch and recorded after, and every buffer the medium or engine
/// allocates stays charged until the call ends.
pub(crate) struct Metered<M> {
    inner: Arc<M>,
    meter: Meter,
    held: Mutex<ReadAllocation>,
    refusal: Mutex<Option<std::io::Error>>,
}

impl<M> Metered<M> {
    fn new(inner: Arc<M>, meter: Meter) -> Self {
        Self {
            held: Mutex::new(ReadAllocation::empty(&meter)),
            inner,
            meter,
            refusal: Mutex::new(None),
        }
    }

    /// Keeps the meter's own refusal for the caller; the engine sees a failure.
    fn charge(&self, result: std::io::Result<()>) -> Result<(), MediumError> {
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
        let grown = self.held.lock().unwrap_or_else(PoisonError::into_inner).grow(bytes);
        self.charge(grown)
    }

    /// The meter's refusal behind `error`, else the engine's error.
    fn cause(&self, error: crate::Error) -> NativeObjectsError {
        match self.refusal.lock().unwrap_or_else(PoisonError::into_inner).take() {
            Some(refusal) => refusal.into(),
            None => NativeObjectsError::from(NativeObjectsCause::Service(error)),
        }
    }
}

#[async_trait::async_trait]
impl<M: Medium> Medium for Metered<M> {
    async fn create(&self, key: &str, bytes: OwnedBytes) -> Result<bool, MediumError> {
        self.charge(self.meter.admit_write(bytes.len()))?;
        let created = self.inner.create(key, bytes.clone()).await;
        let written = if matches!(created, Ok(true)) { bytes.len() } else { 0 };
        self.charge(self.meter.record_write(written))?;
        created
    }

    async fn read(&self, key: &str, range: Option<Range<u64>>) -> Result<Option<OwnedBytes>, MediumError> {
        let maximum = range.as_ref().map_or(0, |range| range.end.saturating_sub(range.start));
        self.charge(self.meter.admit_read(usize::try_from(maximum).unwrap_or(usize::MAX)))?;
        let read = self.inner.read(key, range).await?;
        let length = read.as_ref().map_or(0, OwnedBytes::len);
        self.charge(self.meter.record_read(length))?;
        self.hold(length)?;
        Ok(read)
    }

    async fn remove(&self, key: &str) -> Result<bool, MediumError> {
        self.charge(self.meter.admit_write(0))?;
        let removed = self.inner.remove(key).await;
        self.charge(self.meter.record_write(0))?;
        removed
    }

    async fn list(&self, prefix: &str, after: Option<&str>, limit: usize) -> Result<Vec<(String, u64)>, MediumError> {
        self.charge(self.meter.admit_read(0))?;
        let listed = self.inner.list(prefix, after, limit).await?;
        let bytes = listed
            .iter()
            .map(|(key, _)| key.len() + std::mem::size_of::<(String, u64)>())
            .sum();
        self.charge(self.meter.record_read(bytes))?;
        self.hold(bytes)?;
        Ok(listed)
    }

    fn allocate(&self, bytes: u64) -> Result<(), MediumError> {
        self.hold(usize::try_from(bytes).unwrap_or(usize::MAX))
    }
}

/// `account` over the caller's meter.
fn metered<M: Medium>(account: &Account<M>, meter: &Meter) -> (Arc<Metered<M>>, Account<Metered<M>>) {
    let medium = Arc::new(Metered::new(account.shared_medium(), Arc::clone(meter)));
    let metered = account.over(Arc::clone(&medium));
    (medium, metered)
}

/// Reserves the exact result vector a genuine batch returns.
fn results<T>(count: usize, meter: &Meter) -> Result<(Vec<Result<T, NativeObjectsError>>, ReadAllocation), NativeObjectsError> {
    let bytes = count
        .checked_mul(std::mem::size_of::<Result<T, NativeObjectsError>>())
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
    let mut allocation = ReadAllocation::new(meter, bytes)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|error| meter.allocation_failed(error))?;
    allocation.reconcile_capacity(values.capacity() * std::mem::size_of::<Result<T, NativeObjectsError>>())?;
    Ok((values, allocation))
}

fn checked<T, M>(medium: &Metered<M>, value: Result<T, crate::Error>) -> Result<T, NativeObjectsError> {
    medium.meter.check()?;
    value.map_err(|error| medium.cause(error))
}

pub(crate) fn get<'a, M: Medium>(
    account: &'a Account<M>,
    request: ObjectsNativeRequest<(Digest, ByteRange, u64)>,
    meter: Meter,
) -> Result<ObjectsNativeFuture<'a, Object>, NativeObjectsError> {
    native_input_meter(&meter, &request)?;
    let ((digest, range, maximum), custody) = request.split();
    let (medium, account) = metered(account, &meter);
    native_future(&meter, async move {
        let _custody = custody;
        checked(&medium, account.get_inner(digest, range, maximum).await)
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
    let (medium, account) = metered(account, &meter);
    native_future(&meter, async move {
        let _custody = custody;
        let (mut values, allocation) = results(requests.len(), &future_meter)?;
        for (digest, range, maximum) in requests {
            values.push(checked(&medium, account.get_inner(digest, range, maximum).await));
        }
        ObjectsBatch::new(values, allocation)
    })
}

pub(crate) fn put<'a, M: Medium>(
    account: &'a Account<M>,
    request: ObjectsNativeRequest<(OwnedBytes, Vec<Digest>)>,
    meter: Meter,
) -> Result<ObjectsNativeFuture<'a, ObjectsPutReceipt>, NativeObjectsError> {
    native_input_meter(&meter, &request)?;
    let ((body, references), custody) = request.split();
    let (medium, account) = metered(account, &meter);
    native_future(&meter, async move {
        let _custody = custody;
        checked(&medium, account.put_inner(body, references).await)
            .map(|put| ObjectsPutReceipt::new(put, None))
    })
}

pub(crate) fn put_batch<'a, M: Medium>(
    account: &'a Account<M>,
    requests: ObjectsNativeRequest<Vec<(OwnedBytes, Vec<Digest>)>>,
    meter: Meter,
) -> Result<ObjectsNativeFuture<'a, ObjectsBatch<ObjectsPutReceipt>>, NativeObjectsError> {
    native_input_meter(&meter, &requests)?;
    let (requests, custody) = requests.split();
    let future_meter = Arc::clone(&meter);
    let (medium, account) = metered(account, &meter);
    native_future(&meter, async move {
        let _custody = custody;
        let (mut values, allocation) = results(requests.len(), &future_meter)?;
        for (body, references) in requests {
            values.push(
                checked(&medium, account.put_inner(body, references).await)
                    .map(|put| ObjectsPutReceipt::new(put, None)),
            );
        }
        ObjectsBatch::new(values, allocation)
    })
}
