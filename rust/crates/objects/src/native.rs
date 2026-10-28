//! Original-meter producers over the same engine paths as ordinary calls.
use crate::{
    Account, ByteRange, Digest, Medium, NativeObjectsCause, NativeObjectsError, Object, ObjectsBatch,
    ObjectsNativeFuture, ObjectsNativeRequest, ObjectsPutReceipt, native_future, native_input_meter,
};
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation};
use acyclic_stream::OwnedBytes;
use std::sync::Arc;

type Meter = Arc<dyn NativeReadMeter>;

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

fn checked<T>(meter: &Meter, value: Result<T, crate::Error>) -> Result<T, NativeObjectsError> {
    meter.check()?;
    value.map_err(|error| NativeObjectsError::from(NativeObjectsCause::Service(error)))
}

pub(crate) fn get<'a, M: Medium>(
    account: &'a Account<M>,
    request: ObjectsNativeRequest<(Digest, ByteRange, u64)>,
    meter: Meter,
) -> Result<ObjectsNativeFuture<'a, Object>, NativeObjectsError> {
    native_input_meter(&meter, &request)?;
    let ((digest, range, maximum), custody) = request.split();
    let future_meter = Arc::clone(&meter);
    native_future(&meter, async move {
        let _custody = custody;
        checked(&future_meter, account.get_inner(digest, range, maximum).await)
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
    native_future(&meter, async move {
        let _custody = custody;
        let (mut values, allocation) = results(requests.len(), &future_meter)?;
        for (digest, range, maximum) in requests {
            values.push(checked(&future_meter, account.get_inner(digest, range, maximum).await));
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
    let future_meter = Arc::clone(&meter);
    native_future(&meter, async move {
        let _custody = custody;
        checked(&future_meter, account.put_inner(body, references).await)
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
    native_future(&meter, async move {
        let _custody = custody;
        let (mut values, allocation) = results(requests.len(), &future_meter)?;
        for (body, references) in requests {
            values.push(
                checked(&future_meter, account.put_inner(body, references).await)
                    .map(|put| ObjectsPutReceipt::new(put, None)),
            );
        }
        ObjectsBatch::new(values, allocation)
    })
}
