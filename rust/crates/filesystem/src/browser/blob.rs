//! Physical browser operations retain their original funding through settlement.
use crate::async_storage::object_error;
use crate::kernel::allocation::{
    KernelAccounting,
    retained::{Retained, RetainedAllocation},
};
use crate::{CancellationToken, ObjectStoreError, WorkCounters};
use acyclic_stream::completion::{Completion, Producer};
use acyclic_stream::{OwnedBytes, javascript_bytes};
use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;
use web_sys::Blob;

type RawBuffer = Retained<Result<JsValue, JsValue>>;

struct Physical<F: Future> {
    future: Pin<Box<F>>,
    producer: Option<Producer<F::Output, RetainedAllocation>>,
    // The original future is destroyed before its admission retires.
    _storage: RetainedAllocation,
}

impl<F: Future> Future for Physical<F> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let value = std::task::ready!(self.future.as_mut().poll(cx));
        if let Some(producer) = self.producer.take() {
            producer.publish(value);
        }
        Poll::Ready(())
    }
}

/// Materializes one actual Blob on the caller's original work and allocation
/// ledger. Cancellation stops delivery; a submitted physical read retains its
/// buffer funding until its Promise settles. Only a live caller copies bytes.
/// Browser-owned opaque Promise and executor storage is outside the SDK buffer
/// contract; the result slot, producer frame, payload and Rust carrier are funded.
pub async fn blob_bytes(
    blob: &Blob,
    length: u64,
    accounting: &RefCell<KernelAccounting<'_>>,
    cancellation: &CancellationToken,
) -> Result<OwnedBytes, ObjectStoreError> {
    let capacity = usize::try_from(length).map_err(|_| crate::WorkError::Overflow)?;
    let Retained { value, allocation } =
        retained_promise(|| Ok(blob.array_buffer()), length, accounting, cancellation).await?;
    let buffer = value.map_err(rejected)?;
    let mut allocation_error = None;
    let copied = javascript_bytes::snapshot_buffer_with(&buffer, |count| {
        if count != length {
            return Err(JsValue::UNDEFINED);
        }
        let mut accounting = accounting.borrow_mut();
        let result: Result<_, crate::FsError> = (|| {
            accounting.charge(WorkCounters {
                bytes_copied: length,
                ..Default::default()
            })?;
            let retained = Retained::<Vec<u8>>::allocate(capacity, &mut accounting)?;
            Ok((retained.value, retained.allocation))
        })();
        result.map_err(|error| {
            allocation_error = Some(error);
            JsValue::UNDEFINED
        })
    });
    let (value, retained) = copied.map_err(|error| {
        allocation_error.map_or_else(
            || {
                if error.is_undefined() {
                    ObjectStoreError::Corrupt
                } else {
                    rejected(error)
                }
            },
            object_error,
        )
    })?;
    drop(buffer);
    drop(allocation);
    Retained {
        value,
        allocation: retained,
    }
    .into_bytes((), &mut accounting.borrow_mut())
    .map(OwnedBytes::from)
    .map_err(object_error)
}

/// Validates the browser's integral Blob extent before physical materialization.
pub fn blob_length(blob: &Blob) -> Result<u64, ObjectStoreError> {
    let length = blob.size();
    // Reject fractional, negative and overflowing extents before conversion.
    if !length.is_finite()
        || length < 0.0
        || length >= 18_446_744_073_709_551_616.0
        || length.fract() != 0.0
    {
        return Err(ObjectStoreError::Corrupt);
    }
    Ok(length as u64)
}

/// Waits for one metadata Promise with funded result and producer storage.
/// A dropped caller stops delivery while the one submitted producer settles.
pub async fn promise(
    start: impl FnOnce() -> Result<js_sys::Promise, ObjectStoreError>,
    accounting: &RefCell<KernelAccounting<'_>>,
    cancellation: &CancellationToken,
) -> Result<Result<JsValue, JsValue>, ObjectStoreError> {
    let Retained { value, allocation } =
        retained_promise(start, 0, accounting, cancellation).await?;
    drop(allocation);
    Ok(value)
}

async fn retained_promise(
    start: impl FnOnce() -> Result<js_sys::Promise, ObjectStoreError>,
    length: u64,
    accounting: &RefCell<KernelAccounting<'_>>,
    cancellation: &CancellationToken,
) -> Result<RawBuffer, ObjectStoreError> {
    let payload =
        RetainedAllocation::claim(length, u64::from(length != 0), &mut accounting.borrow_mut())
            .map_err(object_error)?;
    complete(
        || {
            let promise = start()?;
            Ok(async move { payload.into_value(JsFuture::from(promise).await) })
        },
        WorkCounters {
            backend_read_operations: 1,
            object_bytes_read: length,
            bytes_copied: length,
            ..Default::default()
        },
        accounting,
        cancellation,
    )
    .await
}

/// Owns one complete physical browser operation on its original ledger.
/// Admission precedes `start`; the result slot and boxed future remain funded
/// until the producer settles, even if delivery is cancelled or dropped.
/// The future owns its payload leases and any required terminal cleanup.
/// There is one producer and one completion event, with no retry or timer.
pub async fn complete<F: Future + 'static>(
    start: impl FnOnce() -> Result<F, ObjectStoreError>,
    work: WorkCounters,
    accounting: &RefCell<KernelAccounting<'_>>,
    cancellation: &CancellationToken,
) -> Result<F::Output, ObjectStoreError>
where
    F::Output: 'static,
{
    let (completion, producer) = acyclic_stream::completion::channel(|bytes| {
        RetainedAllocation::claim(bytes as u64, 1, &mut accounting.borrow_mut())
    })
    .map_err(object_error)?;
    let storage = RetainedAllocation::claim(
        std::mem::size_of::<F>() as u64,
        1,
        &mut accounting.borrow_mut(),
    )
    .map_err(object_error)?;
    accounting.borrow_mut().charge(work).map_err(object_error)?;
    acyclic_stream::runtime::PlatformExecutor.spawn_owned(Physical {
        future: Box::pin(start()?),
        producer: Some(producer),
        _storage: storage,
    });
    wait(&completion, accounting, cancellation).await?;
    let (_, raw, control) = completion
        .try_take()
        .map_err(|_| ObjectStoreError::SharedReadUnavailable)?;
    drop(control);
    raw.ok_or(ObjectStoreError::SharedReadUnavailable)
}

async fn wait<T>(
    completion: &Completion<T, RetainedAllocation>,
    accounting: &RefCell<KernelAccounting<'_>>,
    cancellation: &CancellationToken,
) -> Result<(), ObjectStoreError> {
    let mut result = std::pin::pin!(completion.result());
    let mut stopped = None;
    std::future::poll_fn(|cx| {
        if let Poll::Ready(result) = result.as_mut().poll(cx) {
            return Poll::Ready(
                result
                    .map(|_| ())
                    .map_err(|_| ObjectStoreError::SharedReadUnavailable),
            );
        }
        accounting
            .borrow()
            .admit(WorkCounters::UNCHARGED)
            .map_err(object_error)?;
        if stopped.is_none() {
            stopped = Some(
                accounting
                    .borrow_mut()
                    .cancellation_wait(cancellation)
                    .map_err(object_error)?,
            );
        }
        if let Some(stopped) = &mut stopped
            && Pin::new(stopped).poll(cx).is_ready()
        {
            return Poll::Ready(Err(ObjectStoreError::Cancelled));
        }
        Poll::Pending
    })
    .await
}

fn rejected(error: JsValue) -> ObjectStoreError {
    ObjectStoreError::Rejected(
        error
            .as_string()
            .unwrap_or_else(|| "browser byte materialization failed".to_owned()),
    )
}
