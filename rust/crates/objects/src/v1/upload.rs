//! Shared bounded upload framing and source-failure cancellation.
use super::{Error, UploadBody, request, wire};
use acyclic_stream::OwnedBytes;
use futures::{
    StreamExt,
    task::AtomicWaker,
    future::{Either, select},
    stream,
};
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};


pub(super) struct State {
    length: AtomicU64,
    complete: AtomicBool,
    closed: AtomicBool,
    failure: Mutex<Option<Error>>,
    waker: AtomicWaker,
    _allocation: Option<acyclic_native_runtime::ReadAllocation>,
}
impl State {
    pub(super) fn finish(&self) -> Result<u64, Error> {
        if !self.complete.load(Ordering::Acquire) {
            return Err(wire::ErrorCode::Unavailable.into());
        }
        Ok(self.length.load(Ordering::Acquire))
    }
    fn fail(&self, error: Error) {
        let mut failure = self.failure.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if failure.is_none() { *failure = Some(error); }
        drop(failure);
        self.waker.wake();
    }
}

struct SourceState(Arc<State>);
impl Drop for SourceState {
    fn drop(&mut self) {
        self.0.closed.store(true, Ordering::Release);
        self.0.waker.wake();
    }
}
pub(super) fn chunks(
    body: UploadBody,
    controls: Option<super::response::NativeGetControls>,
) -> Result<(UploadBody, Arc<State>), Error> {
    let allocation = match &controls {
        Some(controls) => {
            controls.check()?;
            let allocation = acyclic_native_runtime::ReadAllocation::new(
                &controls.meter, std::mem::size_of::<State>(),
            ).map_err(|error| controls.fail(error))?;
            Some(allocation)
        }
        None => None,
    };
    let state = Arc::new(State {
        length: AtomicU64::new(0),
        complete: AtomicBool::new(false),
        closed: AtomicBool::new(false),
        failure: Mutex::new(None),
        waker: AtomicWaker::new(),
        _allocation: allocation,
    });
    let chunks = stream::try_unfold(
        (body, OwnedBytes::new(), SourceState(state.clone()), controls.clone()),
        |(mut body, mut pending, state, controls)| async move {
            if let Some(controls) = &controls {
                if let Err(failure) = controls.check() {
                    state.0.fail(failure);
                    return Err(failure);
                }
            }
            while pending.is_empty() {
                if let Some(controls) = &controls {
                    if let Err(failure) = controls.check() {
                        state.0.fail(failure);
                        return Err(failure);
                    }
                }
                match body.next().await {
                    Some(Ok(bytes)) => {
                        let size = state.0.length.load(Ordering::Acquire)
                            .checked_add(bytes.len() as u64)
                            .ok_or(Error::from(wire::ErrorCode::QuotaExceeded));
                        match size.and_then(|size| {
                            request::upload_length(size)?;
                            Ok(size)
                        }) {
                            Ok(size) => {
                                state.0.length.store(size, Ordering::Release);
                                pending = bytes;
                            }
                            Err(failure) => {
                                state.0.fail(failure);
                                return Err(failure);
                            }
                        }
                    }
                    Some(Err(failure)) => {
                        state.0.fail(failure);
                        return Err(failure);
                    }
                    None => {
                        state.0.complete.store(true, Ordering::Release);
                        return Ok(None);
                    }
                }
            }
            let size = pending.len().min(65_536);
            let chunk = pending.slice(..size);
            pending = pending.slice(size..);
            Ok(Some((chunk, (body, pending, state, controls))))
        },
    );
    let chunks = match &controls {
        Some(controls) => box_native(chunks, &controls.meter).map_err(|error| controls.fail(error))?,
        None => chunks.boxed().into(),
    };
    Ok((chunks, state))
}

fn box_native<S>(
    stream: S,
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> Result<UploadBody, super::NativeObjectsError>
where
    S: futures::Stream<Item = Result<OwnedBytes, Error>> + Send + 'static,
{
    let allocation = acyclic_native_runtime::ReadAllocation::new(
        meter, std::mem::size_of::<S>(),
    )?;
    Ok(UploadBody { stream: stream.boxed(), _allocation: Some(allocation) })
}

pub(super) fn chunks_native(
    body: OwnedBytes,
    meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    errors: Arc<super::NativeTransportErrors>,
) -> Result<UploadBody, super::NativeObjectsError> {
    meter.check()?;
    let source_meter = meter.clone();
    let source = stream::once(async move {
        if let Err(error) = source_meter.check() {
            errors.record(error.into());
            return Err(wire::ErrorCode::Unavailable.into());
        }
        Ok(body)
    });
    // The shared controlled framing engine performs custody-preserving slicing.
    box_native(source, &meter)
}
pub(super) async fn run<T: Send>(
    request: impl Future<Output = Result<T, Error>> + Send,
    state: &State,
) -> Result<T, Error> {
    let failure = futures::future::poll_fn(|cx| {
        state.waker.register(cx.waker());
        let mut failure = state.failure.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(error) = failure.take() { return std::task::Poll::Ready(Some(error)); }
        if state.closed.load(Ordering::Acquire) { return std::task::Poll::Ready(None); }
        std::task::Poll::Pending
    });
    futures::pin_mut!(request, failure);
    match select(failure, request).await {
        Either::Left((Some(error), _)) => Err(error),
        Either::Left((None, request)) => request.await,
        Either::Right((result, _)) => result,
    }
}
