//! Shared bounded upload framing and source-failure cancellation.
use super::{Error, UploadBody, request, wire};
use acyclic_stream::OwnedBytes;
use futures::{
    StreamExt,
    future::{Either, select},
    stream,
    task::AtomicWaker,
};
use std::{
    future::Future,
    sync::{Arc, Mutex},
};

pub(super) struct State {
    result: Mutex<Option<Result<Option<u64>, Error>>>,
    waker: AtomicWaker,
    _allocation: Option<acyclic_native_runtime::ReadAllocation>,
}
impl State {
    pub(super) fn finish(&self) -> Result<u64, Error> {
        match *self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            Some(Ok(Some(length))) => Ok(length),
            Some(Err(error)) => Err(error),
            _ => Err(wire::ErrorCode::Unavailable.into()),
        }
    }
    fn fail(&self, error: Error) {
        let mut result = self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if result.is_none() {
            *result = Some(Err(error));
        }
        drop(result);
        self.waker.wake();
    }
}

struct SourceState(Arc<State>);
impl Drop for SourceState {
    fn drop(&mut self) {
        let mut result = self
            .0
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if result.is_none() {
            *result = Some(Ok(None));
        }
        drop(result);
        self.0.waker.wake();
    }
}
pub(super) fn chunks(
    body: UploadBody,
    controls: Option<Arc<super::NativeTransportErrors>>,
) -> Result<(UploadBody, Arc<State>), Error> {
    let allocation = match &controls {
        Some(controls) => {
            controls.check()?;
            let allocation = acyclic_native_runtime::ReadAllocation::new(
                &controls.meter,
                std::mem::size_of::<State>(),
            )
            .map_err(|error| controls.fail(error))?;
            Some(allocation)
        }
        None => None,
    };
    let state = Arc::new(State {
        result: Mutex::new(None),
        waker: AtomicWaker::new(),
        _allocation: allocation,
    });
    let chunks = stream::try_unfold(
        (
            body,
            OwnedBytes::new(),
            0_u64,
            SourceState(state.clone()),
            controls.clone(),
        ),
        |(mut body, mut pending, mut length, state, controls)| async move {
            let fail = |failure| {
                state.0.fail(failure);
                failure
            };
            if let Some(controls) = &controls {
                controls.check().map_err(fail)?;
            }
            while pending.is_empty() {
                if let Some(controls) = &controls {
                    controls.check().map_err(fail)?;
                }
                match body.next().await {
                    Some(Ok(bytes)) => {
                        length = length
                            .checked_add(bytes.len() as u64)
                            .ok_or(Error::from(wire::ErrorCode::QuotaExceeded))
                            .and_then(|size| {
                                request::upload_length(size)?;
                                Ok(size)
                            })
                            .map_err(fail)?;
                        pending = bytes;
                    }
                    Some(Err(failure)) => {
                        return Err(fail(failure));
                    }
                    None => {
                        *state
                            .0
                            .result
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) =
                            Some(Ok(Some(length)));
                        return Ok(None);
                    }
                }
            }
            let size = pending.len().min(65_536);
            let chunk = pending.slice(..size);
            pending = pending.slice(size..);
            Ok(Some((chunk, (body, pending, length, state, controls))))
        },
    );
    let chunks = match &controls {
        Some(controls) => {
            box_native(chunks, &controls.meter).map_err(|error| controls.fail(error))?
        }
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
    let allocation = acyclic_native_runtime::ReadAllocation::new(meter, std::mem::size_of::<S>())?;
    Ok(UploadBody {
        value: stream.boxed(),
        _allocation: Some(allocation),
    })
}

pub(super) fn chunks_native(
    body: OwnedBytes,
    errors: Arc<super::NativeTransportErrors>,
) -> Result<UploadBody, super::NativeObjectsError> {
    errors.meter.check()?;
    box_native(stream::once(std::future::ready(Ok(body))), &errors.meter)
}
pub(super) async fn run<T: Send>(
    request: impl Future<Output = Result<T, Error>> + Send,
    state: &State,
) -> Result<T, Error> {
    let failure = futures::future::poll_fn(|cx| {
        state.waker.register(cx.waker());
        let mut result = state
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(Err(error)) = *result {
            *result = None;
            return std::task::Poll::Ready(Some(error));
        }
        if result.is_some() {
            return std::task::Poll::Ready(None);
        }
        std::task::Poll::Pending
    });
    futures::pin_mut!(request, failure);
    match select(failure, request).await {
        Either::Left((Some(error), _)) => Err(error),
        Either::Left((None, request)) => request.await,
        Either::Right((result, _)) => result,
    }
}
