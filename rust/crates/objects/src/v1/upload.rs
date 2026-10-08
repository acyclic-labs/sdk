//! Shared bounded upload framing and source-failure cancellation.
use super::{Error, UploadBody, request, wire};
use bytes::Bytes;
use futures::{
    StreamExt,
    channel::oneshot,
    future::{Either, select},
    stream,
};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

pub(super) struct State {
    length: AtomicU64,
    complete: AtomicBool,
}
impl State {
    pub(super) fn finish(&self) -> Result<u64, Error> {
        if !self.complete.load(Ordering::Acquire) {
            return Err(wire::ErrorCode::Unavailable.into());
        }
        Ok(self.length.load(Ordering::Acquire))
    }
}
pub(super) fn chunks(body: UploadBody) -> (UploadBody, Arc<State>, oneshot::Receiver<Error>) {
    let state = Arc::new(State {
        length: AtomicU64::new(0),
        complete: AtomicBool::new(false),
    });
    let (error, failure) = oneshot::channel();
    let chunks = stream::try_unfold(
        (body, Bytes::new(), 0_u64, state.clone(), Some(error)),
        |(mut body, mut pending, mut length, state, mut error)| async move {
            while pending.is_empty() {
                match body.next().await {
                    Some(Ok(bytes)) => {
                        let size = length
                            .checked_add(bytes.len() as u64)
                            .ok_or(Error::from(wire::ErrorCode::QuotaExceeded));
                        match size.and_then(|size| {
                            request::upload_length(size)?;
                            Ok(size)
                        }) {
                            Ok(size) => {
                                length = size;
                                state.length.store(size, Ordering::Release);
                                pending = bytes;
                            }
                            Err(failure) => {
                                if let Some(error) = error.take() {
                                    let _ = error.send(failure);
                                }
                                return Err(failure);
                            }
                        }
                    }
                    Some(Err(failure)) => {
                        if let Some(error) = error.take() {
                            let _ = error.send(failure);
                        }
                        return Err(failure);
                    }
                    None => {
                        state.complete.store(true, Ordering::Release);
                        return Ok(None);
                    }
                }
            }
            let chunk = pending.split_to(pending.len().min(65_536));
            Ok(Some((chunk, (body, pending, length, state, error))))
        },
    )
    .boxed();
    (chunks, state, failure)
}
pub(super) async fn run<T: Send>(
    request: impl Future<Output = Result<T, Error>> + Send,
    failure: oneshot::Receiver<Error>,
) -> Result<T, Error> {
    futures::pin_mut!(request, failure);
    match select(failure, request).await {
        Either::Left((Ok(error), _)) => Err(error),
        Either::Left((Err(_), request)) => request.await,
        Either::Right((result, _)) => result,
    }
}
