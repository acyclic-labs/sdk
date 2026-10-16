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
pub(super) fn chunks(body: UploadBody) -> (UploadBody, Arc<State>) {
    let state = Arc::new(State {
        result: Mutex::new(None),
        waker: AtomicWaker::new(),
    });
    let chunks = stream::try_unfold(
        (body, OwnedBytes::new(), 0_u64, SourceState(state.clone())),
        |(mut body, mut pending, mut length, state)| async move {
            let fail = |failure| {
                state.0.fail(failure);
                failure
            };
            while pending.is_empty() {
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
            let chunk = if size == pending.len() {
                std::mem::take(&mut pending)
            } else {
                let chunk = pending.slice(..size);
                pending = pending.slice(size..);
                chunk
            };
            Ok(Some((chunk, (body, pending, length, state))))
        },
    );
    (chunks.boxed().into(), state)
}

pub(super) fn body_bytes(chunk: OwnedBytes) -> Vec<u8> {
    chunk
        .try_into_ordinary_vec()
        .unwrap_or_else(|chunk| chunk.to_vec())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn request_path_reuses_unique_complete_upload_chunks() -> Result<(), Error> {
        for length in [1, 65_536] {
            let data = vec![7; length];
            let pointer = data.as_ptr();
            let body = stream::iter([Ok(OwnedBytes::from(data))]).boxed().into();
            let (mut chunks, state) = chunks(body);
            let chunk = chunks
                .next()
                .await
                .ok_or(Error::from(wire::ErrorCode::Unavailable))??;
            let frame = body_bytes(chunk);
            assert_eq!(frame.as_ptr(), pointer);
            assert_eq!(frame, vec![7; length]);
            assert!(chunks.next().await.is_none());
            assert_eq!(state.finish()?, length as u64);
        }
        Ok(())
    }

    #[tokio::test]
    async fn request_path_preserves_shared_and_split_upload_ranges() -> Result<(), Error> {
        let original = OwnedBytes::from(vec![1; 65_537]);
        let body = stream::iter([Ok(original.clone())]).boxed().into();
        let (mut chunks, state) = chunks(body);
        let first = body_bytes(
            chunks
                .next()
                .await
                .ok_or(Error::from(wire::ErrorCode::Unavailable))??,
        );
        let mut last = body_bytes(
            chunks
                .next()
                .await
                .ok_or(Error::from(wire::ErrorCode::Unavailable))??,
        );
        assert_eq!(first, vec![1; 65_536]);
        assert_eq!(last, vec![1]);
        last.fill(2);
        assert!(original.iter().all(|byte| *byte == 1));
        assert!(chunks.next().await.is_none());
        assert_eq!(state.finish()?, 65_537);
        Ok(())
    }
    #[test]
    #[ignore = "opt-in debug-profile request-path measurement: npm run bench:clients"]
    fn request_path_measure_upload_frame_conversion() {
        for reuse in [false, true] {
            let chunks: Vec<_> = (0..256)
                .map(|_| OwnedBytes::from(vec![7; 65_536]))
                .collect();
            let started = std::time::Instant::now();
            for chunk in chunks {
                std::hint::black_box(if reuse {
                    body_bytes(chunk)
                } else {
                    chunk.to_vec()
                });
            }
            eprintln!(
                "objects frame conversion reuse={reuse} iterations=256 bytes=65536 elapsed_us={}",
                started.elapsed().as_micros()
            );
        }
    }
}
