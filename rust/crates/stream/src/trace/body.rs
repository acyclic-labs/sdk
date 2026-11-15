//! Original stream custody: EOF settles; cancellation and errors remain unfinished.
#[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
use super::Outcome;
use super::OwnedRecording;
use futures::Stream;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

/// An original response body under its admitted owner. The body is dropped under
/// that owner before its recording retires; no detached task or buffer is added.
pub struct RecordedStream<S> {
    body: Option<S>,
    recording: Option<OwnedRecording>,
}

impl<S> RecordedStream<S> {
    /// Transfer the original body and its optional shared recording together.
    pub fn new(body: S, recording: Option<OwnedRecording>) -> Self {
        Self {
            body: Some(body),
            recording,
        }
    }

    fn close(&mut self) {
        if let Some(recording) = &self.recording {
            recording.within_sync(|| drop(self.body.take()));
        } else {
            drop(self.body.take());
        }
    }
}

impl<S> Drop for RecordedStream<S> {
    fn drop(&mut self) {
        self.close();
    }
}

impl<S, T, E> Stream for RecordedStream<S>
where
    S: Stream<Item = Result<T, E>> + Unpin,
{
    type Item = Result<T, E>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let state = self.get_mut();
        let Some(body) = &mut state.body else {
            return Poll::Ready(None);
        };
        let next = if let Some(recording) = &state.recording {
            recording.within_sync(|| Pin::new(body).poll_next(cx))
        } else {
            Pin::new(body).poll_next(cx)
        };
        match &next {
            Poll::Ready(None) => {
                state.close();
                #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
                if let Some(recording) = state.recording.take() {
                    let _completion = recording.finish_submitted(None, Outcome::Succeeded);
                }
            }
            Poll::Ready(Some(Err(_))) => {
                state.close();
                drop(state.recording.take());
            }
            _ => {}
        }
        next
    }
}
