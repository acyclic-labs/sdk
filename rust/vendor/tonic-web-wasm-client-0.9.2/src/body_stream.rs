use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream::empty};
use http_body::{Body, Frame};
use js_sys::Uint8Array;
use wasm_streams::readable::IntoStream;

use crate::{Error, abort_guard::AbortGuard};

pub struct BodyStream {
    body_stream: Pin<Box<dyn Stream<Item = Result<Bytes, Error>>>>,
    _abort: Option<AbortGuard>,
}

impl BodyStream {
    pub fn new(
        body_stream: IntoStream<'static>,
        abort: AbortGuard,
        maximum: Option<usize>,
    ) -> Self {
        let mut received = 0_usize;
        let body_stream = body_stream.map(move |frame| {
            let js_value = frame.map_err(Error::js_error)?;
            let buffer = Uint8Array::new(&js_value);
            received = checked_response_size(received, buffer.length() as usize, maximum)?;
            let mut bytes_vec = vec![0; buffer.length() as usize];
            buffer.copy_to(&mut bytes_vec);
            Ok(bytes_vec.into())
        });

        Self {
            body_stream: Box::pin(body_stream),
            _abort: Some(abort),
        }
    }

    #[cfg(test)]
    pub fn from_stream(body_stream: impl Stream<Item = Result<Bytes, Error>> + 'static) -> Self {
        Self {
            body_stream: Box::pin(body_stream),
            _abort: None,
        }
    }

    pub fn empty() -> Self {
        let body_stream = empty();

        Self {
            body_stream: Box::pin(body_stream),
            _abort: None,
        }
    }
}

impl Body for BodyStream {
    type Data = Bytes;

    type Error = Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        match self.body_stream.as_mut().poll_next(cx) {
            Poll::Ready(maybe) => Poll::Ready(maybe.map(|result| result.map(Frame::data))),
            Poll::Pending => Poll::Pending,
        }
    }
}

unsafe impl Send for BodyStream {}
unsafe impl Sync for BodyStream {}

fn checked_response_size(
    received: usize,
    chunk: usize,
    maximum: Option<usize>,
) -> Result<usize, Error> {
    received
        .checked_add(chunk)
        .filter(|next| maximum.is_none_or(|maximum| *next <= maximum))
        .ok_or(Error::ResponseTooLarge)
}

#[cfg(test)]
mod bound_tests {
    use super::*;
    #[test]
    fn byte_budget_includes_previous_chunks_and_overflow() {
        assert_eq!(checked_response_size(3, 5, Some(8)).unwrap(), 8);
        assert!(matches!(
            checked_response_size(3, 6, Some(8)),
            Err(Error::ResponseTooLarge)
        ));
        assert!(matches!(
            checked_response_size(usize::MAX, 1, None),
            Err(Error::ResponseTooLarge)
        ));
    }
}
