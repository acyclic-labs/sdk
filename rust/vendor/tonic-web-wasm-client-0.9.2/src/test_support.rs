//! Helpers for unit tests that feed a [`ResponseBody`] from memory instead of a
//! fetch body, so the decoder can be tested natively with `cargo test`.

use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::stream;
use http::HeaderMap;
use http_body::{Body, Frame};

use crate::{Error, ResponseBody, body_stream::BodyStream, limits::UnaryResponseLimits};

pub const GRPC_WEB: &str = "application/grpc-web+proto";

/// A grpc-web frame: the flag byte, the big-endian payload length, the payload.
pub fn frame(flag: u8, payload: &[u8]) -> Bytes {
    let mut frame = BytesMut::with_capacity(5 + payload.len());
    frame.put_u8(flag);
    frame.put_u32(payload.len() as u32);
    frame.put_slice(payload);
    frame.freeze()
}

/// A body fed from `chunks`. The stream returns `Pending` before every chunk,
/// as a fetch body does, so `poll_frame` is re-entered at each chunk boundary.
pub fn body_from_chunks(content_type: &str, chunks: Vec<Bytes>) -> ResponseBody {
    body_from_chunks_with_limits(content_type, chunks, None)
}

pub fn body_from_chunks_with_limits(
    content_type: &str,
    chunks: Vec<Bytes>,
    limits: Option<UnaryResponseLimits>,
) -> ResponseBody {
    let mut chunks = VecDeque::from(chunks);
    let mut pending = false;
    let body_stream = stream::poll_fn(move |cx| {
        pending = !pending;
        if pending {
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(chunks.pop_front().map(Ok))
        }
    });

    ResponseBody::from_body_stream(BodyStream::from_stream(body_stream), content_type, limits)
        .unwrap()
}

struct ThreadWake(std::thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Waits for decoder wakeups until it returns a frame, an error or the end.
pub fn next_frame(body: &mut ResponseBody) -> Option<Result<Frame<Bytes>, Error>> {
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(frame) = Pin::new(&mut *body).poll_frame(&mut cx) {
            return frame;
        }
        std::thread::park();
    }
}

/// Drains `body`, returning the data bytes and the trailers.
pub fn drain(mut body: ResponseBody) -> Result<(BytesMut, Option<HeaderMap>), Error> {
    let (mut data, mut trailers) = (BytesMut::new(), None);

    while let Some(frame) = next_frame(&mut body) {
        match frame?.into_data() {
            Ok(bytes) => data.put(bytes),
            Err(frame) => trailers = frame.into_trailers().ok(),
        }
    }

    Ok((data, trailers))
}
