use super::{GuestError, GuestRequest, GuestResponse};
use bytes::Bytes;
use futures::Stream;
use http_body::{Body, Frame, SizeHint};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Native streaming fetch body, including ongoing SSE; buffered bytes remain convenient.
pub enum GuestBody {
    /// One already-buffered byte sequence.
    Buffered(Bytes),
    /// Host-driven chunks with backpressure and cancellation on stream drop.
    Stream(Pin<Box<dyn Stream<Item = Result<Bytes, GuestError>> + Send>>),
    /// Original HTTP body, preserving data, terminal trailers and drop cancellation.
    Frames(Pin<Box<dyn Body<Data = Bytes, Error = GuestError> + Send>>),
}

impl Body for GuestBody {
    type Data = Bytes;
    type Error = GuestError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, GuestError>>> {
        match self.get_mut() {
            Self::Buffered(bytes) if bytes.is_empty() => Poll::Ready(None),
            Self::Buffered(bytes) => Poll::Ready(Some(Ok(Frame::data(std::mem::take(bytes))))),
            Self::Stream(stream) => stream
                .as_mut()
                .poll_next(cx)
                .map(|part| part.map(|part| part.map(Frame::data))),
            Self::Frames(body) => body.as_mut().poll_frame(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Buffered(bytes) => bytes.is_empty(),
            Self::Stream(_) => false,
            Self::Frames(body) => body.is_end_stream(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Buffered(bytes) => SizeHint::with_exact(bytes.len() as u64),
            Self::Stream(_) => SizeHint::default(),
            Self::Frames(body) => body.size_hint(),
        }
    }
}

/// Native Rust fetch input; TS uses a WHATWG Request with a `ReadableStream` body.
pub type GuestStreamingRequest = http::Request<GuestBody>;
/// Native Rust fetch output; the body preserves terminal trailers and cancellation.
pub type GuestStreamingResponse = http::Response<GuestBody>;

// The JavaScript projection is generated here beside the native HTTP body contract.
pub(super) const JAVASCRIPT_FETCH_RESPONSE: &str = "\
/** Original streaming response. Trailers are terminal metadata, never initial headers.
 * The host must retain close through EOF, failure and cancellation. */
export type GuestResponseStream = { readonly status: number; readonly headers: Headers; readonly body: ReadableStream<Uint8Array> | null; readonly trailers: Promise<Headers>; close(): void; };\n\
export type GuestFetchResponse = Response | GuestResponseStream;\n";

impl GuestRequest {
    /// Convert the buffered portable projection to a native streaming request.
    pub fn into_streaming(self) -> Result<GuestStreamingRequest, http::Error> {
        let mut builder = http::Request::builder()
            .uri(self.url)
            .method(self.method.as_str());
        for (name, value) in self.headers {
            builder = builder.header(name, value);
        }
        builder.body(GuestBody::Buffered(self.body))
    }
}

impl GuestResponse {
    /// Convert buffered output without copying its Bytes allocation.
    pub fn into_streaming(self) -> Result<GuestStreamingResponse, http::Error> {
        let mut builder = http::Response::builder().status(self.status);
        for (name, value) in self.headers {
            builder = builder.header(name, value);
        }
        builder.body(GuestBody::Buffered(self.body))
    }
}
