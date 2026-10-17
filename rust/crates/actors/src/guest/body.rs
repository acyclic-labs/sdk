use super::{GuestError, GuestRequest, GuestResponse};
use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;

/// Native streaming fetch body, including ongoing SSE; buffered bytes remain convenient.
pub enum GuestBody {
    /// One already-buffered byte sequence.
    Buffered(Bytes),
    /// Host-driven chunks with backpressure and cancellation on stream drop.
    Stream(Pin<Box<dyn Stream<Item = Result<Bytes, GuestError>> + Send>>),
}

/// Native Rust fetch input; TS uses a WHATWG Request with a ReadableStream body.
pub type GuestStreamingRequest = http::Request<GuestBody>;
/// Native Rust fetch output; TS uses a WHATWG Response, including SSE streams.
pub type GuestStreamingResponse = http::Response<GuestBody>;

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
