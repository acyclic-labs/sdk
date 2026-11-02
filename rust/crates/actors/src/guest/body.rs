use super::{GuestError, GuestRequest, GuestResponse};
use acyclic_stream::body::BodyChunk;
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

impl GuestBody {
    /// Preserve an admitted HTTP body's original per-fragment custody.
    ///
    /// Construction never polls the source. The caller supplies its ORIGINAL
    /// allocation admission: once for this adapter and once per data frame's
    /// fixed transport owner. No payload is collected or copied. Data, trailers,
    /// source failure and drop cancellation remain on the same original body;
    /// the first error retires the source and is returned exactly once.
    ///
    /// This is a handler-side projection, not request authorization or a new
    /// wire protocol. Pass it through the existing admitted fetch delivery.
    pub fn from_credited<B, C, F, A>(body: B, mut admit: F) -> Result<Self, GuestError>
    where
        B: Body<Data = BodyChunk<C>, Error = GuestError> + Send + Unpin + 'static,
        C: Send + 'static,
        F: FnMut(usize) -> Result<A, GuestError> + Send + Unpin + 'static,
        A: Send + Unpin + 'static,
    {
        let allocation = admit(std::mem::size_of::<CreditedBody<B, F, A>>())?;
        Ok(Self::Frames(Box::pin(CreditedBody {
            body: Some(body),
            admit,
            _allocation: allocation,
        })))
    }
}

struct CreditedBody<B, F, A> {
    // Retire the source and its original owner before the adapter allocation.
    body: Option<B>,
    admit: F,
    _allocation: A,
}

impl<B, C, F, A> Body for CreditedBody<B, F, A>
where
    B: Body<Data = BodyChunk<C>, Error = GuestError> + Unpin,
    C: Send + 'static,
    F: FnMut(usize) -> Result<A, GuestError> + Unpin,
    A: Send + Unpin + 'static,
{
    type Data = Bytes;
    type Error = GuestError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, GuestError>>> {
        let this = self.get_mut();
        let Some(body) = this.body.as_mut() else {
            return Poll::Ready(None);
        };
        let result = match std::task::ready!(Pin::new(body).poll_frame(cx)) {
            Some(Ok(frame)) => match frame.into_data() {
                Ok(chunk) => chunk.try_into_bytes(&mut this.admit).map(Frame::data),
                Err(frame) => {
                    // `into_data` returned this same non-data frame, so the
                    // mapping closure is never called. Preserve its metadata.
                    return Poll::Ready(Some(Ok(frame.map_data(|_| Bytes::new()))));
                }
            },
            Some(Err(error)) => Err(error),
            None => {
                this.body = None;
                return Poll::Ready(None);
            }
        };
        if result.is_err() {
            this.body = None;
        }
        Poll::Ready(Some(result))
    }

    fn is_end_stream(&self) -> bool {
        self.body.as_ref().is_none_or(Body::is_end_stream)
    }

    fn size_hint(&self) -> SizeHint {
        self.body
            .as_ref()
            .map_or_else(|| SizeHint::with_exact(0), Body::size_hint)
    }
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
