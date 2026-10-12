use super::*;

/// A single owned operation, retaining its creating dispatch even if its span is filtered.
pub(super) struct Completion {
    pub(super) span: obs::OperationSpan,
    pub(super) finished: bool,
}

impl Completion {
    pub(super) fn new(span: tracing::Span) -> Self {
        Self {
            span: obs::OperationSpan::new(span),
            finished: false,
        }
    }
    pub(super) fn finish(&mut self, kind: Option<&'static str>, code: Option<Code>) {
        if self.finished {
            return;
        }
        self.finished = true;
        match kind {
            Some(kind) => obs::failed(&self.span, kind),
            None => {
                self.span.record("outcome", "ok");
            }
        }
        if let Some(code) = code {
            self.span.record("rpc.code", code as u64);
        }
    }
    pub(super) fn ok(&mut self) {
        self.finish(None, Some(Code::Ok));
    }
    pub(super) fn status(&mut self, error: &Status) {
        self.finish(
            Some(if error.code() == Code::Cancelled {
                "cancelled"
            } else {
                status(error).code()
            }),
            Some(error.code()),
        );
    }
    pub(super) fn invalid_response(&mut self) {
        // This is local semantic rejection, not an observed gRPC trailer status.
        self.finish(Some("invalid_response"), None);
    }
}
impl Drop for Completion {
    fn drop(&mut self) {
        self.finish(Some("cancelled"), None);
    }
}

pub(super) fn decode_response<T, U>(
    (body, _, mut completion): (T, usize, Completion),
    decode: impl FnOnce(T) -> Result<U, StreamError>,
) -> Result<U, StreamError> {
    // Tonic unary completion already observed the successful trailers.
    completion.span.record("rpc.code", Code::Ok as u64);
    let result = decode(body);
    match &result {
        Ok(_) => completion.ok(),
        Err(_) => completion.invalid_response(),
    }
    result
}

pub(super) fn observe_body<S, T>(
    span: &tracing::Span,
    body: S,
) -> impl Stream<Item = Result<T, Status>> + Unpin + use<S, T>
where
    S: Stream<Item = Result<T, Status>> + Unpin,
{
    let completion = Completion::new(span.clone());
    // Drop the producer under its origin before retiring completion.
    let mut state = (completion.span.scope(body), completion);
    stream::poll_fn(move |cx| {
        let (body, completion) = &mut state;
        if completion.finished {
            return Poll::Ready(None);
        }
        let next = Pin::new(body).poll_next(cx);
        match &next {
            Poll::Ready(Some(Err(error))) => completion.status(error),
            Poll::Ready(None) => completion.ok(),
            _ => {}
        }
        next
    })
}
