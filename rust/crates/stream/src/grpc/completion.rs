/// A single owned operation, retaining its creating dispatch even if its span is filtered.
struct Completion {
    span: obs::OwnedSpan,
    finished: bool,
}

impl Completion {
    fn new(span: tracing::Span) -> Self {
        Self {
            span: obs::OwnedSpan::new(span),
            finished: false,
        }
    }
    fn finish(&mut self, kind: Option<&'static str>, code: Option<Code>) {
        if self.finished {
            return;
        }
        self.finished = true;
        match kind {
            Some(kind) => obs::failed(&self.span, kind),
            None => obs::record(&self.span, "outcome", "ok"),
        }
        if let Some(code) = code {
            obs::record(&self.span, "rpc.code", code as u64);
        }
    }
    fn ok(&mut self) {
        self.finish(None, Some(Code::Ok));
    }
    fn status(&mut self, error: &Status) {
        self.finish(
            Some(if error.code() == Code::Cancelled {
                "cancelled"
            } else {
                status(error).code()
            }),
            Some(error.code()),
        );
    }
    fn invalid_response(&mut self) {
        // This is local semantic rejection, not an observed gRPC trailer status.
        self.finish(Some("invalid_response"), None);
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        self.finish(Some("cancelled"), None);
    }
}
