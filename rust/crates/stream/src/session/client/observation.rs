use super::super::runtime::OperationSpan;
use crate::StreamError;
use tracing::field::Empty;

pub(super) struct Observation {
    pub(super) operation: OperationSpan,
    finished: bool,
}
impl Observation {
    pub(super) fn channel(id: u64) -> Self {
        Self::new(tracing::info_span!(
            "stream.session.channel",
            channel.id = id,
            outcome = Empty,
            error.kind = Empty,
            registration = "pending",
            generation = Empty
        ))
    }
    pub(super) fn session(attempt: u32) -> Self {
        Self {
            operation: OperationSpan::new_root(tracing::info_span!(
            parent: None,
            "stream.session",
            attempt,
            outcome = Empty,
            error.kind = Empty
            )),
            finished: false,
        }
    }
    fn new(span: tracing::Span) -> Self {
        Self {
            operation: OperationSpan::new(span),
            finished: false,
        }
    }
    pub(super) fn finish(&mut self, error: Option<&StreamError>) {
        if !self.finished {
            self.finished = true;
            self.operation.in_scope(|| {
                let span = &self.operation;
                match error {
                    Some(error) => {
                        span.record("outcome", "err")
                            .record("error.kind", error.code());
                    }
                    None => {
                        span.record("outcome", "closed");
                    }
                }
            });
        }
    }
}
impl Drop for Observation {
    fn drop(&mut self) {
        if !self.finished {
            self.operation.in_scope(|| {
                self.operation.record("outcome", "cancelled");
            });
        }
    }
}
