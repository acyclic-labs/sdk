use super::{Empty, OperationSpan};
use acyclic_observability::Completion;
use std::io;

/// Optional observation owned by the original capture handler. The original
/// completion callback supplies cleanup evidence; losing that producer remains
/// incomplete even when another diagnostic span handle survives.
///
/// This supplies no authenticated Binding, final backing deallocation, inherited
/// handle closure or outstanding kernel I/O retirement certificate.
pub struct CaptureObservation {
    completion: Option<Completion>,
    span: OperationSpan,
}

impl CaptureObservation {
    /// Creates one operation on the caller's existing subscriber. Disabled
    /// observation allocates nothing and reads no clock. No subscriber, worker,
    /// queue, timer or additional completion channel is installed.
    #[must_use]
    pub fn new() -> Self {
        let span = super::span!(
            INFO,
            "acyclic.runtime.capture",
            capture.cleanup_completed = Empty,
            error.os = Empty,
            outcome = Empty,
            error.kind = Empty
        );
        Self {
            completion: Some(Completion::new(&span)),
            span,
        }
    }

    /// The original handler's context. An admitted caller may attach its actual
    /// caller-funded Binding here; process IDs and invocation strings cannot
    /// substitute for that admission. Retaining this span owns no physical work.
    pub fn context(&self) -> &OperationSpan {
        &self.span
    }

    pub(crate) fn retired(&mut self, cleanup_completed: bool, error: Option<&io::Error>) {
        let Some(completion) = self.completion.take() else {
            return;
        };
        if !self.span.is_disabled() {
            self.span
                .record("capture.cleanup_completed", cleanup_completed);
            if let Some(code) = error.and_then(io::Error::raw_os_error) {
                self.span.record("error.os", code);
            }
        }
        // Capture success describes the native I/O carrier, independently of
        // the selected command's exit status and the cleanup flag.
        completion.complete((), |_| error.map(super::kind));
    }
}

impl Default for CaptureObservation {
    fn default() -> Self {
        Self::new()
    }
}
