/// Reads are caller-owned; cancellation while waiting for visibility is terminal.
struct ReadObservation {
    span: obs::OwnedSpan,
    finished: bool,
}
impl Drop for ReadObservation {
    fn drop(&mut self) {
        if !self.finished {
            obs::failed(&self.span, "cancelled");
        }
    }
}
