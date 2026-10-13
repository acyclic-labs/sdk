struct FollowState {
    provider: LocalStream,
    path: StreamPath,
    next: u64,
    changed: watch::Receiver<u64>,
    done: bool,
    span: obs::OwnedSpan,
}
impl FollowState {
    fn failed(&mut self, error: &StreamError) {
        self.done = true;
        self.span.record("terminal", "error");
        obs::failed(&self.span, error.code());
    }
}
impl Drop for FollowState {
    fn drop(&mut self) {
        if !self.done {
            self.span.record("terminal", "dropped");
        }
    }
}
