use super::ClientRenderer;
use std::time::Duration;
use tokio::time::Instant;

pub(super) struct Pacer {
    deadline: Instant,
    dirty: bool,
}

impl Default for Pacer {
    fn default() -> Self {
        Self {
            deadline: Instant::now(),
            dirty: false,
        }
    }
}

impl ClientRenderer {
    /// Wait for the next presentation slot without polling an idle terminal.
    pub async fn wait_frame(&self) {
        tokio::time::sleep_until(self.pacing.deadline).await;
    }

    /// Successful presentation starts the next 60 Hz interval. Failed output
    /// never consumes a slot or clears a pending publication.
    pub fn frame_presented(&mut self) {
        self.pacing.deadline = Instant::now() + Duration::from_nanos(16_666_667);
        self.pacing.dirty = false;
    }

    pub(crate) fn request_frame(&mut self) {
        self.pacing.dirty = true;
    }

    pub(crate) fn frame_deadline(&self) -> Option<Instant> {
        self.pacing.dirty.then_some(self.pacing.deadline)
    }

    pub(crate) fn frame_ready(&mut self) {
        self.pacing.dirty = false;
    }
}
