use super::ReleaseSender;

pub(super) struct Released {
    pub id: u64,
    pub generation: u64,
    pub node: String,
    pub epoch: u64,
    pub bytes: u64,
}

// The delivery owns its credit, including while buffered or being discarded.
// No lock is acquired and Drop never waits on the bounded command queue.
pub(super) struct Credit {
    sender: ReleaseSender,
    value: Option<Released>,
}
impl Credit {
    pub(super) fn new(sender: ReleaseSender, value: Released) -> Self {
        Self {
            sender,
            value: Some(value),
        }
    }
}
impl Drop for Credit {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            let _ = self.sender.send(value);
        }
    }
}
