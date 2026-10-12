use super::*;

/// One gapless committed task-inbox item.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub struct InboxItem {
    /// Owning task.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub task_id: TaskId,
    /// One-based sequence.
    pub sequence: u64,
    /// Sender's idempotency identity.
    pub message_id: String,
    /// Pinned payload staged before publication.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub payload: FileRef,
}
/// Local projection of an inbox whose items are durable in Stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskInbox {
    task_id: TaskId,
    items: Vec<InboxItem>,
}
impl TaskInbox {
    /// Creates an empty inbox for one task.
    #[must_use]
    pub const fn new(task_id: TaskId) -> Self {
        Self {
            task_id,
            items: Vec::new(),
        }
    }
    /// Applies a committed item with exact retry and sequence checks.
    pub fn apply(&mut self, item: InboxItem) -> Result<()> {
        if item.task_id != self.task_id || item.message_id.trim().is_empty() {
            return Err(Error::Invalid(
                "inbox item has the wrong task or an empty message identity".into(),
            ));
        }
        item.payload.validate(&mut None)?;
        if let Some(existing) = self
            .items
            .iter()
            .find(|existing| existing.message_id == item.message_id)
        {
            return if existing == &item {
                Ok(())
            } else {
                Err(Error::Conflict("inbox message identity reused".into()))
            };
        }
        let expected = self.items.len() as u64 + 1;
        if item.sequence != expected {
            return Err(Error::Conflict(format!(
                "expected inbox sequence {expected}"
            )));
        }
        self.items.push(item);
        Ok(())
    }
    /// Reads a bounded page after a sequence.
    #[must_use]
    pub fn after(&self, sequence: u64, limit: usize) -> Vec<InboxItem> {
        self.items
            .iter()
            .skip(usize::try_from(sequence).unwrap_or(usize::MAX))
            .take(limit)
            .cloned()
            .collect()
    }
}
