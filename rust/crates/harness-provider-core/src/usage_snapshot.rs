use crate::sync::Mutex;

/// One generation's reported input, as the provider counted it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Usage {
    /// The request input the count covers.
    pub input: acyclic_harness_core::model::InputAnchor,
    /// The provider's reported input tokens for that anchor.
    pub tokens: u64,
}

/// The snapshot lock was poisoned by a panicking holder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Poisoned;

/// Standalone lock: never acquire another lock or perform I/O while holding it.
#[derive(Default)]
pub struct Snapshot(Mutex<Option<Usage>>);
impl Snapshot {
    /// The last published usage, or `None` before the first observation.
    pub fn get(&self) -> Result<Option<Usage>, Poisoned> {
        self.0
            .lock()
            .map(|snapshot| *snapshot)
            .map_err(|_| Poisoned)
    }
    /// Publishes USAGE as the last observation.
    pub fn set(&self, usage: Usage) -> Result<(), Poisoned> {
        *self.0.lock().map_err(|_| Poisoned)? = Some(usage);
        Ok(())
    }
}
