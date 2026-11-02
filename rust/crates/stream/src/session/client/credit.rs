pub(super) struct Released {
    pub id: u64,
    pub generation: u64,
    pub node: String,
    pub epoch: u64,
    pub bytes: u64,
}

// The delivery owns its credit, including while buffered or being discarded.
// Drop transfers the observation through the shared release signal rather
// than waiting on the bounded command queue.
pub(super) type Credit = crate::runtime::signal::Guard<Released>;
