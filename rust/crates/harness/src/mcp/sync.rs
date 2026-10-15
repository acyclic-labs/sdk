// MCP-owned atomics and their ownership use the same types in production and models.
// No locks are involved: Release publishes cancellation/bounds; Acquire observes them.
#[cfg(loom)]
pub(crate) use loom::sync::{Arc, atomic};
#[cfg(not(loom))]
pub(crate) use std::sync::{Arc, atomic};
