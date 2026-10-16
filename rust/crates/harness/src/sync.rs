// Task-volume publication uses the same exclusion in production and models.
// The model-only cfg also instruments the backing Stream provider's locks.
#[cfg(acyclic_loom)]
pub(crate) use acyclic_stream::query_sync::Mutex;
#[cfg(not(acyclic_loom))]
pub(crate) use tokio::sync::Mutex;
