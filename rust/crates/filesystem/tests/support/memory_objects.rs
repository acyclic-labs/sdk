use acyclic_fs::{LogicalObjectStore, MemoryObjectBackend};
use acyclic_objects::MemoryObjects;
use std::sync::Arc;

pub(super) fn memory_objects() -> MemoryObjectBackend {
    LogicalObjectStore::new(Arc::new(MemoryObjects::memory()))
}
