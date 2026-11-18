use acyclic_fs::MemoryObjectStore;

pub(super) fn memory_objects() -> MemoryObjectStore {
    MemoryObjectStore::default()
}
