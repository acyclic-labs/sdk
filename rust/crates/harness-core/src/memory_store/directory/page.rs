/// One entry in a bounded directory page.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, crate::HeapLayout)]
pub(crate) struct MemoryStoreEntry {
    pub(crate) name: String,
    pub(crate) kind: MemoryStoreEntryKind,
}

#[derive(
    Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, crate::HeapLayout,
)]
#[serde(rename_all = "lowercase")]
pub(crate) enum MemoryStoreEntryKind {
    File,
    Directory,
}

/// A generation-pinned page produced by the shared content core.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, crate::HeapLayout)]
#[serde(rename_all = "camelCase")]
#[doc(hidden)]
pub struct MemoryStorePage {
    pub(crate) generation: u64,
    pub(crate) entries: Vec<MemoryStoreEntry>,
    pub(crate) has_more: bool,
}

#[path = "page/index.rs"]
mod index;
pub(crate) use index::DirectoryIndex;

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
#[path = "page/tests.rs"]
mod tests;

#[cfg(all(feature = "test-support", feature = "filesystem"))]
#[path = "page/fuzzing.rs"]
pub(crate) mod fuzzing;
#[cfg(any(
    all(test, not(target_arch = "wasm32")),
    all(feature = "test-support", feature = "filesystem")
))]
#[path = "page/reference.rs"]
mod reference;
