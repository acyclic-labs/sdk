//! Browser persistence and WebAssembly bindings for the canonical Rust engine.

#[cfg(target_arch = "wasm32")]
mod opfs;

#[cfg(target_arch = "wasm32")]
pub use acyclic_fs::browser::{IndexedDbAuthorityStore, IndexedDbObjectStore, IndexedDbOpenError};

#[cfg(target_arch = "wasm32")]
pub use opfs::{OpfsAcceleratedObjectStore, OpfsOpenError};

#[cfg(target_arch = "wasm32")]
#[allow(missing_docs, reason = "members mirror the documented acyclic-fs API")]
mod bindings;

#[cfg(target_arch = "wasm32")]
pub use bindings::{
    BrowserCheckout, BrowserFs, BrowserOperationWindowCoordinator, BrowserVolume,
    BrowserWorkspaceContextRegistry, decode_merge_candidate_json, decode_merge_plan_json,
    decode_multi_root_candidate_json, decode_multi_root_plan_json, decode_publication_json,
    encode_merge_candidate_json, encode_merge_plan_json, encode_multi_root_candidate_json,
    encode_multi_root_plan_json, encode_publication_json, open_browser_fs, open_memory_fs,
};
