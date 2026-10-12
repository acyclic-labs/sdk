//! IndexedDB persistence for the canonical Filesystem engine.
//!
//! Both stores use the sole version-1 schema and reject obsolete databases
//! without deleting or migrating them.

#[cfg(any(test, target_arch = "wasm32"))]
mod authority_codec;
#[cfg(target_arch = "wasm32")]
mod indexed_db;
#[cfg(target_arch = "wasm32")]
pub use indexed_db::{IndexedDbAuthorityStore, IndexedDbObjectStore, IndexedDbOpenError};
