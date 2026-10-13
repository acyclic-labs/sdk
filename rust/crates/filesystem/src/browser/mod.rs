//! Browser persistence providers for the same canonical Filesystem engine.
//!
//! These providers carry no browser binding facade and can be composed directly
//! with ordinary Rust consumers. Opening them grants no workspace authority.

#[cfg(any(test, target_arch = "wasm32"))]
mod authority_codec;
#[cfg(target_arch = "wasm32")]
mod indexed_db;
#[cfg(target_arch = "wasm32")]
pub use indexed_db::{IndexedDbAuthorityStore, IndexedDbObjectStore, IndexedDbOpenError};
