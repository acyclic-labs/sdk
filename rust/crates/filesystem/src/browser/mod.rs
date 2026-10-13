//! Browser persistence providers for the same canonical Filesystem engine.
//!
//! These providers carry no browser binding facade and can be composed directly
//! with ordinary Rust consumers. Opening them grants no workspace authority.
//! Both providers use the sole version-1 schema, rejecting obsolete databases
//! without deleting them or attempting an implicit migration.
//! The obsolete-version browser control commits a real sentinel record before
//! both rejected opens and reads it unchanged after reopening the original schema.
//! Original DOM open failures retain their exception identity through the public
//! WASM facade, including `VersionError`; rejection is not a migration request.
//!
//! Object payloads use `acyclic_stream::OwnedBytes`: ordinary WASM vectors move
//! into their carrier without a second Rust byte copy. Work counters bound SDK
//! buffers and retained Blob-handle vectors before materialization; opaque
//! browser/provider allocations are not native physical-quota receipts.
//! The canonical authority codec and its portable protocol controls also remain
//! available to native unit tests without compiling browser database fixtures.

#[cfg(any(test, target_arch = "wasm32"))]
mod authority_codec;
#[cfg(target_arch = "wasm32")]
mod indexed_db;
#[cfg(target_arch = "wasm32")]
pub use indexed_db::{IndexedDbAuthorityStore, IndexedDbObjectStore, IndexedDbOpenError};
