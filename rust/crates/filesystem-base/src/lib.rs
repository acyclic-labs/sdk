//! Primitives at the base of the Acyclic filesystem crates.
//!
//! These modules depend on no other filesystem module, so they compile once and
//! ahead of `acyclic-fs`. Use them through `acyclic-fs`, which re-exports them
//! at their original paths.
#![deny(unsafe_code)]

pub mod numeric;
/// Readiness bookkeeping shared by native namespace adapters.
pub mod ready;
pub mod text_merge;
