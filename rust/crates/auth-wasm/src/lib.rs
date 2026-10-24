//! Browser and Node holders use the same authenticated verifier as hosts.
#[cfg(target_arch = "wasm32")]
mod wasm;
#[cfg(target_arch = "wasm32")]
mod addressing;

pub use acyclic_auth::trust::{Snapshot, block_ids};
