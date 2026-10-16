//! Browser and Node holders use the same authenticated verifier as hosts.
mod snapshot;
#[cfg(target_arch = "wasm32")]
mod wasm;

pub use snapshot::{Snapshot, block_ids};
