//! The filesystem crate's numeric admission harness over the shared codec.

#[path = "../../crates/filesystem-base/src/numeric.rs"]
pub mod numeric;

mod acyclic_fs_base {
    pub use super::numeric;
}

#[path = "../../crates/filesystem/src/kani_proofs.rs"]
mod production;
