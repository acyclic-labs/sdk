//! The filesystem crate's numeric admission harness over the shared codec.

mod acyclic_fs_base {
    #[path = "../../crates/filesystem-base/src/numeric.rs"]
    pub mod numeric;
}

#[path = "../../crates/filesystem/src/kani_proofs.rs"]
mod production;
