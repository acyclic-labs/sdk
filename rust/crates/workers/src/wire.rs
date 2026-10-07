//! Public Workers v1 wire surface.
//!
//! The contract declarations and schema renderer live in [`crate::contract`].
//! The tonic transport adapter is generated from that contract and included
//! here so existing `wire::workers_service_*` paths remain stable.

mod generated {
    #![allow(
        missing_docs,
        reason = "generated from the Rust-owned Workers contract"
    )]
    #![allow(clippy::all, clippy::pedantic, reason = "generated protobuf bindings")]

    include!(concat!(env!("OUT_DIR"), "/rust/acyclic.workers.v1.rs"));
}

pub use generated::*;
