//! Public Actors v1 wire surface.
//!
//! The contract declarations and schema renderer live in [`crate::contract`].
//! The tonic transport adapter is generated from that contract and included
//! here so existing `wire::actors_service_*` paths remain stable.

pub use crate::contract::*;

include!(concat!(env!("OUT_DIR"), "/rust/acyclic.actors.v1.rs"));
