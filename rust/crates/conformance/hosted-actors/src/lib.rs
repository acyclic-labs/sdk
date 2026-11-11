//! One deployed Actors target for every host. The original provider and remote
//! owner controls run the same public suite and canonical reference package.

#[cfg(target_arch = "wasm32")]
mod wasm;
#[cfg(target_arch = "wasm32")]
pub use wasm::{ActorsDomain, ActorsPackage};
