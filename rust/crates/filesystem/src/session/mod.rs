//! Filesystem metadata changes over ordinary credential-bound Streams follows.
#[cfg(all(feature = "session", not(target_arch = "wasm32")))]
mod resources;
#[cfg(all(feature = "session", not(target_arch = "wasm32")))]
pub use resources::{ChangeProvider, resource};
