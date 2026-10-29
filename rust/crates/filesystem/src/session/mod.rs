//! Filesystem metadata changes over ordinary credential-bound Streams follows.
mod resource;
pub use resource::resource;
#[cfg(all(feature = "session", not(target_arch = "wasm32")))]
mod resources;
#[cfg(all(feature = "session", not(target_arch = "wasm32")))]
pub use resources::ChangeProvider;
