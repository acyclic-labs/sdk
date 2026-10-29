//! Filesystem metadata changes over ordinary credential-bound Streams follows.
mod resource;
pub use resource::resource;
#[cfg(feature = "session")]
mod resources;
#[cfg(feature = "session")]
pub use resources::ChangeProvider;
