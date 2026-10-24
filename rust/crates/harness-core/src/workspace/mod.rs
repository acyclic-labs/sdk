//! Task-selected native views and their filesystem access decisions.
mod composition;
mod discovery;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
mod reconcile;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
mod reserved;
pub use acyclic_fs::access::MountMode;
pub use composition::{Mount, MountTable, VolumeView, compose};
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub use reconcile::reconcile_reserved_names;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub use reserved::RESERVED_NAMES;
#[cfg(all(
    test,
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub(crate) use reserved::sdk_owned;
