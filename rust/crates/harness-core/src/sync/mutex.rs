#[cfg(loom)]
#[doc(hidden)]
pub use loom::sync::Mutex;
#[cfg(not(loom))]
#[doc(hidden)]
pub use std::sync::Mutex;
