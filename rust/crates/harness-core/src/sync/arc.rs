#[cfg(loom)]
#[doc(hidden)]
pub use loom::sync::Arc;
#[cfg(not(loom))]
#[doc(hidden)]
pub use std::sync::Arc;
