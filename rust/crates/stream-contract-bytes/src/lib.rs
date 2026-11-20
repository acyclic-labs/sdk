//! Byte-level helpers for the stream contract: JavaScript snapshots and the native test meter.
#[cfg(all(feature = "js-bytes", target_arch = "wasm32"))]
pub mod javascript;
#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "test-support")))]
#[doc(hidden)]
pub mod test_meter;
