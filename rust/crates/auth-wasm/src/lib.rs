//! Browser and Node holders use the same authenticated verifier as hosts.
#[cfg(target_arch = "wasm32")]
mod addressing;
#[cfg(target_arch = "wasm32")]
mod secrets;
#[cfg(target_arch = "wasm32")]
mod wasm;
