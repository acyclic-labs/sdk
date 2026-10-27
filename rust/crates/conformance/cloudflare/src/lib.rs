//! The shared conformance entry points, called inside a SQLite Durable Object.

#[cfg(target_arch = "wasm32")]
mod hosting;
#[cfg(target_arch = "wasm32")]
mod wasm;
