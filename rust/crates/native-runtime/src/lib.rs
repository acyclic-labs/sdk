//! Native primitives and the pure canonical customer-held account codec.
#![doc = include_str!("../README.md")]

/// Canonical own-leaf holder signing shared by native and browser bindings.
#[cfg(feature = "account-holder")]
pub mod account;

/// OS-sealed customer leaf and distinct SQL-session custody; no file fallback.
#[cfg(all(feature = "customer-custody", any(windows, target_os = "macos", target_os = "linux")))]
pub mod customer_custody;

// Keep the native implementation at crate scope (including private tracing macros
// and platform helpers), while browser holders compile only the pure account codec.
#[cfg(not(target_arch = "wasm32"))]
include!("host.rs");
