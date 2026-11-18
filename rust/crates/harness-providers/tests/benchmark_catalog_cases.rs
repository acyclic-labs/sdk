//! Benchmark generator and production provider catalog conformance.

#[cfg(not(target_arch = "wasm32"))]
#[path = "benchmark_catalog.rs"]
mod benchmark_catalog;
