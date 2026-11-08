//! Existing integration scenarios share one executable entrypoint.

#[cfg(feature = "local-runner")]
#[path = "../src/bin/qualify/tools/benchmark_access.rs"]
mod benchmark_access;

#[cfg(feature = "inference")]
#[path = "inference.rs"]
mod inference;

#[path = "runner_cli.rs"]
mod runner_cli;

#[cfg(not(target_arch = "wasm32"))]
#[path = "../benches/local_hotpath/durability.rs"]
mod native_projection;

#[cfg(feature = "local-runner")]
#[path = "restore_gen.rs"]
mod restore_gen;
