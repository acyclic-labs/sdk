//! The crate the execution-root check compiles: its `OUT_DIR` is its build script's.

execroot_check::assert_in_execution_root!();

/// The build script's file, read back through `OUT_DIR`.
pub const PROBE: &str = include_str!(concat!(env!("OUT_DIR"), "/probe.txt"));
