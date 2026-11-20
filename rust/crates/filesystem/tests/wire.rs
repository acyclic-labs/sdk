//! Wire, S3 and source-lifecycle contracts.
#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(loom))]
acyclic_test_support::counting_allocator!();

#[path = "s3_contracts.rs"]
mod s3_contracts;
#[path = "source_lifecycle.rs"]
mod source_lifecycle;
#[path = "wire_contracts.rs"]
mod wire_contracts;
#[path = "wire_error_contracts.rs"]
mod wire_error_contracts;
#[path = "wire_rpc.rs"]
mod wire_rpc;
