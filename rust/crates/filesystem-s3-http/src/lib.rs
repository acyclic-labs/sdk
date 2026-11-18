//! S3 HTTP adapter over Acyclic filesystem workspaces.

#[cfg(not(target_arch = "wasm32"))]
pub mod s3_http;
