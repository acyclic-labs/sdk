//! Shared unary response policy and maintained browser gRPC-Web transport.
pub mod limits;
#[cfg(any(target_arch = "wasm32", test))]
mod abort_guard;
#[cfg(any(target_arch = "wasm32", test))]
mod body_stream;
#[cfg(any(target_arch = "wasm32", test))]
mod call;
#[cfg(any(target_arch = "wasm32", test))]
mod client;
#[cfg(any(target_arch = "wasm32", test))]
mod content_type;
#[cfg(any(target_arch = "wasm32", test))]
mod error;
#[cfg(any(target_arch = "wasm32", test))]
mod fetch;
#[cfg(any(target_arch = "wasm32", test))]
pub mod options;
#[cfg(any(target_arch = "wasm32", test))]
mod response_body;
#[cfg(test)]
mod test_support;

#[cfg(any(target_arch = "wasm32", test))]
pub use self::{client::Client, error::Error, response_body::ResponseBody};

#[cfg(test)]
mod split_trailer_tests;
