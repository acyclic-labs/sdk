//! Shared lifecycle, execution and workspace admission for machine adapters.

mod operations;
pub use operations::*;
mod files;
#[cfg(test)]
mod operation_race_tests;
pub use files::*;

mod driver;
pub use driver::*;
mod remote;
pub use remote::*;
#[cfg(test)]
mod remote_tests;

pub use crate::MutationIntent;

mod usage;
pub use usage::*;

mod execution;
pub use execution::*;
#[cfg(feature = "modal")]
mod duplex;
mod process_group;
pub use process_group::*;
#[cfg(any(feature = "e2b", feature = "modal"))]
mod instances;

#[cfg(feature = "provider-http")]
mod http;
#[cfg(feature = "provider-http")]
pub use http::http_transport_builder;

#[cfg(feature = "provider-http")]
mod framing;
#[cfg(feature = "provider-http")]
pub use framing::{Frame, Framing, bounded_sse, connect_message, frames};

#[cfg(feature = "e2b")]
pub mod e2b;
#[cfg(feature = "modal")]
pub mod modal;
#[cfg(feature = "modal")]
mod rpc;

#[cfg(feature = "cloudflare")]
pub mod cloudflare;
