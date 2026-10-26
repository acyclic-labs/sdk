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
mod process_group;
pub use process_group::*;

#[cfg(feature = "provider-http")]
mod http;
#[cfg(feature = "provider-http")]
pub use http::http_transport_builder;

#[cfg(feature = "e2b")]
pub mod e2b;
#[cfg(feature = "modal")]
pub mod modal;

#[cfg(feature = "cloudflare")]
pub mod cloudflare;
