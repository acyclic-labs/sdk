use crate::{contract::ActorsFile, wire};
use std::{num::NonZeroU64, path::Path};
use ts_rs::{Config, ExportError, TS, TypeVisitor};

mod identity;
pub use identity::*;
mod configuration;
pub use configuration::*;
mod observations;
pub use observations::*;
mod mutations;
pub use mutations::*;
mod invocation;
pub use invocation::*;
/// TypeScript declaration exports.
pub mod export;
mod scope;

#[cfg(kani)]
mod formal_proofs;

#[cfg(test)]
#[allow(
    clippy::indexing_slicing,
    clippy::too_many_lines,
    reason = "grouped boundary cases"
)]
mod tests;

mod worker;
pub use worker::*;

mod secrets;
pub use secrets::*;

#[cfg(test)]
mod secret_tests;
