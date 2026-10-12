use std::{num::NonZeroU64, path::Path};
use crate::{wire, contract::ActorsFile};
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
mod export;
pub use export::*;

#[cfg(kani)]
mod formal_proofs;

#[cfg(test)]
#[allow(clippy::indexing_slicing, clippy::too_many_lines, reason = "grouped boundary cases")]
mod tests;
