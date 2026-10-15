mod socket_validation;
pub use socket_validation::*;
mod socket_address;
pub use socket_address::*;
mod socket_resources;
pub use socket_resources::*;
mod socket_enums;
pub use socket_enums::*;
mod socket_values;
pub use socket_values::*;
mod socket_capability;
pub use socket_capability::*;
#[path = "socket_payload.rs"]
mod socket_payload_types;
pub use socket_payload_types::*;
mod socket_effects;
pub use socket_effects::*;
mod socket_effect_union;
pub use socket_effect_union::*;
mod socket_event_union;
pub use socket_event_union::*;
mod socket_events;
use crate::{contract::ActorsFile, wire};
pub use socket_events::*;
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

#[cfg(kani)]
mod formal_proofs;

#[cfg(test)]
#[allow(
    clippy::indexing_slicing,
    clippy::too_many_lines,
    reason = "grouped boundary cases"
)]
mod tests;
