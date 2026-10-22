//! Durable host behavior tests.
use super::*;
use crate::conversation::{Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef};
use crate::core::{AggregateKind, AuthorityIssuer};
use crate::resources::ProviderRef;
use crate::runtime::TaskSpawner;
#[cfg(test)]
use crate::runtime::{DurableTaskHost, TaskStateProvider};
use crate::workflow::{
    MachineIdentity, MachineRegistry, MachineStatus, MachineTransition, ResumableMachine,
};
use crate::{AgentId, Capabilities, GroupId};
use acyclic_stream::{MemoryStream, SystemUnixMillisClock};
use serde_json::json;
#[cfg(feature = "filesystem")]
#[doc(hidden)]
pub mod fixture;
#[cfg(test)]
#[cfg(feature = "filesystem")]
use fixture::Fixture;
#[cfg(test)]
#[cfg(feature = "filesystem")]
mod admission;
#[cfg(test)]
#[cfg(feature = "filesystem")]
mod journal;
#[cfg(test)]
#[cfg(feature = "filesystem")]
mod mail;
#[cfg(test)]
#[cfg(feature = "filesystem")]
mod recovery;
#[cfg(test)]
#[cfg(feature = "filesystem")]
mod timers;

#[cfg(test)]
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod release;

#[cfg(test)]
#[cfg(feature = "filesystem")]
mod preparation;

#[cfg(test)]
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
mod readonly;

#[cfg(test)]
#[cfg(feature = "filesystem")]
pub(crate) mod created_fixture;
