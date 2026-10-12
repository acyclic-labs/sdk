//! Durable host behavior tests.
use super::*;
use crate::conversation::{Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef};
use crate::core::{AggregateKind, AuthorityIssuer};
use crate::resources::ProviderRef;
use crate::runtime::{DurableTaskHost, TaskSpawner, TaskStateProvider};
use crate::workflow::{
    MachineIdentity, MachineRegistry, MachineStatus, MachineTransition, ResumableMachine,
};
use crate::{AgentId, Capabilities, GroupId};
use acyclic_stream::{MemoryStream, SystemUnixMillisClock};
use serde_json::json;
#[cfg(feature = "filesystem")]
pub(crate) mod fixture;
#[cfg(feature = "filesystem")]
use fixture::Fixture;
#[cfg(feature = "filesystem")]
mod admission;
#[cfg(feature = "filesystem")]
mod journal;
#[cfg(feature = "filesystem")]
mod mail;
#[cfg(feature = "filesystem")]
mod recovery;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod release;
