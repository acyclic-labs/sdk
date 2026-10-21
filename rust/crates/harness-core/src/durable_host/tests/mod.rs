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
#[cfg(all(test, feature = "filesystem"))]
use fixture::Fixture;
#[cfg(all(test, feature = "filesystem"))]
mod admission;
#[cfg(all(test, feature = "filesystem"))]
mod journal;
#[cfg(all(test, feature = "filesystem"))]
mod mail;
#[cfg(all(test, feature = "filesystem"))]
mod recovery;
#[cfg(all(test, feature = "filesystem"))]
mod timers;

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod release;

#[cfg(all(test, feature = "filesystem"))]
mod preparation;

#[cfg(all(test, feature = "filesystem"))]
pub(crate) mod created_fixture;
