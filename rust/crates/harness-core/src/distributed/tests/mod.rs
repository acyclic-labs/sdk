use super::*;
use crate::{
    Capabilities,
    conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    resources::ProviderRef,
    scheduler::{
        DurableOwner, EntrypointRef, OperationPhase, Orchestration, ParentLink, ResourceRequest,
    },
};
use acyclic_stream::MemoryStream;
use serde_json::Value;
use std::{collections::BTreeSet, sync::Arc};

mod fixtures;
use fixtures::{
    LostSessionAck, TestContentVerifier, declaration_scope, declare, grant, open, owner_issuer,
    publish, spec, start, state_ref, worker,
};
mod admission;
mod cancellation;
mod fencing;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod frame_cancellation;
#[cfg(feature = "filesystem")]
mod limits;
#[cfg(all(
    acyclic_harness_loom,
    acyclic_loom,
    feature = "filesystem",
    not(target_arch = "wasm32")
))]
mod model_checking;
#[cfg(all(
    acyclic_harness_loom,
    acyclic_loom,
    feature = "filesystem",
    not(target_arch = "wasm32")
))]
mod model_provider;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native;
mod observation;
mod origin_tests;
mod results;
#[cfg(all(
    acyclic_harness_loom,
    acyclic_loom,
    feature = "filesystem",
    not(target_arch = "wasm32")
))]
mod scheduler_model;
mod session;
