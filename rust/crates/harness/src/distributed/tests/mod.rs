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
#[cfg(feature = "filesystem")]
mod limits;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native;
mod observation;
mod results;
mod session;
