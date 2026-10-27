//! Bounded hypotheses over immutable authoritative references.

#![doc = include_str!("../../docs/client/README.md")]

use serde::{Deserialize, Serialize};
pub use acyclic_harness_core::preset::Preset;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

mod identity;
pub use identity::{BranchId, Dependency, DependencyRequirement};
mod limits;
pub use limits::{Error, Limits};
mod outcome;
pub use outcome::{Correspondence, OperationOutcome, PredictionOutcome};
mod domain;
pub use domain::{Domain, Fact, Observation};
mod hypothesis;
pub use hypothesis::{Begin, Hypothesis, Provenance, View};
mod kernel;
pub use kernel::Client;
mod command_result;
pub use command_result::{HeadlessOutcome, encode_command_result};
mod mentions;
pub use mentions::*;
mod diff;
pub use diff::*;
mod task_list;
pub use task_list::{TaskItem, TaskList, TaskState, parse_task_list};
mod interests;
#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub use acyclic_harness_client::{
    AgentClient, AgentClientError, AgentEvent, AgentSelection, StreamView, StreamViewPolicy,
    WireSession, WireSessionError,
};
pub use acyclic_harness_client::{
    CREDENTIAL_ENV, ConnectionConfigError, ConnectionOptions, DEFAULT_ENDPOINT, DemandError,
    DemandPolicy, ENDPOINT_ENV, PageDirection, PageRequest, PageWindow, PageWindowError,
    PageWindowPolicy, ViewDemand, WindowPage, resolve_connection_options,
};
pub use interests::{ClientInterests, InterestChanges, InterestTier};
mod offline;
pub use offline::validate_offline_request;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod wasm;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub use wasm::WasmHypothesisClient;

type Record<D> = Fact<<D as Domain>::Key, <D as Domain>::Basis, <D as Domain>::Value>;
/// Persisted data specialized to a domain; never authoritative evidence.
pub type ClientHypothesis<D> = Hypothesis<
    <D as Domain>::Key,
    <D as Domain>::Basis,
    <D as Domain>::Operation,
    <D as Domain>::Assumption,
    <D as Domain>::Value,
>;
type Request<D> = Begin<
    <D as Domain>::Key,
    <D as Domain>::Basis,
    <D as Domain>::Operation,
    <D as Domain>::Assumption,
    <D as Domain>::Value,
>;

/// Evidence result specialized to a typed domain adapter.
pub type DomainObservation<D> = Observation<
    <D as Domain>::Key,
    <D as Domain>::Basis,
    <D as Domain>::Operation,
    <D as Domain>::Value,
>;

/// Narrow observation notification and accountable work receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Changes {
    /// The explicitly supplied demanded key acquired a new authoritative basis.
    pub authoritative: bool,
    /// Only changed hypothesis identities; no history/state serialization.
    pub hypotheses: Vec<BranchId>,
    /// Adapter steps and kernel branch/edge visits in this transition.
    pub work: usize,
}

type Resolution = (BranchId, OperationOutcome, PredictionOutcome);
#[cfg(any(all(feature = "wasm", target_arch = "wasm32"), acyclic_harness_loom))]
pub(crate) mod mailbox;
#[cfg(acyclic_harness_loom)]
pub use mailbox::verify_mailbox;
