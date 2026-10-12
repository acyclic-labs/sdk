//! Bounded hypotheses over immutable authoritative references.
//!
//! This module performs no admission, IO, domain reduction or effect cancellation.
//! A trusted [`Domain`] adapter supplies validated facts and correspondence.
//! Exported hypotheses remain hypotheses; they are never accepted as evidence.

#![doc = include_str!("../../docs/client/README.md")]

use serde::{Deserialize, Serialize};
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
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod wasm;

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
