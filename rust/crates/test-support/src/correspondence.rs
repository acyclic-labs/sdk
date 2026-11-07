//! Structural correspondence over independently extracted source/model inventories.
//!
//! Names are opaque to this checker: Rust and other model languages supply their
//! own extractors. This checks coverage and reachability, not trace refinement,
//! branch semantics, fairness or unbounded liveness.
use std::collections::{BTreeMap, BTreeSet};

/// Declarations and calls obtained from the actual model and shipping source.
#[derive(Clone, Debug, Default)]
pub struct Inventory {
    /// Declared model transitions.
    pub actions: BTreeSet<String>,
    /// Model transitions invoked by the proof harness.
    pub invoked: BTreeSet<String>,
    /// Every function in the mapped scope and its internal callees.
    pub functions: BTreeMap<String, BTreeSet<String>>,
    /// Functions invoked at shipping entry points.
    pub shipping: BTreeSet<String>,
}

/// Explicit correspondence, separate from the inventories it must cover.
#[derive(Clone, Debug, Default)]
pub struct Mapping {
    /// Model transition to production function.
    pub bindings: BTreeMap<String, String>,
    /// Unreachable source functions excluded from this proof, with reasons.
    pub exclusions: BTreeMap<String, String>,
}

/// The first structural discrepancy. The offending identity is retained.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    ActionCoverage,
    UnknownSymbol(String),
    MissingCallee { caller: String, callee: String },
    UncoveredSymbol(String),
    UnobservedEntry(String),
    InvalidExclusion(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ActionCoverage => {
                write!(f, "model declarations, bindings and invocations differ")
            }
            Self::UnknownSymbol(name) => write!(f, "unknown source symbol: {name}"),
            Self::MissingCallee { caller, callee } => {
                write!(f, "unknown internal callee: {caller} -> {callee}")
            }
            Self::UncoveredSymbol(name) => write!(f, "uncovered source symbol: {name}"),
            Self::UnobservedEntry(name) => write!(f, "shipping entry has no model path: {name}"),
            Self::InvalidExclusion(name) => {
                write!(f, "unknown, empty or redundant exclusion: {name}")
            }
        }
    }
}

impl std::error::Error for Error {}

/// Validate both directions and return the source symbols reachable from actions.
///
/// Extractors must reject duplicate declarations before constructing these sets,
/// include all functions in their declared scope, and distinguish internal calls
/// from external dependencies. Exclusion reasons are reviewed evidence, not an
/// automatic assertion of irrelevance. This offline check traverses each internal
/// edge once after constructing the inventories; it runs no production operation.
pub fn validate(inventory: &Inventory, mapping: &Mapping) -> Result<BTreeSet<String>, Error> {
    if inventory.actions.is_empty()
        || inventory.actions != inventory.invoked
        || !inventory.actions.iter().eq(mapping.bindings.keys())
    {
        return Err(Error::ActionCoverage);
    }
    for name in mapping.bindings.values().chain(&inventory.shipping) {
        if !inventory.functions.contains_key(name) {
            return Err(Error::UnknownSymbol(name.clone()));
        }
    }
    for (caller, callees) in &inventory.functions {
        for callee in callees {
            if !inventory.functions.contains_key(callee) {
                return Err(Error::MissingCallee {
                    caller: caller.clone(),
                    callee: callee.clone(),
                });
            }
        }
    }
    let mut reached = BTreeSet::new();
    let mut pending: Vec<_> = mapping.bindings.values().collect();
    while let Some(name) = pending.pop() {
        if reached.insert(name.clone()) {
            pending.extend(&inventory.functions[name]);
        }
    }
    for name in &inventory.shipping {
        if !reached.contains(name) {
            return Err(Error::UnobservedEntry(name.clone()));
        }
    }
    for (name, reason) in &mapping.exclusions {
        if reason.trim().is_empty()
            || !inventory.functions.contains_key(name)
            || reached.contains(name)
        {
            return Err(Error::InvalidExclusion(name.clone()));
        }
    }
    for name in inventory.functions.keys() {
        if !reached.contains(name) && !mapping.exclusions.contains_key(name) {
            return Err(Error::UncoveredSymbol(name.clone()));
        }
    }
    Ok(reached)
}
