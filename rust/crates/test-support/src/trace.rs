//! Finite trace conformance over an independently checked source/model mapping.
//!
//! Language and transport adapters own decoding, original causal ordering and
//! model semantics. This module never turns arrival order into distributed
//! order, or a finite successful prefix into an unbounded progress proof.

use crate::correspondence::{self, Inventory, Mapping};
use std::collections::BTreeSet;

/// A model adapter consuming observations from the original producer.
pub trait Model {
    type Event;
    type Error;

    /// The mapped action actually recorded by the source adapter.
    fn action(event: &Self::Event) -> &str;
    /// Check the observed input/effects and advance the original model state.
    fn transition(&mut self, event: &Self::Event) -> Result<(), Self::Error>;
    /// Check the adapter's declared end condition, including producer custody.
    /// A bounded prefix adapter must explicitly describe its weaker end check.
    fn finish(&mut self) -> Result<(), Self::Error>;
}

/// What this finite trace covered; absent actions remain unqualified.
#[derive(Debug, PartialEq, Eq)]
pub struct Coverage {
    pub events: usize,
    pub actions: BTreeSet<String>,
}

/// The first discrepancy, retaining its original event position and cause.
#[derive(Debug, PartialEq, Eq)]
pub enum Error<E> {
    Mapping(correspondence::Error),
    Empty,
    LengthOverflow,
    UnknownAction { index: usize, action: String },
    Transition { index: usize, source: E },
    Incomplete(E),
}

impl<E: std::fmt::Display> std::fmt::Display for Error<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mapping(error) => write!(f, "trace mapping: {error}"),
            Self::Empty => f.write_str("trace has no original producer events"),
            Self::LengthOverflow => f.write_str("trace event position exceeds addressable length"),
            Self::UnknownAction { index, action } => {
                write!(f, "trace event {index} has unmapped action {action}")
            }
            Self::Transition { index, source } => write!(f, "trace event {index}: {source}"),
            Self::Incomplete(source) => write!(f, "incomplete trace: {source}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for Error<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Mapping(source) => Some(source),
            Self::Transition { source, .. } | Self::Incomplete(source) => Some(source),
            _ => None,
        }
    }
}

/// Check the mapping before consuming any events, then stream the observations.
///
/// Retained checker memory depends only on mapped action coverage and the model
/// adapter, never trace length. Original errors are retained without reducing
/// them to categories. No capture, scheduler, retries or production I/O is added.
pub fn validate<'a, M: Model>(
    inventory: &Inventory,
    mapping: &Mapping,
    events: impl IntoIterator<Item = &'a M::Event>,
    model: &mut M,
) -> Result<Coverage, Error<M::Error>>
where
    M::Event: 'a,
{
    correspondence::validate(inventory, mapping).map_err(Error::Mapping)?;
    let mut coverage = Coverage {
        events: 0,
        actions: BTreeSet::new(),
    };
    for event in events {
        let index = coverage.events;
        let action = M::action(event);
        if !mapping.bindings.contains_key(action) {
            return Err(Error::UnknownAction {
                index,
                action: action.into(),
            });
        }
        model
            .transition(event)
            .map_err(|source| Error::Transition { index, source })?;
        if !coverage.actions.contains(action) {
            coverage.actions.insert(action.into());
        }
        coverage.events = coverage
            .events
            .checked_add(1)
            .ok_or(Error::LengthOverflow)?;
    }
    if coverage.events == 0 {
        return Err(Error::Empty);
    }
    model.finish().map_err(Error::Incomplete)?;
    Ok(coverage)
}
