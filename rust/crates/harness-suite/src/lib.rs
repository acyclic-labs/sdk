//! The in-process conformance suite behind the `acyclic-harness` facade.

// The core's items keep their `crate::` paths here, as in the facade.
use acyclic_harness_core::*;
use acyclic_harness_client as client;
use acyclic_harness_hosting as hosting;

pub mod conformance;
