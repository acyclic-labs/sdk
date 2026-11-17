//! The in-process conformance suite behind the `acyclic-harness` facade.

#[cfg(test)]
acyclic_test_support::counting_allocator!();

// The core's items keep their `crate::` paths here, as in the facade.
use acyclic_harness_core::*;
use acyclic_harness_hosting as hosting;

pub mod conformance;

use acyclic_harness_oidc as oidc;
