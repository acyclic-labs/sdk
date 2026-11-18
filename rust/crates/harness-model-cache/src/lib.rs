//! Provider-neutral model cache observations and residency transitions.

#[cfg(test)]
acyclic_test_support::counting_allocator!();

mod executor;

pub use executor::model_cache::*;
