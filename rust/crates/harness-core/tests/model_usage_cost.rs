//! Usage aggregation and price-selection properties over the production API.
#![cfg(test)]

acyclic_test_support::counting_allocator!();

#[path = "model_usage_cost/executor.rs"]
mod executor;
