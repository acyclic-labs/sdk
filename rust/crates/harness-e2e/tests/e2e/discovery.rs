// Shared fixture declaration; native oracle tests remain in this runner.
include!("../../../harness/tests/support/discovery.rs");

#[cfg(test)]
#[path = "discovery_tests.rs"]
mod tests;
