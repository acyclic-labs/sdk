//! Compatibility facade for the published, provider-neutral model suite.
pub use acyclic_harness_core::model::conformance::*;

/// Captured-real transport and complete provider qualification runner.
#[cfg(feature = "inference")]
pub mod replay;

// Replay validation and the public verifier use the same assertion helper.
#[cfg(feature = "inference")]
fn require(condition: bool, reason: &'static str) -> acyclic_harness_core::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(acyclic_harness_core::Error::Invalid(reason.into()))
    }
}
