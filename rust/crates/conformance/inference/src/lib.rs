//! Captured-real transport and complete provider qualification runner.

/// Replay validation and the public verifier use the same assertion helper.
pub(crate) fn require(
    condition: bool,
    reason: &'static str,
) -> acyclic_harness_core::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(acyclic_harness_core::Error::Invalid(reason.into()))
    }
}

pub mod replay;
