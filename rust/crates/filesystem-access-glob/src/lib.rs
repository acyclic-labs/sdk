//! Glob matching and work admission for filesystem access policies.

pub mod glob;

/// A canonical glob or its work budget is invalid.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid filesystem access policy: {0}")]
pub struct GlobError(pub &'static str);

/// Charges glob work before it is performed.
pub fn spend(remaining: &mut usize, amount: usize) -> Result<(), GlobError> {
    *remaining = remaining
        .checked_sub(amount)
        .ok_or(GlobError("glob compilation exceeds work limit"))?;
    Ok(())
}
