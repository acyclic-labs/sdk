//! Admission limits share the canonical registry, never a second local counter.
use super::OperationRegistry;
use crate::ProviderError;

/// Optional limits chosen once by the account's composition root. None preserves
/// the provider's existing unbounded registry profile; these confer no authority.
#[derive(Clone, Copy, Debug, Default)]
pub struct RegistryLimits {
    /// Accepted pending and terminal operations, excluding definite pre-dispatch refusals.
    pub operations: Option<u64>,
    /// State events for one logical machine.
    pub machine_events: Option<u64>,
    /// Physical simulator machine slots, including destroyed machine identities.
    pub machines: Option<usize>,
}

impl OperationRegistry {
    /// The simulator holds its native-world lock across this admission and its
    /// raw mutation, so capacity cannot change between validation and allocation.
    pub(crate) fn check_machine_capacity(
        &self,
        current: usize,
        additional: usize,
    ) -> Result<(), ProviderError> {
        if self.limits.machines.is_some_and(|limit| {
            current
                .checked_add(additional)
                .is_none_or(|total| total > limit)
        }) {
            return Err(ProviderError::Rejected("Machines machine limit reached; recovery: inspect the configured machine admission profile before allocating further machine identities".into()));
        }
        Ok(())
    }
}
