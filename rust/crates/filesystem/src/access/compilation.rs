use super::InvalidPolicy;

pub(super) const MAXIMUM_NFA_STATES: usize = 32_768;
pub(super) const MAXIMUM_STATES: usize = 4_096;
pub(super) const MAXIMUM_TRANSITIONS: usize = 262_144;
pub(super) const MAXIMUM_STATE_MEMBERS: usize = 262_144;
pub(super) const MAXIMUM_TEXT_BYTES: usize = 1_048_576;
pub(super) const MAXIMUM_WORK: usize = 4_194_304;

pub(super) fn spend(remaining: &mut usize, amount: usize) -> Result<(), InvalidPolicy> {
    *remaining = remaining
        .checked_sub(amount)
        .ok_or(InvalidPolicy("glob compilation exceeds work limit"))?;
    Ok(())
}
