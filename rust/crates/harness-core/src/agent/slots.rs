//! Pure admission arithmetic shared by the agent transition and its proofs.

/// Most children an agent may have open at once.
pub const MAX_OPEN_CHILDREN: usize = 256;

/// The open-child ceiling for a client while recorded tool spawns own slots.
pub(crate) const fn client_ceiling(turn: bool, reserved: usize) -> usize {
    MAX_OPEN_CHILDREN.saturating_sub(if turn { reserved } else { 0 })
}

/// Replaying an existing child consumes no slot; a new child needs one.
pub(crate) const fn admits(open: usize, ceiling: usize, existing: bool) -> bool {
    existing || open < ceiling
}

/// Hold a slot vacated while a paused turn may still replay its recorded spawn.
pub(crate) fn reserve_vacated(reserved: u16) -> u16 {
    reserved.saturating_add(1).min(MAX_OPEN_CHILDREN as u16)
}
