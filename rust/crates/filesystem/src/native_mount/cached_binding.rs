// Lock order: copy and release the projection cache before acquiring the source
// lease. Writers may hold the source lease while updating the projection cache.
// Retain that lease through both the snapshot decision and the fallback check.
pub(super) fn cached_binding_matches<State, Binding: Copy, Lease, Error>(
    cache: &crate::sync::Mutex<State>,
    binding: impl FnOnce(&State) -> Option<Binding>,
    lease: impl FnOnce() -> Result<Lease, Error>,
    current: impl FnOnce(Binding) -> bool,
    unchanged: impl FnOnce(Binding) -> bool,
) -> Result<bool, Error> {
    let binding = binding(
        &cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    );
    cached_binding_decision(binding, lease, current, unchanged)
}

pub(super) fn cached_binding_decision<Binding: Copy, Lease, Error>(
    binding: Option<Binding>,
    lease: impl FnOnce() -> Result<Lease, Error>,
    current: impl FnOnce(Binding) -> bool,
    unchanged: impl FnOnce(Binding) -> bool,
) -> Result<bool, Error> {
    let Some(binding) = binding else {
        return Ok(false);
    };
    let _lease = lease()?;
    Ok(current(binding) || unchanged(binding))
}
