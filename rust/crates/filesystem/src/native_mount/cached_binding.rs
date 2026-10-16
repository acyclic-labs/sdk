// Lock order: copy and release the projection cache before acquiring the source
// lease. Writers may hold the source lease while updating the projection cache.
// Retain that lease through both the snapshot decision and the fallback check.
pub(super) fn cached_binding_matches<Cache, Binding: Copy, Lease, Error>(
    cache: &crate::sync::Mutex<Cache>,
    binding: impl FnOnce(&Cache) -> Option<Binding>,
    lease: impl FnOnce() -> Result<Lease, Error>,
    current: impl FnOnce(Binding) -> bool,
    unchanged: impl FnOnce(Binding) -> bool,
) -> bool {
    let binding = {
        let cache = cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        binding(&cache)
    };
    cached_binding_decision(binding, lease, current, unchanged)
}

pub(super) fn cached_binding_decision<Binding: Copy, Lease, Error>(
    binding: Option<Binding>,
    lease: impl FnOnce() -> Result<Lease, Error>,
    current: impl FnOnce(Binding) -> bool,
    unchanged: impl FnOnce(Binding) -> bool,
) -> bool {
    let Some(binding) = binding else {
        return false;
    };
    let Ok(_lease) = lease() else {
        return false;
    };
    current(binding) || unchanged(binding)
}
