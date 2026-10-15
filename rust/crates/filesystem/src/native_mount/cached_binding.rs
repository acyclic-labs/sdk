// Lock order: copy and release the projection cache before acquiring the source
// lease. Writers may hold the source lease while updating the projection cache.
// Retain that lease through both the snapshot decision and the fallback check.
pub(super) fn cached_binding_matches<Binding: Copy, Lease, Error>(
    binding: impl FnOnce() -> Option<Binding>,
    lease: impl FnOnce() -> Result<Lease, Error>,
    current: impl FnOnce(Binding) -> bool,
    unchanged: impl FnOnce(Binding) -> bool,
) -> bool {
    let Some(binding) = binding() else {
        return false;
    };
    let Ok(_lease) = lease() else {
        return false;
    };
    current(binding) || unchanged(binding)
}
