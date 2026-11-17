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

#[path = "cached_binding/decision.rs"]
mod decision;
pub(super) use decision::cached_binding_decision;

// Keep the original mutex check beside the adapter; the pure decision has a
// standalone solver target over the same production implementation.
#[cfg(any(all(test, not(loom)), kani))]
mod proofs {
    use super::cached_binding_matches;

    #[cfg_attr(kani, kani::proof)]
    #[cfg_attr(not(kani), test)]
    fn projection_copy_releases_its_mutex_and_preserves_lease_errors() {
        let cache = crate::sync::Mutex::new(Some(()));
        let decisions = std::cell::Cell::new(0_u8);
        let result = cached_binding_matches(
            &cache,
            |cache| *cache,
            || {
                assert!(cache.try_lock().is_ok());
                Err::<(), _>(7_u8)
            },
            |()| {
                decisions.set(decisions.get() + 1);
                true
            },
            |()| {
                decisions.set(decisions.get() + 1);
                true
            },
        );
        assert_eq!(result, Err(7));
        assert_eq!(decisions.get(), 0);
    }
}
