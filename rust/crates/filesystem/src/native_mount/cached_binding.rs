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

// The decision and the projection copy: Kani proves them in the library, where no test target and
// none of its dev-dependencies stand between proofs and verifier; unit tests enumerate them.
#[cfg(any(all(test, not(loom)), kani))]
mod proofs {
    use super::{cached_binding_decision, cached_binding_matches};
    use std::cell::Cell;

    struct Lease<'a>(&'a Cell<bool>);

    impl Drop for Lease<'_> {
        fn drop(&mut self) {
            self.0.set(false);
        }
    }

    #[cfg(kani)]
    #[kani::proof]
    #[kani::unwind(1)]
    fn decision_requires_a_binding_and_lease_and_checks_each_path_at_most_once() {
        check(kani::any(), kani::any(), kani::any(), kani::any());
    }

    fn check(present: bool, admitted: bool, current: bool, unchanged: bool) {
        let live = Cell::new(false);
        let lease_calls = Cell::new(0_u8);
        let current_calls = Cell::new(0_u8);
        let fallback_calls = Cell::new(0_u8);
        let matched = cached_binding_decision(
            present.then_some(()),
            || {
                lease_calls.set(lease_calls.get() + 1);
                live.set(admitted);
                if admitted { Ok(Lease(&live)) } else { Err(()) }
            },
            |()| {
                assert!(live.get());
                current_calls.set(current_calls.get() + 1);
                current
            },
            |()| {
                assert!(live.get());
                fallback_calls.set(fallback_calls.get() + 1);
                unchanged
            },
        );
        assert_eq!(
            matched,
            if present && !admitted {
                Err(())
            } else {
                Ok(present && (current || unchanged))
            }
        );
        assert_eq!(lease_calls.get(), u8::from(present));
        assert_eq!(current_calls.get(), u8::from(present && admitted));
        assert_eq!(
            fallback_calls.get(),
            u8::from(present && admitted && !current)
        );
        assert!(!live.get());
    }

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
