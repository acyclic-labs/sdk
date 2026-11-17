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

#[cfg(kani)]
mod proofs {
    use super::cached_binding_decision;
    use std::cell::Cell;

    struct Lease<'a>(&'a Cell<bool>);

    impl Drop for Lease<'_> {
        fn drop(&mut self) {
            self.0.set(false);
        }
    }

    #[kani::proof]
    #[kani::unwind(1)]
    fn decision_requires_a_binding_and_lease_and_checks_each_path_at_most_once() {
        let (present, admitted, current, unchanged): (bool, bool, bool, bool) = kani::any();
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
}
