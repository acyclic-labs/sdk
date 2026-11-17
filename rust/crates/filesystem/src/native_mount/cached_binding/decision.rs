pub(crate) fn cached_binding_decision<Binding: Copy, Lease, Error>(
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
    use core::cell::Cell;

    #[kani::proof]
    #[kani::unwind(1)]
    fn selection_preserves_binding_errors_and_exact_callback_counts() {
        let (present, admitted, current, unchanged): (bool, bool, bool, bool) = kani::any();
        let (binding, error): (u8, u8) = kani::any();
        let lease_calls = Cell::new(0_u8);
        let current_calls = Cell::new(0_u8);
        let fallback_calls = Cell::new(0_u8);
        let matched = cached_binding_decision(
            present.then_some(binding),
            || {
                lease_calls.set(lease_calls.get() + 1);
                if admitted { Ok(()) } else { Err(error) }
            },
            |value| {
                kani::assert(value == binding, "current receives the original binding");
                current_calls.set(current_calls.get() + 1);
                current
            },
            |value| {
                kani::assert(value == binding, "fallback receives the original binding");
                fallback_calls.set(fallback_calls.get() + 1);
                unchanged
            },
        );
        let expected = if present && !admitted {
            Err(error)
        } else {
            Ok(present && (current || unchanged))
        };
        kani::assert(matched == expected, "exact binding result and lease error");
        kani::assert(
            lease_calls.get() == u8::from(present),
            "one lease admission",
        );
        kani::assert(
            current_calls.get() == u8::from(present && admitted),
            "one current check",
        );
        kani::assert(
            fallback_calls.get() == u8::from(present && admitted && !current),
            "fallback only after a leased current miss",
        );
    }

    struct Lease<'a>(&'a Cell<bool>);

    impl Drop for Lease<'_> {
        fn drop(&mut self) {
            self.0.set(false);
        }
    }

    #[kani::proof]
    #[kani::unwind(1)]
    fn lease_covers_both_callbacks_and_is_released_after_decision() {
        let (present, admitted, current, unchanged): (bool, bool, bool, bool) = kani::any();
        let live = Cell::new(false);
        let _ = cached_binding_decision(
            present.then_some(()),
            || {
                if admitted {
                    live.set(true);
                    Ok(Lease(&live))
                } else {
                    Err(())
                }
            },
            |()| {
                kani::assert(live.get(), "lease covers current binding check");
                current
            },
            |()| {
                kani::assert(live.get(), "lease covers fallback binding check");
                unchanged
            },
        );
        kani::assert(!live.get(), "lease released after decision");
    }
}
