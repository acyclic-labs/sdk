use super::*;

/// Borrows the same work frame; only this call's temporary scratch retires.
pub(crate) struct AuthorityScope<'w, 'a> {
    work: &'w mut AuthorityWork<'a>,
    previous_floor: u64,
    #[cfg(all(not(target_arch = "wasm32"), any(test, feature = "distributed")))]
    previous_request: Option<crate::NativeAllocation>,
}

impl<'a> AuthorityWork<'a> {
    pub(crate) fn scope(&mut self) -> Result<AuthorityScope<'_, 'a>, AuthorityStoreError> {
        self.check()?;
        #[cfg(all(not(target_arch = "wasm32"), any(test, feature = "distributed")))]
        let previous_request = self
            .request_allocation
            .as_mut()
            .map(|allocation| allocation.split_off(allocation.bytes()))
            .transpose()?;
        let previous_floor = self.scratch_floor;
        self.scratch_floor = self.allocations.scratch_bytes();
        Ok(AuthorityScope {
            work: self,
            previous_floor,
            #[cfg(all(not(target_arch = "wasm32"), any(test, feature = "distributed")))]
            previous_request,
        })
    }
}

impl<'a> std::ops::Deref for AuthorityScope<'_, 'a> {
    type Target = AuthorityWork<'a>;

    fn deref(&self) -> &Self::Target {
        self.work
    }
}

impl std::ops::DerefMut for AuthorityScope<'_, '_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.work
    }
}

impl Drop for AuthorityScope<'_, '_> {
    fn drop(&mut self) {
        #[cfg(all(not(target_arch = "wasm32"), any(test, feature = "distributed")))]
        {
            // Earlier backing remains paid outside this call, including when a
            // nested request transfers or releases all of its own capacity.
            // Keep the same cached read context available during preparation.
            if let Some(previous) = self.previous_request.take() {
                drop(self.work.request_allocation.replace(previous));
            } else if let Some(allocation) = &mut self.work.request_allocation {
                // Keep the zero-byte original read context for the next call.
                // Retirement cannot require another cancellation admission.
                allocation.release_deferred(allocation.bytes());
            }
        }
        if !self.work.is_native() {
            self.work
                .allocations
                .release_scratch_to(self.work.scratch_floor)
                .unwrap_or_else(|_| unreachable!("a call retires only its own scratch"));
        }
        self.work.scratch_floor = self.previous_floor;
    }
}
