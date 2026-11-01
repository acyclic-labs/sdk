use super::*;

/// Borrows the same work frame; only this call's temporary scratch retires.
pub(crate) struct AuthorityScope<'w, 'a> {
    work: &'w mut AuthorityWork<'a>,
    previous_floor: u64,
}

impl<'a> AuthorityWork<'a> {
    pub(crate) fn scope(&mut self) -> Result<AuthorityScope<'_, 'a>, AuthorityStoreError> {
        self.check()?;
        let previous_floor = self.scratch_floor;
        self.scratch_floor = self.allocations.scratch_bytes();
        Ok(AuthorityScope {
            work: self,
            previous_floor,
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
        if !self.work.is_native() {
            self.work
                .allocations
                .release_scratch_to(self.work.scratch_floor)
                .unwrap_or_else(|_| unreachable!("a call retires only its own scratch"));
        }
        self.work.scratch_floor = self.previous_floor;
    }
}
