//! Bounded recent facts over the same admitted fixed-width index.
//!
//! Two windows retain at least the most recent full window of insertions.
//! Forgetting a window permanently removes the right to infer absence from a
//! cache miss. This is a fact cache, never a registry of live ownership.
use super::*;

pub(crate) struct RecentIndex<V, const N: usize, const WINDOW: usize> {
    current: FixedIndex<V, N>,
    previous: FixedIndex<V, N>,
    current_len: usize,
    complete: bool,
}

impl<V, const N: usize, const WINDOW: usize> Default for RecentIndex<V, N, WINDOW> {
    fn default() -> Self {
        const { assert!(WINDOW > 0) };
        Self {
            current: FixedIndex::default(),
            previous: FixedIndex::default(),
            current_len: 0,
            complete: true,
        }
    }
}

impl<V, const N: usize, const WINDOW: usize> RecentIndex<V, N, WINDOW> {
    pub(crate) fn complete(&self) -> bool {
        self.complete
    }

    pub(crate) fn get(
        &self,
        key: [u8; N],
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<&V>, FsError> {
        if let Some(value) = self.current.get(key, accounting)? {
            return Ok(Some(value));
        }
        self.previous.get(key, accounting)
    }

    pub(crate) fn get_or_try_insert_with(
        &mut self,
        key: [u8; N],
        accounting: &mut KernelAccounting<'_>,
        create: impl FnOnce(&mut KernelAccounting<'_>) -> Result<V, FsError>,
    ) -> Result<&mut V, FsError> {
        if self.current.get(key, accounting)?.is_some() {
            return self.current.get_or_try_insert_with(key, accounting, create);
        }
        if self.previous.get(key, accounting)?.is_some() {
            return self
                .previous
                .get_or_try_insert_with(key, accounting, create);
        }
        if self.current_len < WINDOW {
            let value = self
                .current
                .get_or_try_insert_with(key, accounting, create)?;
            self.current_len += 1;
            return Ok(value);
        }
        // Stage admission and construction before discarding any facts. The
        // final lookup after rotation visits this single funded leaf.
        accounting.charge_items(crate::foundation::usize_to_u64(N) + 1)?;
        let mut next = FixedIndex::default();
        next.get_or_try_insert_with(key, accounting, create)?;
        self.complete &= self.previous.root.is_none();
        let current = std::mem::replace(&mut self.current, next);
        let retired = std::mem::replace(&mut self.previous, current);
        self.current_len = 1;
        // The original index retires values before their own node funding.
        drop(retired);
        Ok(self.current.value_mut(key))
    }
}

#[cfg(test)]
mod tests;
