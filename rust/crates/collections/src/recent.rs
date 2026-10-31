//! Bounded recent facts over the same admitted fixed-width index.
//!
//! Two windows retain at least the most recent full window of insertions.
//! Forgetting a window permanently removes the right to infer absence from a
//! cache miss. This is a fact cache, never a registry of live ownership.
use super::*;

/// Two bounded insertion windows; an eviction invalidates completeness forever.
/// Rotation retires at most WINDOW entries on the original tokens.
pub struct RecentIndex<V, A, const N: usize, const WINDOW: usize> {
    current: FixedIndex<V, A, N>,
    previous: FixedIndex<V, A, N>,
    current_len: usize,
    complete: bool,
}

impl<V, A, const N: usize, const WINDOW: usize> Default for RecentIndex<V, A, N, WINDOW> {
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

impl<V, A, const N: usize, const WINDOW: usize> RecentIndex<V, A, N, WINDOW> {
    /// Whether no fact has yet been discarded by this cache.
    pub fn complete(&self) -> bool {
        self.complete
    }

    /// Reads either retained window with the original cumulative work.
    pub fn get<W: IndexWork<Allocation = A>>(
        &self,
        key: [u8; N],
        accounting: &mut W,
    ) -> Result<Option<&V>, W::Error> {
        if let Some(value) = self.current.get(key, accounting)? {
            return Ok(Some(value));
        }
        self.previous.get(key, accounting)
    }

    /// Admits construction before changing windows or discarding any fact.
    pub fn get_or_try_insert_with<W: IndexWork<Allocation = A>>(
        &mut self,
        key: [u8; N],
        accounting: &mut W,
        create: impl FnOnce(&mut W) -> Result<V, W::Error>,
    ) -> Result<&mut V, W::Error> {
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
        accounting.charge_items(N as u64 + 1)?;
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
