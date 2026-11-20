//! Fixed lock striping, shared by the runtime's actor table and the socket
//! tables: every `u64` names exactly one of [`COUNT`] shards.

/// Shards per striped table.
pub(super) const COUNT: usize = 64;

/// One value per shard.
pub(super) struct Shards<T>([T; COUNT]);

impl<T> Shards<T> {
    pub(super) fn new(mut shard: impl FnMut() -> T) -> Self {
        Self(std::array::from_fn(|_| shard()))
    }

    /// The shard `index` names, reduced modulo [`COUNT`].
    pub(super) fn get(&self, index: u64) -> &T {
        let [first, ..] = &self.0;
        self.0.get(position(index)).unwrap_or(first)
    }

    /// The shard `index` names, reduced modulo [`COUNT`].
    pub(super) fn get_mut(&mut self, index: u64) -> &mut T {
        let [first, rest @ ..] = &mut self.0;
        match position(index).checked_sub(1) {
            Some(position) => rest.get_mut(position).unwrap_or(first),
            None => first,
        }
    }

    pub(super) fn iter(&self) -> std::slice::Iter<'_, T> {
        self.0.iter()
    }

    /// Every shard's projection, in shard order.
    pub(super) fn map<'a, U>(&'a self, project: impl FnMut(&'a T) -> U) -> Shards<U> {
        Shards(self.0.each_ref().map(project))
    }
}

/// `index` reduced below [`COUNT`]: the remainder always converts, so every
/// lookup above finds its own shard rather than the first.
fn position(index: u64) -> usize {
    usize::try_from(index % COUNT as u64).unwrap_or(0)
}
