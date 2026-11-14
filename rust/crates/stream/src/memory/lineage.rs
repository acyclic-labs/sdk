// Derived read indexes (record keys, sealed history ranges) follow a path's
// lineage instead of each path version. Retained namespace cuts hold every
// path version, so an index owned per version would stay funded for as long
// as its revision is retained. A lineage holds only its newest index; every
// version and fork of it reads that index cut at its own tail, and readers
// copy it into a pin. Releasing the last pin therefore drops a superseded
// index by construction.

/// An index that can be restricted to the positions before a sequence cut.
trait LineageIndex: Clone + Default {
    fn cut(&self, before: u64) -> Self;
}

/// One path version's view of a lineage.
#[derive(Clone)]
struct IndexLineage<I> {
    shared: Option<Arc<Lineage<I>>>,
    // The generation this view published; only the newest view extends the
    // lineage in place.
    generation: u64,
    before: Option<u64>,
}

impl<I> Default for IndexLineage<I> {
    fn default() -> Self {
        Self {
            shared: None,
            generation: 0,
            before: None,
        }
    }
}

struct Lineage<I> {
    newest: crate::query_sync::SyncMutex<Newest<I>>,
    _allocation: Allocation,
}

// An extension adds only positions that every other holder's tail or cut
// already hides, so replacing the index never changes another holder's view.
struct Newest<I> {
    index: I,
    generation: u64,
}

impl<I> Lineage<I> {
    fn with<T>(&self, use_newest: impl FnOnce(&mut Newest<I>) -> T) -> T {
        use_newest(
            &mut self
                .newest
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

impl<I: LineageIndex> IndexLineage<I> {
    fn cap(&self, before: u64) -> Self {
        Self {
            shared: self.shared.clone(),
            generation: self.generation,
            before: Some(self.before.map_or(before, |old| old.min(before))),
        }
    }

    /// The lineage's newest index, uncut.
    fn newest(&self) -> I {
        self.shared.as_ref().map_or_else(I::default, |shared| {
            shared.with(|newest| newest.index.clone())
        })
    }

    /// An immutable index of this view for a holder at `tail`.
    fn pin(&self, tail: u64) -> I {
        self.newest()
            .cut(self.before.map_or(tail, |before| before.min(tail)))
    }

    /// The index an extension by the version at `tail` starts from, and
    /// whether that version is the lineage's newest.
    fn base(&self, tail: u64) -> (I, bool) {
        match &self.shared {
            Some(shared) if self.before.is_none() => shared.with(|newest| {
                if newest.generation == self.generation {
                    (newest.index.clone(), true)
                } else {
                    (newest.index.cut(tail), false)
                }
            }),
            _ => (self.pin(tail), false),
        }
    }

    /// Publishes `index`, built from [`Self::base`] at `tail`. The newest view
    /// replaces the lineage's index in place; any other view keeps its cut
    /// when nothing changed and otherwise starts a new lineage.
    fn publish(
        &self,
        tail: u64,
        extends: bool,
        changed: bool,
        index: I,
        work: &mut impl acyclic_collections::IndexWork<Error = StreamError, Allocation = Allocation>,
    ) -> Result<Self, StreamError> {
        let mut index = Some(index);
        if let Some(shared) = self.shared.as_ref().filter(|_| extends) {
            // A lost race leaves the lineage alone and starts a new one below.
            let replaced = shared.with(|newest| {
                (newest.generation == self.generation).then(|| {
                    newest.generation += 1;
                    let next = index.take().unwrap_or_default();
                    (
                        std::mem::replace(&mut newest.index, next),
                        newest.generation,
                    )
                })
            });
            // The superseded index retires here, outside the lock, unless a
            // reader pin still holds it.
            if let Some((_superseded, generation)) = replaced {
                return Ok(Self {
                    shared: Some(Arc::clone(shared)),
                    generation,
                    before: None,
                });
            }
        }
        if !changed {
            return Ok(self.cap(tail));
        }
        let allocation =
            work.claim(acyclic_collections::arc_allocation_bytes::<Lineage<I>>() as u64)?;
        Ok(Self {
            shared: Some(Arc::new(Lineage {
                newest: crate::query_sync::SyncMutex::new(Newest {
                    index: index.take().unwrap_or_default(),
                    generation: 0,
                }),
                _allocation: allocation,
            })),
            generation: 0,
            before: None,
        })
    }
}
