// Derived read indexes (record keys, sealed history ranges) follow a path's
// lineage instead of each path version. Retained namespace cuts hold every
// path version, so an index owned per version would stay funded for as long
// as its revision is retained. A lineage holds only its newest index; every
// version and fork of it reads that index cut at its own tail, and readers
// copy it into a pin. Releasing the last pin therefore drops a superseded
// index by construction.

/// An index that can be restricted to the positions before a sequence cut.
trait LineageIndex: Clone + Default {
    fn cut(
        &self,
        before: u64,
        work: &mut impl acyclic_collections::IndexWork<Error = StreamError, Allocation = Allocation>,
    ) -> Result<Self, StreamError>;
}

/// One path version's view of a lineage.
#[derive(Clone)]
struct IndexLineage<I> {
    shared: Option<Arc<Lineage<I>>>,
    // The generation this view published; only the newest view extends the
    // lineage in place.
    generation: u64,
    before: Option<u64>,
    // An extension planned by a write that is not yet accepted. Only
    // acceptance moves it into the lineage; a refused or cancelled write
    // drops it with its plan.
    pending: Option<I>,
}

impl<I> Default for IndexLineage<I> {
    fn default() -> Self {
        Self {
            shared: None,
            generation: 0,
            before: None,
            pending: None,
        }
    }
}

struct Lineage<I> {
    newest: crate::query_sync::SyncMutex<Newest<I>>,
    // The one path that extends this lineage in place. A subtree fork copies
    // its descendants' views, so several paths can hold the newest view; each
    // other path starts its own lineage when it first changes. One commit
    // names a path at most once, so it never plans two extensions of one
    // lineage.
    owner: StreamPath,
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
            pending: None,
        }
    }

    /// The lineage's newest index, uncut.
    fn newest(&self) -> I {
        self.shared.as_ref().map_or_else(I::default, |shared| {
            shared.with(|newest| newest.index.clone())
        })
    }

    /// An immutable index of this view for a holder at `tail`.
    fn pin(
        &self,
        tail: u64,
        work: &mut impl acyclic_collections::IndexWork<Error = StreamError, Allocation = Allocation>,
    ) -> Result<I, StreamError> {
        self.newest()
            .cut(self.before.map_or(tail, |before| before.min(tail)), work)
    }

    /// The index an extension of `path`'s version at `tail` starts from, and
    /// whether that version is the newest view of a lineage `path` owns.
    fn base(
        &self,
        path: &StreamPath,
        tail: u64,
        work: &mut impl acyclic_collections::IndexWork<Error = StreamError, Allocation = Allocation>,
    ) -> Result<(I, bool), StreamError> {
        match &self.shared {
            Some(shared) if self.before.is_none() && shared.owner == *path => {
                shared.with(|newest| {
                    if newest.generation == self.generation {
                        Ok((newest.index.clone(), true))
                    } else {
                        newest.index.cut(tail, work).map(|index| (index, false))
                    }
                })
            }
            _ => self.pin(tail, work).map(|index| (index, false)),
        }
    }

    /// Publishes `path`'s `index`, built from [`Self::base`] at `tail`. The
    /// newest view plans to replace the lineage's index when its write is
    /// accepted; any other view keeps its cut when nothing changed and
    /// otherwise starts a new lineage that `path` owns.
    fn publish(
        &self,
        path: &StreamPath,
        tail: u64,
        extends: bool,
        changed: bool,
        index: I,
        work: &mut impl acyclic_collections::IndexWork<Error = StreamError, Allocation = Allocation>,
    ) -> Result<Self, StreamError> {
        if let Some(shared) = self.shared.as_ref().filter(|_| extends) {
            return Ok(Self {
                shared: Some(Arc::clone(shared)),
                generation: self.generation,
                before: None,
                pending: Some(index),
            });
        }
        if !changed {
            return Ok(self.cap(tail));
        }
        let allocation =
            work.claim(acyclic_collections::arc_allocation_bytes::<Lineage<I>>() as u64)?;
        Ok(Self {
            shared: Some(Arc::new(Lineage {
                newest: crate::query_sync::SyncMutex::new(Newest {
                    index,
                    generation: 0,
                }),
                owner: path.clone(),
                _allocation: allocation,
            })),
            generation: 0,
            before: None,
            pending: None,
        })
    }

    /// Moves this view's planned extension into its lineage. Only acceptance
    /// calls this, so the shared index never holds a refused write.
    fn accept(&mut self) -> Result<(), StreamError> {
        let (Some(shared), Some(index)) = (&self.shared, self.pending.take()) else {
            return Ok(());
        };
        // Only the owning path extends a lineage, one commit names that path
        // once, and the publication owner excludes every other write from
        // planning to acceptance, so the lineage is still at the planned
        // generation.
        let _superseded = shared
            .with(|newest| {
                (newest.generation == self.generation).then(|| {
                    newest.generation += 1;
                    std::mem::replace(&mut newest.index, index)
                })
            })
            .ok_or(StreamError::Unavailable)?;
        self.generation += 1;
        // The superseded index retires here, outside the lock, unless a reader
        // pin still holds it.
        Ok(())
    }
}
