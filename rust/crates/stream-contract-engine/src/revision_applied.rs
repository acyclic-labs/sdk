use crate::query_sync::watch;

#[derive(Clone, Copy)]
struct Applied {
    index: u64,
    closed: bool,
}

/// The single applied-revision authority of one provider incarnation: issues
/// indexes, publishes application, and parks reads on its notification until a
/// requested minimum applies, it closes, or the caller drops the wait.
pub struct AppliedRevisions {
    scope: RevisionScope,
    incarnation: u128,
    applied: watch::Sender<Applied>,
    #[cfg(all(feature = "wasm-journal", target_arch = "wasm32"))]
    retired: tokio::sync::Notify,
}

impl std::fmt::Debug for AppliedRevisions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppliedRevisions")
            .field("scope", &self.scope)
            .field("incarnation", &self.incarnation)
            .finish_non_exhaustive()
    }
}

impl AppliedRevisions {
    /// An authority with an externally owned scope and incarnation, applied
    /// through `applied`.
    #[must_use]
    pub fn new(scope: RevisionScope, incarnation: u128, applied: u64) -> Self {
        Self {
            scope,
            incarnation,
            applied: watch::channel(Applied {
                index: applied,
                closed: false,
            })
            .0,
            #[cfg(all(feature = "wasm-journal", target_arch = "wasm32"))]
            retired: tokio::sync::Notify::new(),
        }
    }

    /// A fresh process-local authority: random scope and incarnation, so no
    /// token survives a reset or names another provider.
    #[must_use]
    pub fn in_process() -> Self {
        let scope = crate::new_identity();
        let incarnation = u128::from_be_bytes(crate::new_identity());
        Self::new(RevisionScope::new(scope.to_vec()), incarnation, 0)
    }

    /// A stable scope for one named storage location, so a reopened store
    /// keeps its scope and refuses older incarnations instead of ignoring them.
    #[must_use]
    pub fn storage_scope(kind: &str, location: &[u8]) -> RevisionScope {
        use sha2::Digest as _;
        let mut hash = sha2::Sha256::new();
        hash.update(b"acyclic.stream.revision.scope.v1\0");
        hash.update(kind.as_bytes());
        hash.update(b"\0");
        hash.update(location);
        let scope: [u8; 32] = hash.finalize().into();
        // A location is a host path; a seeded world records its schedule
        // stand-in, so one location keeps one scope within the world.
        #[cfg(not(target_arch = "wasm32"))]
        let scope =
            acyclic_native_runtime::stable_identity("acyclic.stream.revision.scope/v1", scope);
        RevisionScope::new(scope.to_vec())
    }

    /// A staging authority with this identity, applied through nothing yet.
    /// A provider that applies before it persists issues from the staging
    /// authority and advances this one only once its writes are durable.
    #[must_use]
    pub fn staging(&self) -> Self {
        Self::new(self.scope.clone(), self.incarnation, 0)
    }

    /// The authority's scope.
    #[must_use]
    pub fn scope(&self) -> &RevisionScope {
        &self.scope
    }

    /// This authority's revision at `index`.
    #[must_use]
    pub fn at(&self, index: u64) -> Revision {
        Revision {
            scope: self.scope.clone(),
            incarnation: self.incarnation,
            index,
        }
    }

    /// The currently applied revision.
    #[must_use]
    pub fn current(&self) -> Revision {
        self.at(self.applied.borrow().index)
    }

    /// Publishes application through `index` and wakes every parked read whose
    /// minimum it covers. Equal and lower indexes leave parked reads asleep.
    pub fn advance(&self, index: u64) {
        self.applied.send_if_modified(|applied| {
            if index <= applied.index {
                return false;
            }
            applied.index = index;
            true
        });
    }

    /// Ends this incarnation: every parked and later wait refuses as unavailable.
    pub fn close(&self) {
        self.applied.send_modify(|applied| applied.closed = true);
        #[cfg(all(feature = "wasm-journal", target_arch = "wasm32"))]
        self.retired.notify_waiters();
    }

    /// Waits until this incarnation is closed.
    #[cfg(all(feature = "wasm-journal", target_arch = "wasm32"))]
    pub async fn closed(&self) {
        // Register before reading the retained terminal bit. Ordinary revision
        // advances never wake an unrelated follow's retirement observer.
        let retired = std::pin::pin!(self.retired.notified());
        let mut retired = retired;
        retired.as_mut().enable();
        if !self.applied.borrow().closed {
            retired.await;
        }
    }

    /// Selects this authority's floor from `cut`. A token naming another
    /// scope, or this scope in another incarnation, is foreign: this authority
    /// cannot prove it. A multi-authority router hands each participant only
    /// its own entry of the cut.
    pub fn floor(&self, cut: &RevisionCut) -> Result<Option<u64>, StreamError> {
        let mut floor = None;
        for revision in cut.iter() {
            if revision.scope != self.scope || revision.incarnation != self.incarnation {
                return Err(StreamError::RevisionForeign);
            }
            floor = Some(revision.index);
        }
        Ok(floor)
    }

    /// Returns the applied revision once it covers `cut`. Parks on the applied
    /// notification otherwise; dropping the future is the deadline bound.
    pub async fn wait(&self, cut: &RevisionCut) -> Result<Revision, StreamError> {
        let Some(minimum) = self.floor(cut)? else {
            return self.ready();
        };
        let mut applied = self.applied.subscribe();
        loop {
            let observed = *applied.borrow_and_update();
            if observed.closed {
                return Err(StreamError::Unavailable);
            }
            if observed.index >= minimum {
                return Ok(self.at(observed.index));
            }
            applied
                .changed()
                .await
                .map_err(|_| StreamError::Unavailable)?;
        }
    }

    /// Waits until this authority applies `revision`, ends, or `deadline`
    /// resolves first, which refuses as [`StreamError::DeadlineElapsed`]. The
    /// deadline is any timer future, so every runtime and replica adapter
    /// shares this one park: a foreign token refuses before parking.
    pub async fn wait_applied(
        &self,
        revision: &Revision,
        deadline: impl Future<Output = ()>,
    ) -> Result<Revision, StreamError> {
        let cut = RevisionCut::of(revision.clone());
        let applied = std::pin::pin!(self.wait(&cut));
        let deadline = std::pin::pin!(deadline);
        match futures::future::select(applied, deadline).await {
            futures::future::Either::Left((applied, _)) => applied,
            futures::future::Either::Right(((), _)) => Err(StreamError::DeadlineElapsed),
        }
    }

    fn ready(&self) -> Result<Revision, StreamError> {
        let applied = *self.applied.borrow();
        if applied.closed {
            return Err(StreamError::Unavailable);
        }
        Ok(self.at(applied.index))
    }
}
