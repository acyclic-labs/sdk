//! One applied-revision token shared by Streams, Objects and Actors.
//!
//! A [`Revision`] names a committed publication of one authority (an account or
//! storage scope) in one physical incarnation. An index is never compared across
//! scopes or incarnations, and a stream offset is never a revision.
use crate::OwnedBytes as Bytes;
use crate::StreamError;
use crate::query_sync::watch;

/// Opaque identity of the authority that orders revisions.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RevisionScope(Bytes);

impl RevisionScope {
    /// Binds caller-owned authority bytes, such as an account or storage id.
    #[must_use]
    pub fn new(bytes: impl Into<Bytes>) -> Self {
        Self(bytes.into())
    }

    /// The authority bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// One committed publication of an authority in one physical incarnation.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Revision {
    /// The ordering authority.
    pub scope: RevisionScope,
    /// The authority's physical incarnation; a reset never reuses one.
    pub incarnation: u128,
    /// Monotonic publication index within that scope and incarnation.
    pub index: u64,
}

impl Revision {
    /// Whether both revisions belong to the same scope and incarnation.
    #[must_use]
    pub fn same_authority(&self, other: &Self) -> bool {
        self.incarnation == other.incarnation && self.scope == other.scope
    }
}

/// A minimum cut: at most one revision per scope, ordered by scope. Multiple
/// authorities keep their own participant cuts; they are never folded into a max.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RevisionCut(Vec<Revision>);

impl RevisionCut {
    /// A cut with one participant.
    #[must_use]
    pub fn of(revision: Revision) -> Self {
        Self(vec![revision])
    }

    /// Canonicalizes participants: one entry per scope, as [`Self::observe`] keeps.
    pub fn new(revisions: impl IntoIterator<Item = Revision>) -> Self {
        let mut cut = Self::default();
        for revision in revisions {
            cut.observe(revision);
        }
        cut
    }

    /// Raises this cut with one observed revision of its own authority.
    /// A different incarnation of a known scope replaces it: the old floor
    /// is unprovable after a reset.
    pub fn observe(&mut self, revision: Revision) {
        match self
            .0
            .binary_search_by(|entry| entry.scope.cmp(&revision.scope))
        {
            Ok(at) => {
                let entry = &mut self.0[at];
                if entry.incarnation != revision.incarnation {
                    *entry = revision;
                } else if entry.index < revision.index {
                    entry.index = revision.index;
                }
            }
            Err(at) => self.0.insert(at, revision),
        }
    }

    /// Removes one scope's floor, after its authority refused the incarnation.
    pub fn forget(&mut self, scope: &RevisionScope) {
        self.0.retain(|entry| &entry.scope != scope);
    }

    /// The participant for one scope.
    #[must_use]
    pub fn get(&self, scope: &RevisionScope) -> Option<&Revision> {
        self.0
            .binary_search_by(|entry| entry.scope.cmp(scope))
            .ok()
            .map(|at| &self.0[at])
    }

    /// Whether the cut has no participant.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Participants in scope order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &Revision> {
        self.0.iter()
    }
}

/// A client's last observed revision per authority, forwarded as the floor of
/// every later read. Each authority keeps its own participant; a reset
/// incarnation replaces its scope's entry.
#[derive(Debug, Default)]
pub struct RevisionTracker(std::sync::Mutex<RevisionCut>);

impl RevisionTracker {
    fn cut(&self) -> std::sync::MutexGuard<'_, RevisionCut> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Records a revision a peer issued or served.
    pub fn observe(&self, revision: &Revision) {
        let mut cut = self.cut();
        if cut.get(&revision.scope) != Some(revision) {
            cut.observe(revision.clone());
        }
    }

    /// The floor to forward: every observed participant, raised by `explicit`.
    #[must_use]
    pub fn floor(&self, explicit: &RevisionCut) -> RevisionCut {
        let mut cut = self.cut().clone();
        for revision in explicit.iter() {
            cut.observe(revision.clone());
        }
        cut
    }

    /// Drops every floor after an authority refused one as foreign: the
    /// refusal surfaced once, and later reads proceed without the lost proof.
    pub fn refused(&self, error: &StreamError) {
        if *error == StreamError::RevisionForeign {
            *self.cut() = RevisionCut::default();
        }
    }
}

/// The revision a server reports for one read: `provider`'s applied revision
/// once it covers `at_least`. A provider that issues no revisions reports none
/// and still enforces the cut it forwards.
pub async fn served<P: crate::StreamProvider + ?Sized>(
    provider: &P,
    at_least: &RevisionCut,
) -> Result<Option<Revision>, StreamError> {
    match provider.wait_applied(at_least).await {
        Ok(revision) => Ok(Some(revision)),
        Err(StreamError::Unsupported) => Ok(None),
        Err(error) => Err(error),
    }
}

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
        }
    }

    /// A fresh process-local authority: random scope and incarnation, so no
    /// token survives a reset or names another provider.
    #[must_use]
    pub fn in_process() -> Self {
        let scope = uuid::Uuid::new_v4().into_bytes();
        let incarnation = u128::from_be_bytes(uuid::Uuid::new_v4().into_bytes());
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
        RevisionScope::new(hash.finalize().to_vec())
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
    /// minimum it covers. Lower indexes never move the applied revision back.
    pub fn advance(&self, index: u64) {
        self.applied
            .send_modify(|applied| applied.index = applied.index.max(index));
    }

    /// Ends this incarnation: every parked and later wait refuses as unavailable.
    pub fn close(&self) {
        self.applied.send_modify(|applied| applied.closed = true);
    }

    /// Selects this authority's participant of `cut`. This scope in another
    /// incarnation is foreign: that floor is unprovable after a reset. Other
    /// scopes are other participants' cuts, enforced by their own authorities.
    pub fn floor(&self, cut: &RevisionCut) -> Result<Option<u64>, StreamError> {
        match cut.get(&self.scope) {
            None => Ok(None),
            Some(revision) if revision.incarnation == self.incarnation => Ok(Some(revision.index)),
            Some(_) => Err(StreamError::RevisionForeign),
        }
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

    fn ready(&self) -> Result<Revision, StreamError> {
        let applied = *self.applied.borrow();
        if applied.closed {
            return Err(StreamError::Unavailable);
        }
        Ok(self.at(applied.index))
    }
}

#[cfg(all(test, not(target_arch = "wasm32"), not(loom)))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn parks_until_applied_and_refuses_foreign_incarnations() {
        let authority = std::sync::Arc::new(AppliedRevisions::in_process());
        let wanted = authority.at(2);
        let parked = tokio::spawn({
            let authority = std::sync::Arc::clone(&authority);
            let cut = RevisionCut::of(wanted.clone());
            async move { authority.wait(&cut).await }
        });
        tokio::task::yield_now().await;
        authority.advance(1);
        tokio::task::yield_now().await;
        assert!(
            !parked.is_finished(),
            "applied 1 must not satisfy minimum 2"
        );
        authority.advance(2);
        assert_eq!(parked.await.unwrap().unwrap(), wanted);

        let mut foreign = wanted.clone();
        foreign.incarnation ^= 1;
        assert_eq!(
            authority.wait(&RevisionCut::of(foreign)).await,
            Err(StreamError::RevisionForeign)
        );
        // Another participant's cut constrains only its own authority.
        let other = AppliedRevisions::in_process().at(u64::MAX);
        assert_eq!(
            authority.wait(&RevisionCut::of(other)).await,
            Ok(authority.at(2))
        );

        let closing = tokio::spawn({
            let authority = std::sync::Arc::clone(&authority);
            let cut = RevisionCut::of(authority.at(9));
            async move { authority.wait(&cut).await }
        });
        tokio::task::yield_now().await;
        authority.close();
        assert_eq!(closing.await.unwrap(), Err(StreamError::Unavailable));
    }
}
