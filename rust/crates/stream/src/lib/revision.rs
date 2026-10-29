//! One applied-revision token shared by Streams, Objects and Actors.
//!
//! A [`Revision`] names a committed publication of one authority (an account or
//! storage scope) in one physical incarnation. An index is never compared across
//! scopes or incarnations, and a stream offset is never a revision.
use crate::OwnedBytes as Bytes;
use crate::StreamError;
use crate::StreamProviderExt as _;
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

    /// Canonicalizes participants in place, retaining the greatest index per
    /// authority. Conflicting incarnations refuse with `RevisionForeign`:
    /// neither incarnation proves the other's committed publications.
    pub fn new(revisions: impl IntoIterator<Item = Revision>) -> Result<Self, StreamError> {
        let mut revisions: Vec<_> = revisions.into_iter().collect();
        revisions.sort_unstable_by(|left, right| left.scope.cmp(&right.scope));
        let mut foreign = false;
        revisions.dedup_by(|next, kept| {
            if next.scope != kept.scope {
                return false;
            }
            if !next.same_authority(kept) {
                foreign = true;
            } else {
                kept.index = kept.index.max(next.index);
            }
            true
        });
        if foreign {
            Err(StreamError::RevisionForeign)
        } else {
            revisions.shrink_to_fit();
            Ok(Self(revisions))
        }
    }

    /// Raises this cut with one observed revision of its own authority.
    /// A different incarnation refuses with `RevisionForeign` and leaves the
    /// cut unchanged. A reset cannot silently retire the original requirement.
    /// Returns the retained participant without another lookup or clone.
    pub fn observe(&mut self, revision: Revision) -> Result<&Revision, StreamError> {
        let at = match self
            .0
            .binary_search_by(|entry| entry.scope.cmp(&revision.scope))
        {
            Ok(at) => {
                let entry = &mut self.0[at];
                if entry.incarnation != revision.incarnation {
                    return Err(StreamError::RevisionForeign);
                } else if entry.index < revision.index {
                    entry.index = revision.index;
                }
                at
            }
            Err(at) => {
                self.0.insert(at, revision);
                at
            }
        };
        Ok(&self.0[at])
    }

    /// Whether this cut holds `revision`'s authority at or above its index.
    #[must_use]
    pub fn covers(&self, revision: &Revision) -> bool {
        self.get(&revision.scope)
            .is_some_and(|held| held.same_authority(revision) && held.index >= revision.index)
    }

    /// Whether `applied` proves every participant of this minimum. An empty
    /// minimum is always proven; a missing revision proves no minimum.
    #[must_use]
    pub fn proven_by(&self, applied: Option<&Revision>) -> bool {
        self.iter().all(|minimum| {
            applied.is_some_and(|applied| {
                applied.same_authority(minimum) && applied.index >= minimum.index
            })
        })
    }

    /// Explicitly removes one participant. The caller must already possess
    /// coverage evidence; a refusal alone proves no retirement.
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
/// every later read. A foreign refusal or a reply from another incarnation
/// never retires an existing requirement. A new client starts a new frontier.
#[derive(Debug)]
pub struct RevisionTracker(TrackedRevisions);

type ObservedRevisions =
    std::collections::BTreeMap<RevisionScope, crate::query_sync::Arc<ObservedAuthority>>;

#[path = "revision/participant.rs"]
mod participant;
use participant::ObservedAuthority;

#[derive(Debug)]
enum TrackedRevisions {
    All(crate::query_sync::SyncMutex<ObservedRevisions>),
    Selected {
        source: crate::query_sync::Arc<RevisionTracker>,
        authority: crate::query_sync::Arc<ObservedAuthority>,
    },
}

impl Default for RevisionTracker {
    fn default() -> Self {
        Self(TrackedRevisions::All(
            crate::query_sync::SyncMutex::default(),
        ))
    }
}

impl RevisionTracker {
    fn cut(&self) -> impl std::ops::DerefMut<Target = ObservedRevisions> + '_ {
        match &self.0 {
            TrackedRevisions::All(observed) => observed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            TrackedRevisions::Selected { source, .. } => source.cut(),
        }
    }

    fn authority(&self, scope: &RevisionScope) -> crate::query_sync::Arc<ObservedAuthority> {
        if let TrackedRevisions::Selected { authority, .. } = &self.0
            && &authority.scope == scope
        {
            return crate::query_sync::Arc::clone(authority);
        }
        crate::query_sync::Arc::clone(
            self.cut().entry(scope.clone()).or_insert_with(|| {
                crate::query_sync::Arc::new(ObservedAuthority::new(scope.clone()))
            }),
        )
    }

    fn participants(
        &self,
        mut visit: impl FnMut(Revision) -> Result<(), StreamError>,
    ) -> Result<(), StreamError> {
        match &self.0 {
            TrackedRevisions::All(_) => {
                for authority in self.cut().values() {
                    if let Some(revision) = authority.revision()? {
                        visit(revision)?;
                    }
                }
            }
            TrackedRevisions::Selected { authority, .. } => {
                if let Some(revision) = authority.revision()? {
                    visit(revision)?;
                }
            }
        }
        Ok(())
    }

    /// Selects one explicitly routed storage authority's implicit frontier.
    /// Observations remain shared: other views keep every other authority and
    /// every explicit request minimum is still forwarded unchanged.
    ///
    /// Choose the scope from an authenticated receipt or observation. Selection
    /// conveys no authorization and proves no retirement or reset. A selected
    /// floor and same-authority observation access their retained participant
    /// directly, independent of this client's K unrelated authorities. View
    /// construction performs one indexed lookup and allocates one view; the
    /// participant is allocated once per scope. Views never form a chain.
    #[must_use]
    pub fn scoped(
        tracker: &crate::query_sync::Arc<Self>,
        scope: RevisionScope,
    ) -> crate::query_sync::Arc<Self> {
        let source = match &tracker.0 {
            TrackedRevisions::All(_) => tracker,
            TrackedRevisions::Selected { source, .. } => source,
        };
        crate::query_sync::Arc::new(Self(TrackedRevisions::Selected {
            source: crate::query_sync::Arc::clone(source),
            authority: source.authority(&scope),
        }))
    }

    /// Records a revision a peer issued or served.
    pub fn observe(&self, revision: &Revision) {
        self.with_authority(&revision.scope, |authority| authority.observe(revision));
    }

    fn with_authority<T>(
        &self,
        scope: &RevisionScope,
        apply: impl FnOnce(&ObservedAuthority) -> T,
    ) -> T {
        if let TrackedRevisions::Selected { authority, .. } = &self.0
            && &authority.scope == scope
        {
            return apply(authority);
        }
        apply(&self.authority(scope))
    }

    /// Accepts a reply served against the `minimum` this client sent: the
    /// reply's applied revision must prove it, so a peer that ignored the
    /// minimum cannot return older data as success. The revision then raises
    /// the floor. Every direct read and tail shares this one check.
    pub fn served(
        &self,
        minimum: &RevisionCut,
        applied: Option<&Revision>,
    ) -> Result<(), StreamError> {
        if !minimum.proven_by(applied) {
            return Err(StreamError::InvalidResponse);
        }
        if let Some(applied) = applied {
            self.with_authority(&applied.scope, |authority| {
                authority.observe(applied);
                authority.revision().map(|_| ())
            })?;
        }
        Ok(())
    }

    /// Records an append's committed revision.
    pub fn appended(&self, outcome: &crate::AppendOutcome) {
        if let crate::AppendOutcome::Committed(receipt) = outcome {
            self.observe(&receipt.revision);
        }
    }

    /// Records a commit's committed revision.
    pub fn committed(&self, outcome: &crate::CommitOutcome) {
        if let crate::CommitOutcome::Committed(receipt) = outcome {
            self.observe(&receipt.revision);
        }
    }

    /// The floor to forward: every selected participant, raised by `explicit`.
    /// Conflicting incarnations refuse with `RevisionForeign`: neither one
    /// proves the other's writes, including after an explicit reset minimum.
    #[must_use]
    pub fn floor(&self, explicit: &RevisionCut) -> Result<RevisionCut, StreamError> {
        fn merge(
            observed: impl Iterator<Item = Result<Option<Revision>, StreamError>>,
            capacity: usize,
            explicit: &RevisionCut,
        ) -> Result<RevisionCut, StreamError> {
            let mut cut = Vec::with_capacity(capacity.saturating_add(explicit.0.len()));
            let mut explicit = explicit.iter().peekable();
            for revision in observed {
                let Some(mut revision) = revision? else {
                    continue;
                };
                while explicit
                    .peek()
                    .is_some_and(|next| next.scope < revision.scope)
                {
                    cut.push(explicit.next().unwrap().clone());
                }
                if explicit
                    .peek()
                    .is_some_and(|next| next.scope == revision.scope)
                {
                    let next = explicit.next().unwrap();
                    if !revision.same_authority(next) {
                        return Err(StreamError::RevisionForeign);
                    }
                    if revision.index < next.index {
                        revision = next.clone();
                    }
                }
                cut.push(revision);
            }
            cut.extend(explicit.cloned());
            Ok(RevisionCut(cut))
        }
        match &self.0 {
            TrackedRevisions::All(_) => {
                let observed = self.cut();
                merge(
                    observed.values().map(|value| value.revision()),
                    observed.len(),
                    explicit,
                )
            }
            TrackedRevisions::Selected { authority, .. } => {
                let revision = authority.revision()?;
                let capacity = usize::from(revision.is_some());
                merge(std::iter::once(Ok(revision)), capacity, explicit)
            }
        }
    }

    /// Raises the wire cut `at_least` with every observed participant that
    /// `served` does not already cover. A node that already served a revision
    /// has applied it, so a follow on its session sends nothing extra and
    /// allocates nothing; a resume on another node carries the whole floor.
    pub(crate) fn raise_uncovered(
        &self,
        at_least: &mut Vec<crate::wire::Revision>,
        served: &RevisionCut,
    ) -> Result<(), StreamError> {
        let mut positions = std::collections::BTreeMap::new();
        for (index, revision) in at_least.iter().enumerate() {
            positions.entry(revision.scope.clone()).or_insert(index);
        }
        self.participants(|revision| {
            match positions
                .get(revision.scope.as_bytes())
                .map(|index| &mut at_least[*index])
            {
                Some(entry) => {
                    if entry.incarnation.as_ref() != revision.incarnation.to_be_bytes() {
                        return Err(StreamError::RevisionForeign);
                    }
                    if !served.covers(&revision) {
                        entry.index = entry.index.max(revision.index);
                    }
                }
                // Tracked scopes are unique. Appended entries never need an
                // index: later iterations can only match the original cut.
                None if !served.covers(&revision) => {
                    at_least.push(crate::wire_codec::revision_to_wire(&revision));
                }
                None => {}
            }
            Ok(())
        })
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

/// The revision to report for data captured after [`served`] returned
/// `waited`: sampled again once the capture is pinned, so it covers every
/// write the captured data includes, and still proves the minimum `served`
/// waited for. Every server read, tail and open reports through this.
pub async fn captured<P: crate::StreamProvider + ?Sized>(
    provider: &P,
    waited: Option<Revision>,
) -> Result<Option<Revision>, StreamError> {
    match waited {
        None => Ok(None),
        Some(_) => served(provider, &RevisionCut::default()).await,
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
    /// minimum it covers. Lower indexes never move the applied revision back.
    pub fn advance(&self, index: u64) {
        self.applied
            .send_modify(|applied| applied.index = applied.index.max(index));
    }

    /// Ends this incarnation: every parked and later wait refuses as unavailable.
    pub fn close(&self) {
        self.applied.send_modify(|applied| applied.closed = true);
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

#[cfg(all(test, not(target_arch = "wasm32"), not(loom)))]
mod tests {
    use super::*;

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(64))]
        #[test]
        fn minimum_cut_refuses_incarnation_replacement_in_every_input_order(
            scope in proptest::collection::vec(proptest::num::u8::ANY, 0..32),
            incarnation in proptest::num::u128::ANY,
            first_index in proptest::num::u64::ANY,
            next_index in proptest::num::u64::ANY,
        ) {
            let first = Revision { scope: RevisionScope::new(scope.clone()), incarnation, index: first_index };
            let reset = Revision { incarnation: incarnation ^ 1, index: next_index, ..first.clone() };
            for revisions in [[first.clone(), reset.clone()], [reset.clone(), first.clone()]] {
                proptest::prop_assert_eq!(RevisionCut::new(revisions.clone()), Err(StreamError::RevisionForeign));
                proptest::prop_assert_eq!(crate::wire_codec::cut_from_wire(revisions.iter().map(crate::wire_codec::revision_to_wire).collect()), Err(StreamError::RevisionForeign));
            }
            let mut cut = RevisionCut::of(first.clone());
            proptest::prop_assert_eq!(cut.observe(reset), Err(StreamError::RevisionForeign));
            proptest::prop_assert_eq!(&cut, &RevisionCut::of(first.clone()));

            // Same-authority duplicates retain their maximum in either order;
            // an unrelated scope remains an independent requirement.
            let next = Revision { index: next_index, ..first.clone() };
            let other = Revision { scope: RevisionScope::new([scope, vec![0]].concat()), ..first.clone() };
            for revisions in [vec![first.clone(), other.clone(), next.clone()], vec![next.clone(), other.clone(), first.clone()]] {
                let cut = RevisionCut::new(revisions).unwrap();
                proptest::prop_assert_eq!(cut.iter().len(), 2);
                proptest::prop_assert!(cut.covers(&first) && cut.covers(&next) && cut.covers(&other));
                proptest::prop_assert_eq!(crate::wire_codec::cut_from_wire(crate::wire_codec::cut_to_wire(&cut)).unwrap(), cut);
            }
            proptest::prop_assert!(cut.observe(next.clone()).is_ok());
            proptest::prop_assert!(cut.covers(&first) && cut.covers(&next));
        }
    }

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
        // Another scope is foreign too, even at a covered index.
        let other = AppliedRevisions::in_process().at(0);
        assert_eq!(
            authority.wait(&RevisionCut::of(other)).await,
            Err(StreamError::RevisionForeign)
        );
        // Negative control: no minimum serves the local cut at once.
        assert_eq!(
            authority.wait(&RevisionCut::default()).await,
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

    #[test]
    fn scoped_frontiers_retain_requirements_without_cloning_unrelated_authorities() {
        use prost::Message as _;
        use std::sync::Arc;
        for count in [1_u64, 1_000, 100_000] {
            let tracker = Arc::new(RevisionTracker::default());
            for index in 0..count {
                tracker.observe(&Revision {
                    scope: RevisionScope::new(index.to_be_bytes().to_vec()),
                    incarnation: 1,
                    index: 7,
                });
            }
            let selected = RevisionScope::new(0_u64.to_be_bytes().to_vec());
            let view = RevisionTracker::scoped(&tracker, selected.clone());
            let floor = view.floor(&RevisionCut::default()).unwrap();
            assert_eq!(floor.iter().len(), 1);
            assert_eq!(floor.get(&selected).unwrap().index, 7);
            let bytes: usize = floor
                .iter()
                .map(|revision| crate::wire_codec::revision_to_wire(revision).encoded_len())
                .sum();
            // Negative control: choosing the full dependency set must pay for
            // all participants. Selection cannot erase that original view.
            assert_eq!(
                tracker.floor(&RevisionCut::default()).unwrap().iter().len(),
                count as usize
            );
            eprintln!("revision_scopes={count} selected_participants=1 floor_bytes={bytes}");

            let unrelated = Revision {
                scope: RevisionScope::new(b"explicit".to_vec()),
                incarnation: 19,
                index: 81,
            };
            let explicit = RevisionCut::of(unrelated.clone());
            let floor = view.floor(&explicit).unwrap();
            assert_eq!(floor.iter().len(), 2);
            assert_eq!(floor.get(&unrelated.scope), Some(&unrelated));
            let reset = Revision {
                scope: selected.clone(),
                incarnation: 2,
                index: 1,
            };
            assert_eq!(
                view.floor(&RevisionCut::of(reset.clone())),
                Err(StreamError::RevisionForeign)
            );
            assert_eq!(
                view.floor(&RevisionCut::default())
                    .unwrap()
                    .get(&selected)
                    .unwrap()
                    .incarnation,
                1
            );

            let mut wire = vec![crate::wire_codec::revision_to_wire(&unrelated)];
            view.raise_uncovered(&mut wire, &RevisionCut::default())
                .unwrap();
            assert_eq!(wire.len(), 2);
            assert_eq!(crate::wire_codec::cut_from_wire(wire).unwrap(), floor);
            let mut warm = Vec::new();
            view.raise_uncovered(&mut warm, &floor).unwrap();
            assert!(
                warm.is_empty(),
                "a served frontier adds no warm wire control"
            );
            let mut reset_wire = vec![crate::wire_codec::revision_to_wire(&reset)];
            assert_eq!(
                view.raise_uncovered(&mut reset_wire, &floor),
                Err(StreamError::RevisionForeign)
            );
            view.observe(&Revision {
                scope: selected.clone(),
                incarnation: 1,
                index: 8,
            });
            assert_eq!(
                tracker
                    .floor(&RevisionCut::default())
                    .unwrap()
                    .get(&selected)
                    .unwrap()
                    .index,
                8
            );
            let again = RevisionTracker::scoped(&view, unrelated.scope.clone());
            match &again.0 {
                TrackedRevisions::Selected { source, .. } => assert!(Arc::ptr_eq(source, &tracker)),
                TrackedRevisions::All(_) => unreachable!(),
            }
        }
    }

    #[test]
    fn a_reconnect_to_a_lagging_replica_never_lowers_the_floor() {
        let first = AppliedRevisions::in_process();
        let second = AppliedRevisions::in_process();
        let tracker = RevisionTracker::default();
        tracker.observe(&first.at(5));
        tracker.observe(&second.at(2));
        // A resumed open on a replica that served an older cut.
        tracker.observe(&first.at(3));
        let floor = tracker.floor(&RevisionCut::default()).unwrap();
        assert_eq!(floor.get(first.scope()), Some(&first.at(5)));
        // Each authority keeps its own participant, never a max.
        assert_eq!(floor.get(second.scope()), Some(&second.at(2)));
        // An explicit lower minimum does not lower it either.
        let floor = tracker.floor(&RevisionCut::of(first.at(1))).unwrap();
        assert_eq!(floor.get(first.scope()), Some(&first.at(5)));
        // An authenticated reset observation cannot prove the old writes.
        let reset = Revision {
            incarnation: first.at(5).incarnation ^ 1,
            ..first.at(9)
        };
        tracker.observe(&reset);
        assert_eq!(
            tracker.floor(&RevisionCut::default()),
            Err(StreamError::RevisionForeign)
        );
        assert_eq!(
            tracker.served(&floor, Some(&reset)),
            Err(StreamError::InvalidResponse)
        );
    }

    #[tokio::test]
    async fn wait_applied_bounds_a_parked_read_by_its_deadline() {
        let authority = AppliedRevisions::in_process();
        // A lagging replica parks past its deadline.
        assert_eq!(
            authority
                .wait_applied(&authority.at(1), std::future::ready(()))
                .await,
            Err(StreamError::DeadlineElapsed)
        );
        // Negative control: an applied minimum serves before any deadline.
        authority.advance(1);
        assert_eq!(
            authority
                .wait_applied(&authority.at(1), std::future::pending())
                .await,
            Ok(authority.at(1))
        );
        // A foreign token refuses before parking, even with no deadline.
        let mut foreign = authority.at(1);
        foreign.incarnation ^= 1;
        assert_eq!(
            authority
                .wait_applied(&foreign, std::future::pending())
                .await,
            Err(StreamError::RevisionForeign)
        );
    }
}
