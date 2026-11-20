use crate::revision::revision_to_wire;

/// A client's last observed revision per authority, forwarded as the floor of
/// every later read. A foreign refusal or a reply from another incarnation
/// never retires an existing requirement. A new client starts a new frontier.
#[derive(Debug)]
pub struct RevisionTracker(TrackedRevisions);

type ObservedRevisions =
    std::collections::BTreeMap<RevisionScope, crate::query_sync::Arc<ObservedAuthority>>;

#[path = "revision_participant.rs"]
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

    fn authority(&self, scope: RevisionScope) -> crate::query_sync::Arc<ObservedAuthority> {
        if let TrackedRevisions::Selected { authority, .. } = &self.0
            && authority.scope == scope
        {
            return crate::query_sync::Arc::clone(authority);
        }
        crate::query_sync::Arc::clone(self.cut().entry(scope).or_insert_with_key(|scope| {
            crate::query_sync::Arc::new(ObservedAuthority::new(scope.clone()))
        }))
    }

    #[cfg(any(
        test,
        not(target_arch = "wasm32"),
        all(feature = "wasm", target_arch = "wasm32")
    ))]
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
            authority: source.authority(scope),
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
        apply(&self.authority(scope.clone()))
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
    pub fn floor(&self, explicit: &RevisionCut) -> Result<RevisionCut, StreamError> {
        fn merge(
            observed: impl Iterator<Item = Result<Option<Revision>, StreamError>>,
            capacity: usize,
            explicit: &RevisionCut,
        ) -> Result<RevisionCut, StreamError> {
            let mut cut = Vec::with_capacity(capacity.saturating_add(explicit.iter().len()));
            let mut explicit = explicit.iter().peekable();
            for revision in observed {
                let Some(mut revision) = revision? else {
                    continue;
                };
                while let Some(next) = explicit.next_if(|next| next.scope < revision.scope) {
                    cut.push(next.clone());
                }
                if let Some(next) = explicit.next_if(|next| next.scope == revision.scope) {
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
            RevisionCut::new(cut)
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
    #[cfg(any(
        test,
        not(target_arch = "wasm32"),
        all(feature = "wasm", target_arch = "wasm32")
    ))]
    pub fn raise_uncovered(
        &self,
        at_least: &mut Vec<acyclic_stream_wire::wire::Revision>,
        served: &RevisionCut,
    ) -> Result<(), StreamError> {
        let mut positions = std::collections::BTreeMap::new();
        for (index, revision) in at_least.iter().enumerate() {
            positions.entry(revision.scope.clone()).or_insert(index);
        }
        self.participants(|revision| {
            match positions.get(revision.scope.as_bytes()).copied() {
                Some(index) => {
                    let entry = at_least
                        .get_mut(index)
                        .ok_or(StreamError::InvalidResponse)?;
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
                    at_least.push(revision_to_wire(&revision));
                }
                None => {}
            }
            Ok(())
        })
    }
}
#[cfg(all(test, not(target_arch = "wasm32"), not(loom)))]
mod tests {
    use super::*;
    use crate::revision::{cut_from_wire, cut_to_wire};

    #[cfg(feature = "serde")]
    #[test]
    fn json_cuts_preserve_positions_above_javascript_integer_precision() -> serde_json::Result<()> {
        for index in [(1 << 53) + 1, u64::MAX] {
            let original = Revision {
                scope: RevisionScope::new(vec![1]),
                incarnation: 2,
                index,
            };
            let encoded = serde_json::to_value(&original)?;
            assert_eq!(encoded["index"], index.to_string());
            assert_eq!(
                serde_json::from_value::<Revision>(encoded.clone())?,
                original
            );
            let mut rounded = encoded;
            rounded["index"] = serde_json::Value::from(index);
            assert!(serde_json::from_value::<Revision>(rounded).is_err());
            let positions = std::collections::BTreeMap::from([("original".to_owned(), index)]);
            #[derive(serde::Serialize, serde::Deserialize)]
            struct Positions(
                #[serde(with = "acyclic_decimal_u64::map")] std::collections::BTreeMap<String, u64>,
            );
            let encoded = serde_json::to_value(Positions(positions.clone()))?;
            assert_eq!(encoded["original"], index.to_string());
            assert_eq!(serde_json::from_value::<Positions>(encoded)?.0, positions);
        }
        Ok(())
    }
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
                proptest::prop_assert_eq!(RevisionCut::new(revisions.clone()).map_err(|error| error.category()), Err(StreamCategory::RevisionForeign));
                proptest::prop_assert_eq!(cut_from_wire(revisions.iter().map(revision_to_wire).collect()).map_err(|error| error.category()), Err(StreamCategory::RevisionForeign));
            }
            let mut cut = RevisionCut::of(first.clone());
            proptest::prop_assert_eq!(cut.observe(reset).map_err(|error| error.category()), Err(StreamCategory::RevisionForeign));
            proptest::prop_assert_eq!(&cut, &RevisionCut::of(first.clone()));

            // Same-authority duplicates retain their maximum in either order;
            // an unrelated scope remains an independent requirement.
            let next = Revision { index: next_index, ..first.clone() };
            let other = Revision {
                scope: RevisionScope::new([scope, vec![0]].concat()),
                incarnation: first.incarnation,
                index: first.index,
            };
            for revisions in [vec![first.clone(), other.clone(), next.clone()], vec![next.clone(), other.clone(), first.clone()]] {
                let cut = RevisionCut::new(revisions).unwrap();
                proptest::prop_assert_eq!(cut.iter().len(), 2);
                proptest::prop_assert!(cut.covers(&first) && cut.covers(&next) && cut.covers(&other));
                proptest::prop_assert_eq!(cut_from_wire(cut_to_wire(&cut)).unwrap(), cut);
            }
            proptest::prop_assert!(cut.observe(next.clone()).is_ok());
            proptest::prop_assert!(cut.covers(&first) && cut.covers(&next));

            // Compare the streaming floor merge with independent canonical
            // construction, including an explicit scope before, at and after
            // the observed scope, and either empty side.
            for observed in [vec![], vec![first.clone()], vec![other.clone()], vec![first.clone(), other.clone()]] {
                let tracker = RevisionTracker::default();
                for revision in &observed {
                    tracker.observe(revision);
                }
                for minimum in [RevisionCut::default(), RevisionCut::of(next.clone()), RevisionCut::of(other.clone()), RevisionCut::new([next.clone(), other.clone()]).unwrap()] {
                    let expected = RevisionCut::new(observed.iter().cloned().chain(minimum.iter().cloned()));
                    proptest::prop_assert_eq!(
                        tracker.floor(&minimum).map_err(|error| error.category()),
                        expected.map_err(|error| error.category()),
                        "revision floor lost an original participant or maximum"
                    );
                }
            }
            let tracker = RevisionTracker::default();
            tracker.observe(&first);
            proptest::prop_assert_eq!(tracker.floor(&RevisionCut::of(Revision { incarnation: incarnation ^ 1, ..next })).map_err(|error| error.category()), Err(StreamCategory::RevisionForeign), "revision floor accepted a foreign incarnation");
        }
    }

    #[tokio::test]
    async fn parks_until_applied_and_refuses_foreign_incarnations() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        struct Observer(AtomicUsize);
        impl futures::task::ArcWake for Observer {
            fn wake_by_ref(observer: &Arc<Self>) {
                observer.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let authority = AppliedRevisions::in_process();
        let wanted = authority.at(2);
        let cut = RevisionCut::of(wanted.clone());
        let parked = authority.wait(&cut);
        tokio::pin!(parked);
        let observer = Arc::new(Observer(AtomicUsize::new(0)));
        let waker = futures::task::waker(observer.clone());
        let mut context = std::task::Context::from_waker(&waker);
        assert!(parked.as_mut().poll(&mut context).is_pending());
        for index in [0, 0] {
            authority.advance(index);
            assert_eq!(
                observer.0.load(Ordering::Relaxed),
                0,
                "unchanged applied revision woke a parked reader"
            );
            assert!(parked.as_mut().poll(&mut context).is_pending());
        }
        authority.advance(1);
        assert_eq!(observer.0.load(Ordering::Relaxed), 1);
        assert!(
            parked.as_mut().poll(&mut context).is_pending(),
            "applied 1 must not satisfy minimum 2"
        );
        let covered = RevisionCut::of(authority.at(1));
        for index in [1, 0, 1] {
            authority.advance(index);
            assert_eq!(
                observer.0.load(Ordering::Relaxed),
                1,
                "unchanged applied revision woke a parked reader"
            );
            assert!(parked.as_mut().poll(&mut context).is_pending());
            assert_eq!(
                futures::FutureExt::now_or_never(authority.wait(&covered))
                    .map(|result| result.map_err(|error| error.category())),
                Some(Ok(authority.at(1))),
                "older publication regressed a covered revision"
            );
        }
        authority.advance(2);
        assert_eq!(observer.0.load(Ordering::Relaxed), 2);
        assert_eq!(parked.await.unwrap(), wanted);
        assert_eq!(authority.wait(&cut).await.unwrap(), wanted);

        let mut foreign = wanted.clone();
        foreign.incarnation ^= 1;
        assert_eq!(
            authority
                .wait(&RevisionCut::of(foreign))
                .await
                .map_err(|error| error.category()),
            Err(StreamCategory::RevisionForeign)
        );
        // Another scope is foreign too, even at a covered index.
        let other = AppliedRevisions::in_process().at(0);
        assert_eq!(
            authority
                .wait(&RevisionCut::of(other))
                .await
                .map_err(|error| error.category()),
            Err(StreamCategory::RevisionForeign)
        );
        // Negative control: no minimum serves the local cut at once.
        assert_eq!(
            authority
                .wait(&RevisionCut::default())
                .await
                .map_err(|error| error.category()),
            Ok(authority.at(2))
        );

        let closing_cut = RevisionCut::of(authority.at(9));
        let closing = authority.wait(&closing_cut);
        tokio::pin!(closing);
        assert!(closing.as_mut().poll(&mut context).is_pending());
        authority.close();
        assert_eq!(
            observer.0.load(Ordering::Relaxed),
            3,
            "retired revision authority did not wake its parked reader"
        );
        assert!(
            matches!(
                closing.as_mut().poll(&mut context),
                std::task::Poll::Ready(Err(ref error))
                    if error.category() == StreamCategory::Unavailable
            ),
            "retired revision authority left its reader parked"
        );
        assert_eq!(
            authority
                .wait(&closing_cut)
                .await
                .map_err(|error| error.category()),
            Err(StreamCategory::Unavailable)
        );
    }

    #[test]
    fn scoped_frontiers_retain_requirements_without_cloning_unrelated_authorities() {
        use prost::Message as _;
        use std::sync::Arc;
        for count in [1_usize, 1_000, 100_000] {
            let tracker = Arc::new(RevisionTracker::default());
            for index in 0..count {
                tracker.observe(&Revision {
                    scope: RevisionScope::new(index.to_be_bytes().to_vec()),
                    incarnation: 1,
                    index: 7,
                });
            }
            let selected = RevisionScope::new(0_usize.to_be_bytes().to_vec());
            let view = RevisionTracker::scoped(&tracker, selected.clone());
            let floor = view.floor(&RevisionCut::default()).unwrap();
            assert_eq!(floor.iter().len(), 1);
            assert_eq!(floor.get(&selected).unwrap().index, 7);
            let bytes: usize = floor
                .iter()
                .map(|revision| revision_to_wire(revision).encoded_len())
                .sum();
            // Negative control: choosing the full dependency set must pay for
            // all participants. Selection cannot erase that original view.
            assert_eq!(
                tracker.floor(&RevisionCut::default()).unwrap().iter().len(),
                count
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
                view.floor(&RevisionCut::of(reset.clone()))
                    .map_err(|error| error.category()),
                Err(StreamCategory::RevisionForeign)
            );
            assert_eq!(
                view.floor(&RevisionCut::default())
                    .unwrap()
                    .get(&selected)
                    .unwrap()
                    .incarnation,
                1
            );

            let mut wire = vec![revision_to_wire(&unrelated)];
            view.raise_uncovered(&mut wire, &RevisionCut::default())
                .unwrap();
            assert_eq!(wire.len(), 2);
            assert_eq!(cut_from_wire(wire).unwrap(), floor);
            let mut warm = Vec::new();
            view.raise_uncovered(&mut warm, &floor).unwrap();
            assert!(
                warm.is_empty(),
                "a served frontier adds no warm wire control"
            );
            let mut reset_wire = vec![revision_to_wire(&reset)];
            assert_eq!(
                view.raise_uncovered(&mut reset_wire, &floor)
                    .map_err(|error| error.category()),
                Err(StreamCategory::RevisionForeign)
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
            tracker
                .floor(&RevisionCut::default())
                .map_err(|error| error.category()),
            Err(StreamCategory::RevisionForeign)
        );
        assert_eq!(
            tracker
                .served(&floor, Some(&reset))
                .map_err(|error| error.category()),
            Err(StreamCategory::InvalidResponse)
        );
    }

    #[tokio::test]
    async fn wait_applied_bounds_a_parked_read_by_its_deadline() {
        let authority = AppliedRevisions::in_process();
        // A lagging replica parks past its deadline.
        assert_eq!(
            authority
                .wait_applied(&authority.at(1), std::future::ready(()))
                .await
                .map_err(|error| error.category()),
            Err(StreamCategory::DeadlineElapsed)
        );
        // Negative control: an applied minimum serves before any deadline.
        authority.advance(1);
        assert_eq!(
            authority
                .wait_applied(&authority.at(1), std::future::pending())
                .await
                .map_err(|error| error.category()),
            Ok(authority.at(1))
        );
        // A foreign token refuses before parking, even with no deadline.
        let mut foreign = authority.at(1);
        foreign.incarnation ^= 1;
        assert_eq!(
            authority
                .wait_applied(&foreign, std::future::pending())
                .await
                .map_err(|error| error.category()),
            Err(StreamCategory::RevisionForeign)
        );
    }
}
