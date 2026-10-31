use super::*;
use protocol::{Completion, Demand, Refusal, State};

/// Deliberately enumerates every source item. Unlike the implementation this
/// oracle neither chooses a cut nor reconstructs coverage by invoking compact.
fn check_partition(head: usize, summaries: &[std::ops::Range<usize>], verbatim: &[usize]) -> bool {
    let mut occurrences = vec![0_u32; head];
    for item in summaries
        .iter()
        .flat_map(Clone::clone)
        .chain(verbatim.iter().copied())
    {
        let Some(count) = occurrences.get_mut(item) else {
            return false;
        };
        *count += 1;
    }
    occurrences.into_iter().all(|count| count == 1)
}

#[test]
fn partition_oracle_rejects_window_loss_overlap_and_invented_history() {
    let items: Vec<_> = (0..100).collect();
    assert!(check_partition(100, &[], &items));
    assert!(check_partition(100, &[0..70], &items[70..]));
    // The former sliding window loses the first 36 items with no coverage.
    assert!(!check_partition(100, &[], &items[36..]));
    assert!(!check_partition(100, &[0..70], &items[69..]));
    assert!(!check_partition(100, &[0..70], &items[71..]));
    assert!(!check_partition(100, &[0..101], &[]));
}

#[test]
fn cuts_keep_open_exchanges_and_refuse_noncontiguous_observation() {
    let mut cut = Cut::after(9);
    assert!(cut.push(10, Boundary::Closed));
    assert!(cut.push(11, Boundary::Open));
    assert!(cut.push(12, Boundary::Open));
    assert_eq!(cut.cursor(), 10);
    let before = cut;
    assert!(!cut.push(14, Boundary::Closed));
    assert_eq!(cut, before);
    assert!(!cut.push(12, Boundary::Closed));
    assert!(cut.push(13, Boundary::Closed));
    assert_eq!(cut.cursor(), 13);
    assert!(!Cut::after(u64::MAX).push(0, Boundary::Closed));
    // The unguarded last-item cut splits this still-open reasoning/tool group.
    let mut open = Cut::after(9);
    assert!(open.push(10, Boundary::Open));
    assert_ne!(open.cursor(), 10);
}

#[test]
fn budget_bound_preserves_reservation_and_refuses_overflow() {
    assert_eq!(token_bound(50, 20, 3), Some(73));
    assert!(fits(73, 27, 100));
    assert!(!fits(74, 27, 100));
    assert!(!fits(0, 101, 100));
    assert_eq!(token_bound(u64::MAX, 1, 0), None);
    assert_eq!(token_bound(u64::MAX - 1, 1, 1), None);
    // Wrapping arithmetic would admit this request as a zero-token input.
    assert!(fits(u64::MAX.wrapping_add(1), 1, 100));
    assert!(!fits(u64::MAX, 1, 100));
}

#[test]
fn source_range_and_renderer_model_identity_fence_shared_summaries() {
    let key = SummaryKey {
        source: StreamPath::new("account/agent/conversation").unwrap(),
        through: 30,
        renderer: 1,
        model: [1; 32],
    };
    let digest = key.digest();
    let mut other = key.clone();
    other.through += 1;
    assert_ne!(digest, other.digest());
    other = key.clone();
    other.renderer += 1;
    assert_ne!(digest, other.digest());
    other = key.clone();
    other.model = [2; 32];
    assert_ne!(digest, other.digest());
    other = key.clone();
    other.source = StreamPath::new("account/child/conversation").unwrap();
    assert_ne!(digest, other.digest());
    let invalid = Epoch {
        header: [0; 32],
        profile: [0; 32],
        key,
        summary: None,
    };
    assert_eq!(
        invalid.validate(&other.source, 30),
        Err(InvalidEpoch::Source)
    );
    assert_eq!(
        invalid.validate(&invalid.key.source, 29),
        Err(InvalidEpoch::OutsidePrefix)
    );
    assert_eq!(
        invalid.validate(&invalid.key.source, 30),
        Err(InvalidEpoch::MissingCoverage)
    );
}

/// Full materialized model: a summary marks source items individually. It
/// intentionally pays O(history) to serve as an independent coverage oracle.
#[derive(Clone, Default)]
struct Reference {
    items: Vec<bool>,
    swaps: Vec<usize>,
    job: Option<(protocol::SummaryId, usize, Option<Completion>)>,
}

impl Reference {
    fn verify(&self, state: State) {
        let cursor = self.items.iter().take_while(|covered| **covered).count();
        assert!(self.items[cursor..].iter().all(|covered| !covered));
        assert_eq!(state.cursor(), cursor as u64);
        assert_eq!(state.generation(), self.swaps.len() as u64);
        let ranges = if cursor == 0 {
            Vec::new()
        } else {
            vec![0..cursor]
        };
        let verbatim: Vec<_> = (cursor..self.items.len()).collect();
        assert!(check_partition(self.items.len(), &ranges, &verbatim));
        assert_eq!(
            state
                .pending()
                .map(|p| (p.summary, p.through as usize, p.completion)),
            self.job
        );
    }

    fn prepare(&mut self, id: protocol::SummaryId, end: usize) -> bool {
        let remaining = self
            .items
            .get(..end)
            .is_some_and(|items| items.iter().any(|covered| !covered));
        if !remaining {
            return false;
        }
        match self.job {
            None => {
                self.job = Some((id, end, None));
                true
            }
            Some((key, through, _)) => key == id && through == end,
        }
    }

    fn complete(&mut self, id: protocol::SummaryId, result: Completion) -> bool {
        let Some((key, _, outcome)) = &mut self.job else {
            return false;
        };
        if *key != id || outcome.is_some_and(|prior| prior != result) {
            return false;
        }
        *outcome = Some(result);
        true
    }

    fn swap(&mut self, id: protocol::SummaryId, expected: u64) -> bool {
        let Some((key, end, Some(Completion::Ready))) = self.job else {
            return false;
        };
        if key != id || expected != self.swaps.len() as u64 {
            return false;
        }
        for covered in &mut self.items[..end] {
            *covered = true;
        }
        self.swaps.push(end);
        self.job = None;
        true
    }
}

proptest::proptest! {
    #[test]
    fn epoch_reducer_matches_materialized_history_at_every_commit_cut(
        schedule in proptest::collection::vec((0_u8..6, 0_u8..8, proptest::bool::ANY), 1..128)
    ) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all().build().unwrap()
            .block_on(replay_snapshot_schedule(schedule));
    }
}

async fn replay_snapshot_schedule(schedule: Vec<(u8, u8, bool)>) {
    use acyclic_stream::{
        AppendRecord, CommitMutation, CommitOutcome, CommitRequest, IdempotencyKey, MemoryStream,
        StreamProvider,
    };

    let provider = MemoryStream::default();
    let source = StreamPath::new("account/agent/conversation").unwrap();
    let path = StreamPath::new("account/agent/epochs").unwrap();
    let genesis = Snapshot::genesis(Epoch {
        header: [1; 32],
        profile: [2; 32],
        summary: None,
        key: SummaryKey {
            source: source.clone(),
            through: 0,
            renderer: 1,
            model: [3; 32],
        },
    })
    .unwrap();
    assert!(matches!(
        provider
            .commit(snapshot_request(&genesis, path.clone()))
            .await
            .unwrap(),
        CommitOutcome::Committed(_)
    ));
    let body =
        super::super::tests::file(crate::AgentId::from_bytes([1; 16]), "summary.txt").unwrap();
    let mut reference = Reference::default();
    let mut swapped = false;
    let mut interrupted = [false; 3];
    let mut publications = 1;
    // Reach a real swap in every generated case; random suffixes then
    // interrupt preparations, completions and later swaps at commit.
    let prefix = [
        (0, 0, false),
        (0, 0, false),
        (1, 2, true),
        (1, 2, false),
        (2, 0, true),
        (2, 0, false),
        (4, 0, true),
        (4, 0, false),
    ];
    for (operation, argument, crash_before_commit) in prefix.into_iter().chain(schedule) {
        let head = reference.items.len() as u64;
        let durable =
            Snapshot::decode(&last_snapshot(&provider, &path).await, &source, head).unwrap();
        if operation == 0 {
            let request = CommitRequest {
                deadline_unix_millis: None,
                conditions: Vec::new(),
                mutations: vec![CommitMutation::Append {
                    path: source.clone(),
                    records: vec![AppendRecord::from(vec![argument])],
                }],
                idempotency_key: IdempotencyKey::unique(),
            };
            if !crash_before_commit {
                assert!(matches!(
                    provider.commit(request).await.unwrap(),
                    CommitOutcome::Committed(_)
                ));
                reference.items.push(false);
            }
        } else {
            let mut candidate = durable.clone();
            let mut expected = reference.clone();
            let id = if operation == 1 {
                candidate.epoch().key.digest_through(u64::from(argument))
            } else {
                candidate
                    .state()
                    .pending()
                    .filter(|_| argument % 2 == 0)
                    .map(|p| p.summary)
                    .unwrap_or_else(|| candidate.epoch().key.digest_through(head + 1))
            };
            let (actual, accepted) = match operation {
                1 => (
                    candidate.prepare(u64::from(argument), head).is_ok(),
                    expected.prepare(id, usize::from(argument)),
                ),
                2 | 3 => {
                    let (result, outcome) = if operation == 2 {
                        (Completion::Ready, SummaryOutcome::Ready(body.clone()))
                    } else {
                        (Completion::Failed, SummaryOutcome::Failed)
                    };
                    (
                        candidate.complete(id, outcome).is_ok(),
                        expected.complete(id, result),
                    )
                }
                _ => {
                    let mut next = candidate.epoch().clone();
                    next.key.through = candidate
                        .state()
                        .pending()
                        .filter(|_| argument % 2 == 0)
                        .map_or(head + 1, |p| p.through);
                    next.summary = Some(body.clone());
                    // Odd selectors represent a result from another
                    // preparation, not a request to repair its identity.
                    let actual = candidate.swap(u64::from(argument / 2), next, head).is_ok();
                    (actual, expected.swap(id, u64::from(argument / 2)))
                }
            };
            assert_eq!(actual, accepted);
            if !actual {
                assert_eq!(candidate, durable);
            } else {
                let request = snapshot_request(&candidate, path.clone());
                if crash_before_commit {
                    interrupted[match operation {
                        1 => 0,
                        2 | 3 => 1,
                        _ => 2,
                    }] = true;
                } else {
                    assert!(matches!(
                        provider.commit(request).await.unwrap(),
                        CommitOutcome::Committed(_)
                    ));
                    swapped |= candidate.state().generation() > durable.state().generation();
                    publications += 1;
                    reference = expected;
                }
            }
        }
        // Discard every volatile candidate and reopen after every
        // step, including both sides of each commit/crash boundary.
        let reopened = Snapshot::decode(
            &last_snapshot(&provider, &path).await,
            &source,
            reference.items.len() as u64,
        )
        .unwrap();
        assert_eq!(
            provider.tail(source.clone()).await.unwrap(),
            reference.items.len() as u64
        );
        assert_eq!(
            provider
                .tail(StreamPath::new("account/agent/transaction-marker").unwrap())
                .await
                .unwrap(),
            publications
        );
        reference.verify(reopened.state());
    }
    assert!(swapped, "generated execution never reached a swap");
    assert!(
        interrupted.into_iter().all(|reached| reached),
        "generated execution did not interrupt every publication boundary"
    );
}

async fn last_snapshot(
    provider: &acyclic_stream::MemoryStream,
    path: &StreamPath,
) -> acyclic_stream::Record {
    use acyclic_stream::{ReadRequest, StreamProvider, StreamProviderExt as _};
    use futures::TryStreamExt as _;

    let tail = provider.tail(path.clone()).await.unwrap();
    let mut records = provider
        .read(ReadRequest {
            key: None,
            path: path.clone(),
            from: tail - 1,
            limit: 1,
            at_least: Default::default(),
        })
        .await
        .unwrap();
    let record = records.try_next().await.unwrap().unwrap();
    assert!(records.try_next().await.unwrap().is_none());
    record
}

fn snapshot_request(snapshot: &Snapshot, path: StreamPath) -> acyclic_stream::CommitRequest {
    use acyclic_stream::{AppendRecord, CommitMutation, CommitRequest, IdempotencyKey};

    // A preexisting mutation witnesses that epoch conditions guard the whole
    // caller transaction, not a separately submitted epoch-only request.
    let mut request = CommitRequest {
        deadline_unix_millis: None,
        conditions: Vec::new(),
        mutations: vec![CommitMutation::Append {
            path: StreamPath::new("account/agent/transaction-marker").unwrap(),
            records: vec![AppendRecord::from("joined")],
        }],
        idempotency_key: IdempotencyKey::unique(),
    };
    snapshot.append_to(path, &mut request).unwrap();
    request
}

#[test]
fn fanout_waits_for_the_parent_swap_without_discarding_pending_custody() {
    let id = [7; 32];
    let mut parent = State::default();
    parent.prepare(id, 20, 25).unwrap();
    assert_eq!(parent.demand(), Demand::Await(id));
    assert_eq!(parent.fork(25), Err(Refusal::NotReady));
    assert_eq!(parent.fork(19), Err(Refusal::NotReady));
    assert_eq!(parent.prepare([8; 32], 24, 25), Err(Refusal::Busy));
    assert_eq!(parent.swap(id, 0, 25), Err(Refusal::NotReady));
    let before = parent;
    let mut uncommitted = parent;
    uncommitted.complete(id, Completion::Ready).unwrap();
    assert_eq!(before.demand(), Demand::Await(id));
    assert_eq!(uncommitted.fork(25), Err(Refusal::NotReady));
    parent = uncommitted;
    parent.swap(id, 0, 30).unwrap();
    let child = parent.fork(25).unwrap();
    assert_eq!(parent.cursor(), 20);
    assert_eq!(child.cursor(), 20);
    assert_eq!(parent.swap(id, 0, 30), Err(Refusal::Stale));
    assert_eq!(child.fork(19), Err(Refusal::Range));
    let mut failed = before;
    failed.complete(id, Completion::Failed).unwrap();
    assert_eq!(failed.fork(25), Err(Refusal::NotReady));
    // The former projection dropped a pending slot when its cut lay beyond the
    // fork, permitting fan-out while the parent still awaited its producer.
    let dropped_pending = State::restore(0, 0, None, 19).unwrap();
    assert!(dropped_pending.fork(19).is_ok());
    assert_eq!(before.fork(19), Err(Refusal::NotReady));
}

#[test]
fn producer_failure_is_terminal_and_cannot_be_relabelled_ready() {
    let id = [3; 32];
    let mut state = State::default();
    state.prepare(id, 1, 1).unwrap();
    state.complete(id, Completion::Failed).unwrap();
    assert_eq!(state.demand(), Demand::Failed(id));
    assert_eq!(
        state.complete(id, Completion::Ready),
        Err(Refusal::Contradiction)
    );
    assert_eq!(state.swap(id, 0, 1), Err(Refusal::NotReady));
    let unchanged = state;
    assert_eq!(
        state.complete([4; 32], Completion::Ready),
        Err(Refusal::Stale)
    );
    assert_eq!(state, unchanged);
}

#[test]
fn restoration_retains_pending_custody_and_refuses_torn_epoch_state() {
    let id = [5; 32];
    let mut state = State::default();
    state.prepare(id, 10, 12).unwrap();
    for completion in [None, Some(Completion::Ready), Some(Completion::Failed)] {
        let pending = protocol::Pending {
            completion,
            ..state.pending().unwrap()
        };
        let restored = State::restore(0, 0, Some(pending), 12).unwrap();
        let mut expected = state;
        if let Some(outcome) = completion {
            expected.complete(id, outcome).unwrap();
        }
        assert_eq!(restored, expected);
        assert_eq!(State::restore(1, 0, Some(pending), 12), Err(Refusal::Stale));
        assert_eq!(
            State::restore(0, 10, Some(pending), 12),
            Err(Refusal::Range)
        );
        assert_eq!(State::restore(0, 0, Some(pending), 9), Err(Refusal::Range));
    }
    assert_eq!(State::restore(0, 13, None, 12), Err(Refusal::Range));
    // Dropping the pending slot on restore would grant another producer.
    let mut lost = State::default();
    assert!(lost.prepare([6; 32], 11, 12).is_ok());
    assert_eq!(state.prepare([6; 32], 11, 12), Err(Refusal::Busy));
}

#[tokio::test]
async fn conditional_snapshots_preserve_coverage_and_one_pending_identity_across_reopen() {
    use acyclic_stream::{CommitOutcome, MemoryStream, StreamProvider};

    let provider = MemoryStream::default();
    let source = StreamPath::new("account/agent/conversation").unwrap();
    let path = StreamPath::new("account/agent/epochs").unwrap();
    let marker = StreamPath::new("account/agent/transaction-marker").unwrap();
    let genesis = Epoch {
        header: [1; 32],
        profile: [2; 32],
        summary: None,
        key: SummaryKey {
            source: source.clone(),
            through: 0,
            renderer: 1,
            model: [3; 32],
        },
    };
    let initial = Snapshot::genesis(genesis).unwrap();
    let request = snapshot_request(&initial, path.clone());
    assert!(matches!(
        provider.commit(request).await.unwrap(),
        CommitOutcome::Committed(_)
    ));
    // A caller cannot attach an old genesis to a newer observed head.
    assert!(matches!(
        provider
            .commit(snapshot_request(&initial, path.clone()))
            .await
            .unwrap(),
        CommitOutcome::Conflict(_)
    ));

    let record = last_snapshot(&provider, &path).await;
    assert_eq!(provider.tail(marker.clone()).await.unwrap(), 1);
    let mut first = Snapshot::decode(&record, &source, 12).unwrap();
    let mut competing = first.clone();
    let summary = first.prepare(10, 12).unwrap();
    competing.prepare(11, 12).unwrap();
    let request = snapshot_request(&first, path.clone());
    // A crash before commit leaves the previous complete snapshot intact.
    assert_eq!(
        Snapshot::decode(&last_snapshot(&provider, &path).await, &source, 12)
            .unwrap()
            .state(),
        State::default()
    );
    assert!(matches!(
        provider.commit(request).await.unwrap(),
        CommitOutcome::Committed(_)
    ));
    assert!(matches!(
        provider
            .commit(snapshot_request(&competing, path.clone()))
            .await
            .unwrap(),
        CommitOutcome::Conflict(_)
    ));

    let prepared_record = last_snapshot(&provider, &path).await;
    assert_eq!(provider.tail(marker.clone()).await.unwrap(), 2);
    let mut reopened = Snapshot::decode(&prepared_record, &source, 12).unwrap();
    assert_eq!(reopened.state().demand(), Demand::Await(summary));
    assert!(matches!(
        reopened.prepare(11, 12),
        Err(SnapshotError::Transition(Refusal::Busy))
    ));
    let mut next = reopened.epoch().clone();
    next.key.through = 10;
    next.header = [4; 32];
    next.summary = Some(
        super::super::tests::file(crate::AgentId::from_bytes([1; 16]), "summary.txt").unwrap(),
    );
    let unchanged = reopened.clone();
    assert!(matches!(
        reopened.swap(0, next.clone(), 12),
        Err(SnapshotError::Transition(Refusal::NotReady))
    ));
    assert_eq!(reopened, unchanged);

    let body = next.summary.clone().unwrap();
    reopened
        .complete(summary, SummaryOutcome::Ready(body.clone()))
        .unwrap();
    let other_body =
        super::super::tests::file(crate::AgentId::from_bytes([1; 16]), "other-summary.txt")
            .unwrap();
    let ready = reopened.clone();
    assert!(matches!(
        reopened.complete(summary, SummaryOutcome::Ready(other_body.clone())),
        Err(SnapshotError::Transition(Refusal::Contradiction))
    ));
    assert!(matches!(
        reopened.complete(summary, SummaryOutcome::Failed),
        Err(SnapshotError::Transition(Refusal::Contradiction))
    ));
    assert_eq!(reopened, ready);
    let request = snapshot_request(&reopened, path.clone());
    assert!(matches!(
        provider.commit(request).await.unwrap(),
        CommitOutcome::Committed(_)
    ));
    let mut reopened =
        Snapshot::decode(&last_snapshot(&provider, &path).await, &source, 12).unwrap();
    assert_eq!(reopened.outcome(), Some(&SummaryOutcome::Ready(body)));
    let ready = reopened.clone();
    let mut wrong = next.clone();
    wrong.key.through = 11;
    assert!(matches!(
        reopened.swap(0, wrong, 12),
        Err(SnapshotError::Identity)
    ));
    let mut wrong = next.clone();
    wrong.summary = Some(other_body);
    // The former unbound-ready route accepted both this epoch and its swap.
    // The result association is required in addition to those two checks.
    assert!(wrong.validate(&source, 12).is_ok());
    let mut unbound = reopened.state();
    assert!(unbound.swap(summary, 0, 12).is_ok());
    assert!(matches!(
        reopened.swap(0, wrong, 12),
        Err(SnapshotError::ResultReference)
    ));
    assert_eq!(reopened, ready);
    reopened.swap(0, next, 12).unwrap();
    let request = snapshot_request(&reopened, path.clone());
    assert!(matches!(
        provider.commit(request).await.unwrap(),
        CommitOutcome::Committed(_)
    ));
    let final_record = last_snapshot(&provider, &path).await;
    let settled = Snapshot::decode(&final_record, &source, 12).unwrap();
    assert_eq!(settled.state().cursor(), 10);
    assert_eq!(settled.state().generation(), 1);
    assert!(settled.outcome().is_none());
    assert!(check_partition(12, &[0..10], &[10, 11]));
    // The unchanged stale producer cannot replace the committed epoch.
    assert!(matches!(
        provider
            .commit(snapshot_request(&competing, path.clone()))
            .await
            .unwrap(),
        CommitOutcome::Conflict(_)
    ));

    // Removing the exact-head condition is the real publication negative: the
    // same stale producer now overwrites coverage in the authoritative stream.
    let mut bypass = snapshot_request(&competing, path.clone());
    assert_eq!(provider.tail(marker.clone()).await.unwrap(), 4);
    bypass.conditions = vec![acyclic_stream::CommitCondition::Present { path: path.clone() }];
    assert!(matches!(
        provider.commit(bypass).await.unwrap(),
        CommitOutcome::Committed(_)
    ));
    assert_eq!(provider.tail(marker).await.unwrap(), 5);
    assert_ne!(
        Snapshot::decode(&last_snapshot(&provider, &path).await, &source, 12)
            .unwrap()
            .state()
            .cursor(),
        settled.state().cursor()
    );

    let mut corrupt: serde_json::Value = serde_json::from_slice(&prepared_record.value).unwrap();
    corrupt["snapshot"]["pending"]["summary"][0] = serde_json::json!(summary[0] ^ 1);
    let mut corrupt_record = prepared_record;
    corrupt_record.value = serde_json::to_vec(&corrupt).unwrap().into();
    assert!(matches!(
        Snapshot::decode(&corrupt_record, &source, 12),
        Err(SnapshotError::Identity)
    ));
}
