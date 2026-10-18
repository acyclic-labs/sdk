//! The enumerator cuts at every persisted journal frame: a conflicting
//! commit's retained receipt is a cut point, an elided replay is not.
//! Every sweep also runs under limits that compact the journal mid-run.
use super::*;
use acyclic_stream::{
    AppendRequest, CommitCondition, CommitMutation, CommitOutcome, CommitRequest, IdempotencyKey,
    OwnedBytes, StreamPath, StreamProvider,
};

/// The default limits, and limits that compact after every second frame:
/// after the setup append and frame 1, and again after frame 3.
fn limits() -> [LocalStreamLimits; 2] {
    [
        LocalStreamLimits::default(),
        LocalStreamLimits {
            journal_operations: 4,
            ..LocalStreamLimits::default()
        },
    ]
}

fn path() -> StreamPath {
    StreamPath::new("cut-frames").unwrap()
}
fn key(name: &'static str) -> IdempotencyKey {
    IdempotencyKey::new(OwnedBytes::from_static(name.as_bytes())).unwrap()
}
fn commit(name: &'static str, expected: u64) -> CommitRequest {
    CommitRequest {
        conditions: vec![CommitCondition::Tail {
            path: path(),
            expected,
        }],
        mutations: vec![CommitMutation::Append {
            path: path(),
            records: vec![OwnedBytes::from_static(b"record")],
        }],
        idempotency_key: key(name),
    }
}

/// (stale receipt retained, retry receipt retained, tail)
type Frames = (bool, bool, u64);

async fn cuts(limits: LocalStreamLimits) -> Result<CrashCuts<Frames>, CutSimulationError> {
    enumerate_with(
        limits,
        |stream| async move {
            stream
                .append(AppendRequest {
                    path: path(),
                    records: vec![OwnedBytes::from_static(b"setup")],
                    if_tail: Some(0),
                    idempotency_key: None,
                })
                .await
                .unwrap();
        },
        |stream| async move {
            // Frame 1: a stale condition, retained with its receipt.
            let Ok(CommitOutcome::Conflict(_)) = stream.commit(commit("stale", 0)).await else {
                return;
            };
            // Frame 2: the retry commits.
            if stream.commit(commit("retry", 1)).await.is_err() {
                return;
            }
            // Elided: a replay of the retry persists nothing.
            if stream.commit(commit("retry", 1)).await.is_err() {
                return;
            }
            // Frame 3: an unconditioned append.
            let _ = stream
                .append(AppendRequest {
                    path: path(),
                    records: vec![OwnedBytes::from_static(b"after")],
                    if_tail: None,
                    idempotency_key: None,
                })
                .await;
        },
        |stream| async move {
            let receipt = |name| {
                let stream = stream.clone();
                async move {
                    stream
                        .inspect_idempotency(key(name))
                        .await
                        .unwrap()
                        .is_some()
                }
            };
            (
                receipt("stale").await,
                receipt("retry").await,
                stream.tail(path()).await.unwrap(),
            )
        },
    )
    .await
}

/// Independent oracle: the reopened journal holds exactly the named prefix.
fn prefix(frames: usize) -> Frames {
    (frames >= 1, frames >= 2, 1 + u64::from(frames >= 2) + u64::from(frames >= 3))
}

fn mismatches(found: &CrashCuts<Frames>) -> Vec<(Cut, Frames)> {
    found
        .cuts
        .iter()
        .filter(|(cut, seen)| {
            let (Cut::BeforeWrite(k) | Cut::LostAck(k)) = *cut;
            *seen != prefix(k)
        })
        .copied()
        .collect()
}

#[tokio::test]
async fn conflict_receipts_are_cut_points_and_replays_are_not() {
    for limits in limits() {
        let found = cuts(limits).await.unwrap();
        assert_eq!(
            found.writes, 3,
            "conflict receipt or replay elision miscounted"
        );
        assert_eq!(found.clean, prefix(3));
        assert_eq!(found.cuts.len(), 7);
        assert_eq!(mismatches(&found), [], "{limits:?}");
    }
}

#[tokio::test]
async fn negative_control_an_ack_lost_before_its_flush_is_detected() {
    LOSE_UNFLUSHED_ACK.with(|flag| flag.set(true));
    for limits in limits() {
        let found = cuts(limits).await.unwrap();
        let lost: Vec<_> = mismatches(&found).into_iter().map(|(cut, _)| cut).collect();
        assert_eq!(
            lost,
            [Cut::LostAck(1), Cut::LostAck(2), Cut::LostAck(3)],
            "{limits:?}"
        );
    }
}

#[tokio::test]
async fn negative_control_a_failed_truncation_fails_the_enumeration() {
    acyclic_stream::fail_cut_truncation(true);
    let found = cuts(LocalStreamLimits::default()).await;
    assert!(
        matches!(found, Err(CutSimulationError::Truncate(_))),
        "an unfaithful crash was accepted"
    );
}
