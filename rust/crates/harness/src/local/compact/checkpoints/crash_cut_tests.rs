//! Crash after each durable write of checkpoint publication, reopen the
//! directory, replay the same publication and a stale one, and check the
//! result against the files found at reopen.
use super::*;
use crate::test_support::crash_cuts::{Cut, enumerate};
use acyclic_stream::{LocalStream, StreamPath};
use std::cell::RefCell;

const MAXIMUM: u64 = 4096;

#[derive(Debug)]
struct Recovered {
    retained: bool,
    selected_before: bool,
    recovery: std::result::Result<(FileKey, bool), String>,
    selected_after: Option<FileKey>,
    file_matches: bool,
    stale_selected: Option<bool>,
    stale_kept: bool,
    replay_stable: bool,
}

fn path() -> StreamPath {
    StreamPath::new("summary-checkpoints").unwrap()
}
fn task() -> TaskId {
    TaskId::from_bytes([1; 16])
}
fn operation() -> OperationId {
    OperationId::from_bytes([2; 16])
}
fn key(checkpoint: &SummaryCheckpoint) -> FileKey {
    checkpoint_key(task(), checkpoint.boundary.through.position, operation()).unwrap()
}

/// Selects an earlier checkpoint, then derives the next one from that exact
/// selection, as manual compaction does when it captures its source.
async fn prepare<P: StreamProvider>(volume: &TaskVolume<P>) -> SummaryCheckpoint {
    let initial = SummarySelection {
        continuation: None,
        checkpoint: checkpoint_key(task(), 1, OperationId::from_bytes([9; 16])).unwrap(),
        selected_at: 0,
        through: None,
    };
    let head = volume.head().await.unwrap();
    volume
        .append(
            head,
            &FileValue::new(
                commands::compact_key(task()).unwrap(),
                serde_json::to_vec(&initial).unwrap(),
            ),
        )
        .await
        .unwrap();
    let (cut, _, identity) = capture_selection(volume, task(), MAXIMUM).await.unwrap();
    let mut next =
        crate::context::summary_source::checkpoint_fixture::checkpoint(cut.cursor).unwrap();
    next.boundary.previous = identity;
    next
}

async fn selected<P: StreamProvider>(volume: &TaskVolume<P>) -> Option<FileKey> {
    let (_, current, _) = capture_selection(volume, task(), MAXIMUM).await.unwrap();
    current.map(|current| current.checkpoint)
}

async fn recover(stream: Arc<LocalStream>, next: SummaryCheckpoint) -> Recovered {
    let volume = TaskVolume::open(stream, path()).await.unwrap();
    let expected = key(&next);
    let retained = volume
        .snapshot(&Query::File(expected.clone()), 1)
        .await
        .unwrap()
        .files
        .first()
        .is_some();
    let selected_before = selected(&volume).await.as_ref() == Some(&expected);
    let recovery = publish(&volume, operation(), task(), &next, MAXIMUM)
        .await
        .map_err(|error| error.to_string());
    let selected_after = selected(&volume).await;
    let file_matches = volume
        .snapshot(&Query::File(expected.clone()), 1)
        .await
        .unwrap()
        .files
        .first()
        .is_some_and(|file| file.bytes.as_ref() == serde_json::to_vec(&next).unwrap());
    // A stale replay names no predecessor and an older observation.
    let mut stale = next.clone();
    stale.boundary.previous = None;
    stale.boundary.through.position -= 1;
    stale.boundary.captured_at = 0;
    let stale_selected = publish(&volume, operation(), task(), &stale, MAXIMUM)
        .await
        .ok()
        .map(|(_, selected)| selected);
    let stale_kept = selected(&volume).await == selected_after;
    let before = volume.head().await.unwrap();
    let replay = publish(&volume, operation(), task(), &next, MAXIMUM).await;
    let after = volume.head().await.unwrap();
    Recovered {
        retained,
        selected_before,
        recovery,
        selected_after,
        file_matches,
        stale_selected,
        stale_kept,
        replay_stable: replay.is_ok_and(|(key, selected)| key == expected && selected)
            && before == after,
    }
}

/// Independent oracle: one replay completes publication from any cut.
fn violation(expected: &FileKey, observed: &Recovered) -> Option<&'static str> {
    match &observed.recovery {
        Err(_) => return Some("recovery failed"),
        Ok((key, selected)) if key != expected || !selected => {
            return Some("recovery did not report its own selection");
        }
        Ok(_) => {}
    }
    if observed.selected_after.as_ref() != Some(expected) {
        return Some("recovery left another checkpoint selected");
    }
    if !observed.file_matches {
        return Some("retained checkpoint differs from the published bytes");
    }
    if observed.stale_selected != Some(false) || !observed.stale_kept {
        return Some("a stale replay replaced the recovered selection");
    }
    if !observed.replay_stable {
        return Some("a second replay wrote again");
    }
    None
}

async fn cuts() -> (FileKey, Vec<(Option<Cut>, Recovered)>) {
    let next = RefCell::new(None);
    let found = enumerate(
        |stream| {
            let next = &next;
            async move {
                let volume = TaskVolume::open(stream, path()).await.unwrap();
                *next.borrow_mut() = Some(prepare(&volume).await);
            }
        },
        |stream| {
            let checkpoint = next.borrow().clone().unwrap();
            async move {
                if let Ok(volume) = TaskVolume::open(stream, path()).await {
                    let _ = publish(&volume, operation(), task(), &checkpoint, MAXIMUM).await;
                }
            }
        },
        |stream| recover(stream, next.borrow().clone().unwrap()),
    )
    .await
    .expect("every crash is simulated faithfully");
    // The checkpoint file, then its selection.
    assert_eq!(found.writes, 2, "publication durable boundaries changed");
    let expected = key(next.borrow().as_ref().unwrap());
    let cuts = std::iter::once((None, found.clean))
        .chain(found.cuts.into_iter().map(|(cut, seen)| (Some(cut), seen)))
        .collect();
    (expected, cuts)
}

#[tokio::test]
async fn every_durable_cut_replays_to_one_selected_checkpoint() {
    let (expected, cuts) = cuts().await;
    for (cut, observed) in &cuts {
        assert_eq!(
            violation(&expected, observed),
            None,
            "{cut:?}: {observed:?}"
        );
    }
    let states: std::collections::BTreeSet<_> = cuts
        .iter()
        .map(|(_, seen)| (seen.retained, seen.selected_before))
        .collect();
    for state in [(false, false), (true, false), (true, true)] {
        assert!(
            states.contains(&state),
            "no cut reached {state:?}: {states:?}"
        );
    }
}

#[tokio::test]
async fn negative_control_treating_a_retained_file_as_selected_is_detected() {
    RETAINED_MEANS_SELECTED.with(|flag| flag.set(true));
    let (expected, cuts) = cuts().await;
    RETAINED_MEANS_SELECTED.with(|flag| flag.set(false));
    let caught: Vec<_> = cuts
        .iter()
        .filter_map(|(cut, seen)| violation(&expected, seen).map(|why| (cut, why, seen)))
        .collect();
    assert!(!caught.is_empty(), "skipped selection escaped every cut");
    for (cut, why, seen) in caught {
        assert!(
            seen.retained && !seen.selected_before,
            "{cut:?} failed outside the split-write window: {why}"
        );
    }
}
