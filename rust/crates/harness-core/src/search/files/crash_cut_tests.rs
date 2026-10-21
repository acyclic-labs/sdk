//! Crash after every durable write of one search settlement, reopen the
//! directory with a replacement owner, and check recovery against the disk
//! state found at reopen, never against the candidate's own result.
use super::*;
use crate::test_support::{
    ScriptedSearch, ScriptedSearchProvider,
    crash_cuts::{Cut, enumerate},
};
use acyclic_stream::{LocalStream, StreamPath};
use std::cell::Cell;

#[derive(Debug)]
struct Recovered {
    claim: bool,
    ready: Option<Vec<u8>>,
    result_before: bool,
    dispatched: usize,
    recovery_dispatched: usize,
    recovery: std::result::Result<(), String>,
    result: Option<Vec<u8>>,
    replay_stable: bool,
}

fn path() -> StreamPath {
    StreamPath::new("search-crash").unwrap()
}
fn task() -> Component {
    Component::new("task").unwrap()
}
fn id() -> Component {
    Component::new("search-crash").unwrap()
}
fn fence(reservation: &str) -> LeaseFence {
    LeaseFence {
        reservation_id: reservation.into(),
        placement: "crash-cut".into(),
    }
}
fn response() -> SearchResponse {
    SearchResponse { results: vec![] }
}
fn provider() -> Arc<ScriptedSearchProvider> {
    Arc::new(
        ScriptedSearchProvider::new([ScriptedSearch {
            request: SearchRequest {
                query: "crash".into(),
                max_results: 5,
            },
            response: Some(response()),
            error: None,
        }])
        .unwrap(),
    )
}

async fn run_settle<P: StreamProvider>(
    volume: &TaskVolume<P>,
    reservation: &str,
    provider: &Arc<ScriptedSearchProvider>,
) -> Result<()> {
    settle(
        volume,
        &task(),
        &id(),
        &fence(reservation),
        &mut || Ok(Some(provider.clone() as Arc<dyn SearchProvider>)),
        &|_| std::future::ready(Ok(vec![])),
        None,
    )
    .await
}

async fn exists<P: StreamProvider>(volume: &TaskVolume<P>, key: FileKey) -> Option<Vec<u8>> {
    let found = volume.snapshot(&Query::File(key.clone()), 1).await.unwrap();
    if let Some(file) = found.files.first() {
        return Some(file.bytes.to_vec());
    }
    None
}

/// Observes the receipt without creating its completion volume: a missing
/// volume holds no receipt, and recovery must find it missing too.
async fn ready_bytes(
    stream: &Arc<LocalStream>,
    volume: &TaskVolume<LocalStream>,
) -> Option<Vec<u8>> {
    if let Some(bytes) = exists(volume, ready(&task(), &id())).await {
        return Some(bytes);
    }
    let path = volume
        .file_volume_path(&ready(&task(), &id()))
        .await
        .unwrap();
    match stream.watermarks(path.clone()).await {
        Err(acyclic_stream::StreamError::NotFound) => return None,
        Ok(watermarks) if watermarks.accepted == 0 => return None,
        found => {
            found.unwrap();
        }
    }
    let completion = TaskVolume::open_existing(stream.clone(), path)
        .await
        .unwrap();
    exists(&completion, ready(&task(), &id())).await
}

async fn recover(stream: Arc<LocalStream>, dispatched: usize) -> Recovered {
    let volume = TaskVolume::open(stream.clone(), path()).await.unwrap();
    let claim = exists(&volume, file(&task(), &id(), false)).await.is_some();
    let ready = ready_bytes(&stream, &volume).await;
    let result_before = exists(&volume, file(&task(), &id(), true)).await.is_some();
    let replacement = provider();
    let recovery = run_settle(&volume, "replacement", &replacement)
        .await
        .map_err(|error| error.to_string());
    let recovery_dispatched = 1 - replacement.remaining();
    let result = exists(&volume, file(&task(), &id(), true)).await;
    let before = volume.snapshot(&Query::Task(task()), 8).await.unwrap();
    let again = provider();
    let replay = run_settle(&volume, "replacement", &again).await;
    let after = volume.snapshot(&Query::Task(task()), 8).await.unwrap();
    Recovered {
        claim,
        ready,
        result_before,
        dispatched,
        recovery_dispatched,
        recovery,
        result,
        replay_stable: replay.is_ok()
            && again.remaining() == 1
            && before.generation == after.generation
            && before.files == after.files,
    }
}

/// Independent oracle: the outcome follows from the durable files at reopen.
fn violation(observed: &Recovered) -> Option<&'static str> {
    let Some(result) = &observed.result else {
        return Some("recovery published no result");
    };
    let Ok(outcome) = serde_json::from_slice::<SearchOutcome>(result) else {
        return Some("result is not a search outcome");
    };
    let expected = if observed.ready.is_some() || !observed.claim {
        SearchOutcome::Succeeded {
            response: response(),
        }
    } else {
        SearchOutcome::Indeterminate
    };
    if observed.recovery.is_err() {
        return Some("recovery failed");
    }
    if observed.ready.as_ref().is_some_and(|ready| ready != result) {
        return Some("result differs from the retained response receipt");
    }
    if outcome != expected {
        return Some("outcome contradicts the durable files at reopen");
    }
    if observed.claim && observed.recovery_dispatched != 0 {
        return Some("recovery redispatched a claimed request");
    }
    let total = observed.dispatched + observed.recovery_dispatched;
    if total > 1 || (matches!(outcome, SearchOutcome::Succeeded { .. }) && total != 1) {
        return Some("provider dispatch count is not exactly once");
    }
    if !observed.replay_stable {
        return Some("a second recovery changed files or dispatched");
    }
    None
}

async fn cuts() -> Vec<(Option<Cut>, Recovered)> {
    let dispatched = Cell::new(0);
    let found = enumerate(
        |stream| async move {
            let volume = TaskVolume::open(stream, path()).await.unwrap();
            let snapshot = volume.snapshot(&Query::Root, 0).await.unwrap();
            let request = FileKey::Task {
                task: task(),
                file: TaskFile::Request(id()),
            };
            volume
                .append(
                    snapshot.generation,
                    &FileValue::new(request, br#"{"query":"crash"}"#.as_slice()),
                )
                .await
                .unwrap();
        },
        |stream| {
            let dispatched = &dispatched;
            async move {
                let original = provider();
                if let Ok(volume) = TaskVolume::open(stream, path()).await {
                    let _ = run_settle(&volume, "original", &original).await;
                }
                dispatched.set(1 - original.remaining());
            }
        },
        |stream| recover(stream, dispatched.get()),
    )
    .await
    .expect("every crash is simulated faithfully");
    // Completion volume, claim, retained response, result.
    assert_eq!(found.writes, 4, "settlement durable boundaries changed");
    std::iter::once((None, found.clean))
        .chain(found.cuts.into_iter().map(|(cut, seen)| (Some(cut), seen)))
        .collect()
}

#[tokio::test]
async fn every_durable_cut_recovers_the_retained_receipt_exactly_once() {
    let cuts = cuts().await;
    for (cut, observed) in &cuts {
        assert_eq!(violation(observed), None, "{cut:?}: {observed:?}");
    }
    // The enumeration reached every distinct durable state of the operation.
    let states: std::collections::BTreeSet<_> = cuts
        .iter()
        .map(|(_, seen)| (seen.claim, seen.ready.is_some(), seen.result_before))
        .collect();
    for state in [
        (false, false, false),
        (true, false, false),
        (true, true, false),
        (true, true, true),
    ] {
        assert!(
            states.contains(&state),
            "no cut reached {state:?}: {states:?}"
        );
    }
}

#[tokio::test]
async fn negative_control_judging_the_fence_before_the_receipt_is_detected() {
    FENCE_BEFORE_RECEIPT.with(|flag| flag.set(true));
    let cuts = cuts().await;
    FENCE_BEFORE_RECEIPT.with(|flag| flag.set(false));
    let caught: Vec<_> = cuts
        .iter()
        .filter_map(|(cut, seen)| violation(seen).map(|why| (cut, why, seen)))
        .collect();
    assert!(!caught.is_empty(), "fence-first recovery escaped every cut");
    for (cut, why, seen) in caught {
        assert!(
            seen.claim && seen.ready.is_some() && !seen.result_before,
            "{cut:?} failed outside the receipt window: {why}"
        );
    }
}
