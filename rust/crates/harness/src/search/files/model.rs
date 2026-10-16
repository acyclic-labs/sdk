use super::*;
use acyclic_stream::{AppendOutcome, AppendRequest, BoxProviderFuture, MemoryStream, StreamPath};
use loom::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
};

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    loom::future::block_on(Box::pin(future))
}

fn model(run: impl Fn() + Send + Sync + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(run);
}

struct Provider {
    calls: AtomicUsize,
    release: crate::sync::Mutex<Option<mpsc::Receiver<()>>>,
}

impl Provider {
    fn new(release: Option<mpsc::Receiver<()>>) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            release: crate::sync::Mutex::new(release),
        }
    }
}

impl SearchProvider for Provider {
    fn search<'a>(&'a self, _: &'a SearchRequest) -> BoxProviderFuture<'a, Result<SearchResponse>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let release = self.release.lock().await.take();
            if let Some(release) = release {
                // A complete response arrives only after the owner was replaced.
                release.recv().unwrap();
            }
            Ok(SearchResponse { results: vec![] })
        })
    }
}

fn fence(id: &str) -> LeaseFence {
    LeaseFence {
        reservation_id: id.into(),
        placement: "model".into(),
    }
}

fn condition(owner: u64) -> Vec<CommitCondition> {
    vec![CommitCondition::Tail {
        path: StreamPath::new("owner").unwrap(),
        expected: owner,
    }]
}

fn append_owner(stream: &MemoryStream, expected: u64) {
    assert!(matches!(
        block_on(stream.append(AppendRequest {
            path: StreamPath::new("owner").unwrap(),
            records: vec![OwnedBytes::from_static(b"owner")],
            if_tail: Some(expected),
            idempotency_key: None,
        }))
        .unwrap(),
        AppendOutcome::Committed(_)
    ));
}

fn initialized() -> (Arc<MemoryStream>, Arc<TaskVolume<MemoryStream>>) {
    let stream = Arc::new(MemoryStream::default());
    append_owner(&stream, 0);
    let volume = Arc::new(
        block_on(TaskVolume::open(
            stream.clone(),
            StreamPath::new("search-model").unwrap(),
        ))
        .unwrap(),
    );
    let generation = block_on(volume.snapshot(&Query::Root, 0))
        .unwrap()
        .generation;
    block_on(volume.append(
        generation,
        &FileValue::new(
            FileKey::parse(".tasks/model/requests/search-one.json").unwrap(),
            br#"{"query":"model"}"#.as_slice(),
        ),
    ))
    .unwrap();
    (stream, volume)
}

fn run_worker(
    stream: &MemoryStream,
    volume: &TaskVolume<MemoryStream>,
    provider: Arc<Provider>,
    owner: u64,
    broken: bool,
) -> Result<()> {
    block_on(settle(
        volume,
        &Component::new("model").unwrap(),
        &Component::new("search-one").unwrap(),
        &fence(if owner == 1 { "old" } else { "new" }),
        &mut || Ok(Some(provider.clone())),
        &|settlement| async move {
            if broken && settlement {
                return Ok(vec![]);
            }
            let tail = stream
                .watermarks(StreamPath::new("owner").unwrap())
                .await
                .unwrap()
                .accepted;
            if tail != owner {
                return Err(storage(crate::task_volume::Error::FenceConflict));
            }
            Ok(condition(tail))
        },
        None,
    ))
}

fn outcome(volume: &TaskVolume<MemoryStream>) -> SearchOutcome {
    let snapshot =
        block_on(volume.snapshot(&Query::Task(Component::new("model").unwrap()), 4)).unwrap();
    assert_eq!(snapshot.files.len(), 3, "unfinished search lifecycle");
    let result = snapshot
        .files
        .iter()
        .find(|value| {
            matches!(
                value.key,
                FileKey::Task {
                    file: TaskFile::RequestResult(_),
                    ..
                }
            )
        })
        .unwrap();
    serde_json::from_slice(&result.bytes).unwrap()
}

#[test]
fn competing_claims_dispatch_once_and_replay_the_terminal_file() {
    model(|| {
        let (stream, volume) = initialized();
        let provider = Arc::new(Provider::new(None));
        let worker = thread::spawn({
            let stream = stream.clone();
            let volume = volume.clone();
            let provider = provider.clone();
            move || run_worker(&stream, &volume, provider, 1, false)
        });
        let result = run_worker(&stream, &volume, provider.clone(), 1, false);
        for result in [result, worker.join().unwrap()] {
            assert!(result.is_ok() || matches!(result, Err(Error::SearchPublicationDeferred)));
        }
        assert_eq!(
            provider.calls.load(Ordering::SeqCst),
            1,
            "duplicate search dispatch"
        );
        assert!(matches!(outcome(&volume), SearchOutcome::Succeeded { .. }));
        run_worker(&stream, &volume, provider.clone(), 1, false).unwrap();
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    });
}

fn replacement(broken: bool) {
    model(move || {
        let (stream, volume) = initialized();
        let (release, waiting) = mpsc::channel();
        let old = Arc::new(Provider::new(Some(waiting)));
        let new = Arc::new(Provider::new(None));
        let worker = thread::spawn({
            let stream = stream.clone();
            let volume = volume.clone();
            let old = old.clone();
            move || run_worker(&stream, &volume, old, 1, broken)
        });
        append_owner(&stream, 1);
        let _ = release.send(());
        run_worker(&stream, &volume, new.clone(), 2, false).unwrap();
        let result = worker.join().unwrap();
        assert!(
            result.is_ok()
                || matches!(
                    result,
                    Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Storage,
                        ..
                    })
                ),
            "{result:?}"
        );
        assert!(
            old.calls.load(Ordering::SeqCst) + new.calls.load(Ordering::SeqCst) <= 1,
            "replacement redispatched a claimed search"
        );
        let terminal = outcome(&volume);
        assert_eq!(
            matches!(terminal, SearchOutcome::Succeeded { .. }),
            new.calls.load(Ordering::SeqCst) == 1,
            "stale search result escaped replacement fence"
        );
        run_worker(&stream, &volume, new.clone(), 2, false).unwrap();
        assert_eq!(outcome(&volume), terminal);
    });
}

#[test]
fn replacement_fences_claim_and_result_without_redispatch() {
    replacement(false);
}

#[test]
#[should_panic(expected = "stale search result escaped replacement fence")]
fn negative_control_detects_missing_result_fence() {
    replacement(true);
}

fn shared_tail(broken: bool) {
    model(move || {
        let (stream, volume) = initialized();
        let provider = Arc::new(Provider::new(None));
        let (authorized, advance) = mpsc::channel();
        let (advanced, released) = mpsc::channel();
        let other_task = thread::spawn({
            let stream = stream.clone();
            move || {
                advance.recv().unwrap();
                append_owner(&stream, 1);
                advanced.send(()).unwrap();
            }
        });
        let first = std::cell::Cell::new(true);
        let authorize = |settlement| {
            let (stream, first, authorized, released) = (&stream, &first, &authorized, &released);
            async move {
                if broken && settlement && !first.get() {
                    return Err(storage(crate::task_volume::Error::FenceConflict));
                }
                let tail = stream
                    .watermarks(StreamPath::new("owner").unwrap())
                    .await
                    .map_err(|_| Error::Storage("model owner missing".into()))?
                    .accepted;
                if settlement && first.replace(false) {
                    authorized.send(()).unwrap();
                    released.recv().unwrap();
                }
                Ok(condition(tail))
            }
        };
        let result = block_on(settle(
            &volume,
            &Component::new("model").unwrap(),
            &Component::new("search-one").unwrap(),
            &fence("old"),
            &mut || Ok(Some(provider.clone() as Arc<dyn SearchProvider>)),
            &authorize,
            None,
        ));
        other_task.join().unwrap();
        assert!(
            result.is_ok(),
            "completed search response was lost: {result:?}"
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert!(matches!(outcome(&volume), SearchOutcome::Succeeded { .. }));
    });
}

#[test]
fn shared_tail_advance_reauthorizes_the_completed_response() {
    shared_tail(false);
}

#[test]
#[should_panic(expected = "completed search response was lost")]
fn negative_control_detects_discarding_a_completed_response() {
    shared_tail(true);
}

fn saturated_tail(broken: bool) {
    model(move || {
        let (stream, volume) = initialized();
        let provider = Arc::new(Provider::new(None));
        let (authorized, advance) = mpsc::channel();
        let (advanced, released) = mpsc::channel();
        let competitor = thread::spawn({
            let stream = stream.clone();
            move || {
                while advance.recv().unwrap() {
                    let tail = block_on(stream.watermarks(StreamPath::new("owner").unwrap()))
                        .unwrap()
                        .accepted;
                    append_owner(&stream, tail);
                    advanced.send(()).unwrap();
                }
            }
        });
        let attempts = std::cell::Cell::new(0);
        let authorize = |settlement| {
            let state = (&stream, &attempts, &authorized, &released);
            async move {
                let (stream, attempts, authorized, released) = state;
                let tail = stream
                    .watermarks(StreamPath::new("owner").unwrap())
                    .await
                    .unwrap()
                    .accepted;
                if settlement {
                    let next = attempts.get() + 1;
                    attempts.set(next);
                    if next > PUBLICATION_ATTEMPTS {
                        return Err(Error::Storage(
                            "model detected an unbounded publisher".into(),
                        ));
                    }
                    authorized.send(true).unwrap();
                    released.recv().unwrap();
                }
                Ok(condition(tail))
            }
        };
        let task = Component::new("model").unwrap();
        let id = Component::new("search-one").unwrap();
        let mut result = Ok(());
        for _ in 0..if broken { 2 } else { 1 } {
            result = block_on(settle(
                &volume,
                &task,
                &id,
                &fence("old"),
                &mut || Ok(Some(provider.clone() as Arc<dyn SearchProvider>)),
                &authorize,
                None,
            ));
            if !matches!(result, Err(Error::SearchPublicationDeferred)) {
                break;
            }
        }
        authorized.send(false).unwrap();
        competitor.join().unwrap();
        assert!(
            matches!(result, Err(Error::SearchPublicationDeferred)),
            "publication attempt bound was ignored: {result:?}"
        );
        assert_eq!(attempts.get(), PUBLICATION_ATTEMPTS);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        let completion = block_on(volume.file_volume(&ready(&task, &id))).unwrap();
        let saved = block_on(completion.snapshot(&Query::File(ready(&task, &id)), 1)).unwrap();
        assert_eq!(saved.files.len(), 1);
        assert!(
            block_on(volume.snapshot(&Query::File(file(&task, &id, true)), 1))
                .unwrap()
                .files
                .is_empty()
        );
        let resume = |_| async {
            let tail = stream
                .watermarks(StreamPath::new("owner").unwrap())
                .await
                .unwrap()
                .accepted;
            Ok(condition(tail))
        };
        block_on(settle(
            &volume,
            &task,
            &id,
            &fence("old"),
            &mut || panic!("resume redispatched a completed response"),
            &resume,
            None,
        ))
        .unwrap();
        assert!(matches!(outcome(&volume), SearchOutcome::Succeeded { .. }));
    });
}

#[test]
fn every_conflict_is_bounded_and_resumes_from_the_ready_file() {
    saturated_tail(false);
}

#[test]
#[should_panic(expected = "publication attempt bound was ignored")]
fn negative_control_detects_ignoring_the_attempt_bound() {
    saturated_tail(true);
}

#[test]
fn parent_writes_cannot_starve_completion_staging() {
    model(|| {
        let provider = Arc::new(Provider::new(None));
        block_on(super::staging_tests::schedule(provider.clone(), false)).unwrap();
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    });
}

#[test]
#[should_panic(expected = "completed response was not retained")]
fn negative_control_detects_shared_staging_generation() {
    model(|| {
        block_on(super::staging_tests::schedule(
            Arc::new(Provider::new(None)),
            true,
        ))
        .unwrap();
    });
}

#[test]
fn failed_completion_open_leaves_request_retryable() {
    model(|| {
        let provider = Arc::new(Provider::new(None));
        block_on(super::staging_tests::failed_open_retry(
            provider.clone(),
            false,
        ))
        .unwrap();
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    });
}

#[test]
#[should_panic(expected = "failed open committed a claim")]
fn negative_control_detects_claim_before_open() {
    model(|| {
        block_on(super::staging_tests::failed_open_retry(
            Arc::new(Provider::new(None)),
            true,
        ))
        .unwrap();
    });
}
