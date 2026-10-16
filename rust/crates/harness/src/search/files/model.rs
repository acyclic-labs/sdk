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
    builder.max_permutations = Some(128);
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
        &|settlement| {
            std::future::ready(Ok(if broken && settlement {
                vec![]
            } else {
                condition(owner)
            }))
        },
        None,
    ))
}

fn outcome(volume: &TaskVolume<MemoryStream>) -> SearchOutcome {
    let snapshot =
        block_on(volume.snapshot(&Query::Task(Component::new("model").unwrap()), 3)).unwrap();
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
        let (_, volume) = initialized();
        let provider = Arc::new(Provider::new(None));
        let worker = thread::spawn({
            let volume = volume.clone();
            let provider = provider.clone();
            move || run_worker(&volume, provider, 1, false).unwrap()
        });
        run_worker(&volume, provider.clone(), 1, false).unwrap();
        worker.join().unwrap();
        assert_eq!(
            provider.calls.load(Ordering::SeqCst),
            1,
            "duplicate search dispatch"
        );
        assert!(matches!(outcome(&volume), SearchOutcome::Succeeded { .. }));
        run_worker(&volume, provider.clone(), 1, false).unwrap();
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
            let volume = volume.clone();
            let old = old.clone();
            move || run_worker(&volume, old, 1, broken)
        });
        append_owner(&stream, 1);
        let _ = release.send(());
        run_worker(&volume, new.clone(), 2, false).unwrap();
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
        run_worker(&volume, new.clone(), 2, false).unwrap();
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
