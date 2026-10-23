use super::*;
use acyclic_stream::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenRequest, CommitId, CommitOutcome,
    CommitRequest, CommittedEnvelope, ForkReceipt, ForkRequest, IdempotencyKey,
    IdempotencyObservation, RecordStream, StreamBounds, StreamError, StreamWatermarks,
};
use std::sync::{Mutex, RwLock};

type Storage<T> = std::result::Result<T, StreamError>;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Op {
    Watermarks,
    Read,
    Append,
    Follow,
    Commit,
}

impl Op {
    fn index(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum When {
    Before,
    After,
    Item,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Loss {
    Missing,
    Truncated,
    Reused,
    NotFound,
    OutOfRange,
    PrefixNotRetained,
    Eof,
    Unavailable,
}

#[derive(Clone, Copy, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Entry {
    Snapshot,
    Heads,
    SnapshotAt,
    HeadsAt,
    Follow,
    Live,
    Append,
    AppendIf,
    RequestWrite,
    Open,
    Initialize,
    Observe,
    ObserveInitial,
    ObserveReset,
}

#[derive(Clone, Deserialize)]
struct Case {
    name: String,
    entry: Entry,
    op: Op,
    call: usize,
    when: When,
    loss: Loss,
    expected: String,
    #[serde(default)]
    item: usize,
    #[serde(default)]
    suffix: bool,
    #[serde(default)]
    header_cut: bool,
    #[serde(default)]
    accepted_ahead: bool,
    #[serde(default)]
    history: usize,
    #[serde(default)]
    delivered: usize,
    #[serde(default)]
    contend: bool,
}

#[derive(Clone)]
struct FaultStream {
    operation_claims: std::sync::Arc<acyclic_stream::OperationClaims>,
    // Fixture lock order: fault -> current; calls, accepted and release are never nested.
    current: Arc<RwLock<Arc<MemoryStream>>>,
    replacement: Arc<MemoryStream>,
    fault: Arc<Mutex<Option<Case>>>,
    calls: Arc<Mutex<[usize; 5]>>,
    contend: Arc<std::sync::atomic::AtomicBool>,
    accepted: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    release: Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
}

impl FaultStream {
    fn source(&self) -> Storage<Arc<MemoryStream>> {
        self.current
            .read()
            .map(|source| source.clone())
            .map_err(|_| StreamError::Unavailable)
    }

    fn call(&self, op: Op) -> Storage<usize> {
        let mut calls = self.calls.lock().map_err(|_| StreamError::Unavailable)?;
        let count = calls
            .get_mut(op.index())
            .ok_or(StreamError::InvalidArgument)?;
        *count += 1;
        Ok(*count)
    }

    fn trip(&self, op: Op, call: usize, when: When, item: usize) -> Storage<bool> {
        let mut fault = self.fault.lock().map_err(|_| StreamError::Unavailable)?;
        if !fault.as_ref().is_some_and(|f| {
            f.op == op && f.call == call && f.when == when && (when != When::Item || f.item == item)
        }) {
            return Ok(false);
        }
        let fault = fault.take().ok_or(StreamError::Unavailable)?;
        if fault.loss == Loss::Unavailable {
            return Err(StreamError::Unavailable);
        }
        *self.current.write().map_err(|_| StreamError::Unavailable)? = self.replacement.clone();
        match fault.loss {
            Loss::NotFound => Err(StreamError::NotFound),
            Loss::OutOfRange => Err(StreamError::OutOfRange),
            Loss::PrefixNotRetained => Err(StreamError::PrefixNotRetained),
            Loss::Eof => Ok(true),
            _ => Ok(false),
        }
    }

    fn items(&self, records: RecordStream, op: Op, call: usize) -> RecordStream {
        let provider = self.clone();
        Box::pin(futures::stream::try_unfold(
            (records, provider, 0),
            move |(mut records, provider, item)| async move {
                if provider.trip(op, call, When::Item, item)? {
                    return Ok(None);
                }
                let Some(record) = records.next().await else {
                    return Ok(None);
                };
                Ok(Some((record?, (records, provider, item + 1))))
            },
        ))
    }
}

#[async_trait::async_trait]
impl StreamProvider for FaultStream {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        &self.operation_claims
    }

    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Storage<Option<IdempotencyObservation>> {
        self.source()?.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> Storage<u64> {
        self.source()?.tail(path).await
    }
    async fn bounds(&self, path: StreamPath) -> Storage<StreamBounds> {
        self.source()?.bounds(path).await
    }
    async fn watermarks(&self, path: StreamPath) -> Storage<StreamWatermarks> {
        let n = self.call(Op::Watermarks)?;
        self.trip(Op::Watermarks, n, When::Before, 0)?;
        let result = self.source()?.watermarks(path).await;
        self.trip(Op::Watermarks, n, When::After, 0)?;
        result
    }
    async fn append(&self, request: AppendRequest) -> Storage<AppendOutcome> {
        let n = self.call(Op::Append)?;
        self.trip(Op::Append, n, When::Before, 0)?;
        let result = self.source()?.append(request).await;
        self.trip(Op::Append, n, When::After, 0)?;
        result
    }
    async fn fork(&self, request: ForkRequest) -> Storage<ForkReceipt> {
        self.source()?.fork(request).await
    }
    async fn read(&self, request: ReadRequest) -> Storage<RecordStream> {
        let n = self.call(Op::Read)?;
        self.trip(Op::Read, n, When::Before, 0)?;
        let result = self.source()?.read(request).await;
        self.trip(Op::Read, n, When::After, 0)?;
        result.map(|records| self.items(records, Op::Read, n))
    }
    async fn follow(
        &self,
        path: StreamPath,
        from: u64,
        filter: acyclic_stream::FollowFilter,
    ) -> Storage<RecordStream> {
        self.source()?.follow(path, from, filter).await
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
        filter: acyclic_stream::FollowFilter,
    ) -> Storage<RecordStream> {
        let n = self.call(Op::Follow)?;
        self.trip(Op::Follow, n, When::Before, 0)?;
        let result = self
            .source()?
            .follow_checked(path, from, preceding, filter)
            .await;
        self.trip(Op::Follow, n, When::After, 0)?;
        result.map(|records| self.items(records, Op::Follow, n))
    }
    async fn children(&self, request: ChildrenRequest) -> Storage<ChildStream> {
        self.source()?.children(request).await
    }
    async fn read_commit(&self, id: CommitId) -> Storage<CommittedEnvelope> {
        self.source()?.read_commit(id).await
    }
    async fn commit(&self, request: CommitRequest) -> Storage<CommitOutcome> {
        let n = self.call(Op::Commit)?;
        if self
            .contend
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            let contender = TaskVolume::open(self.source()?, StreamPath::new("task-volume")?)
                .await
                .map_err(|_| StreamError::Unavailable)?;
            let cut = contender
                .snapshot(&Query::Root, usize::MAX)
                .await
                .map_err(|_| StreamError::Unavailable)?;
            contender
                .append(
                    cut.generation,
                    &Write::new(FileKey::Goal, OwnedBytes::from_static(b"contender")),
                )
                .await
                .map_err(|_| StreamError::Unavailable)?;
        }
        self.trip(Op::Commit, n, When::Before, 0)?;
        let result = self.source()?.commit(request).await;
        self.trip(Op::Commit, n, When::After, 0)?;
        let accepted = self
            .accepted
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .take();
        if let Some(accepted) = accepted {
            accepted.send(()).map_err(|_| StreamError::Unavailable)?;
        }
        let release = self.release.lock().await.take();
        if let Some(release) = release {
            release.await.map_err(|_| StreamError::Unavailable)?;
        }
        result
    }
}

async fn exercise(
    volume: &TaskVolume<FaultStream>,
    provider: &Arc<FaultStream>,
    case: &Case,
    cursor: Cursor,
    generation: acyclic_harness::task_volume::Generation,
    seen: &mut Option<Vec<acyclic_harness::task_volume::FileValue>>,
) -> acyclic_harness::task_volume::Result<bool> {
    match case.entry {
        Entry::Snapshot => {
            *seen = Some(volume.snapshot(&Query::Root, usize::MAX).await?.files);
        }
        Entry::Heads => {
            volume.heads(&Query::Root, None, usize::MAX).await?;
        }
        Entry::SnapshotAt => {
            *seen = Some(
                volume
                    .snapshot_at(&Query::Root, cursor, usize::MAX)
                    .await?
                    .files,
            );
        }
        Entry::HeadsAt => {
            volume.heads(&Query::Root, Some(cursor), usize::MAX).await?;
        }
        Entry::Follow => {
            drop(volume.follow(Query::Root, cursor).await?);
        }
        Entry::Live => {
            let mut changes = volume.follow(Query::Root, cursor).await?;
            for _ in 0..case.delivered {
                changes.next().await.ok_or(Error::ResetRequired)??;
            }
            changes.next().await.ok_or(Error::ResetRequired)??;
        }
        Entry::Append => {
            volume
                .append(
                    generation,
                    &Write::new(FileKey::Goal, OwnedBytes::from_static(b"publish")),
                )
                .await?;
        }
        Entry::AppendIf => {
            volume
                .append_if(
                    generation,
                    &Write::new(FileKey::Goal, OwnedBytes::from_static(b"publish")),
                    Vec::new(),
                    None,
                    None,
                )
                .await?;
        }
        Entry::RequestWrite => {
            let initial = volume
                .snapshot(&Query::Requests(Component::new("t")?), 0)
                .await?;
            volume
                .append(
                    initial.generation,
                    &Write::new(
                        FileKey::parse(".tasks/t/requests/r.json")?,
                        OwnedBytes::from_static(b"request"),
                    ),
                )
                .await?;
        }
        Entry::Open | Entry::Initialize => {
            TaskVolume::open(provider.clone(), StreamPath::new("task-volume")?).await?;
        }
        Entry::Observe | Entry::ObserveInitial | Entry::ObserveReset => {
            let mut cursor = cursor;
            if case.entry == Entry::ObserveReset {
                cursor.query[0] ^= 1;
            }
            let cursor = (case.entry != Entry::ObserveInitial).then_some(cursor);
            let observation = volume.observe(Query::Root, cursor, usize::MAX).await?;
            *seen = observation.snapshot.map(|snapshot| snapshot.files);
            return Ok(observation.reset);
        }
    }
    Ok(false)
}

async fn replacement(original: &MemoryStream, loss: Loss) -> Result<Arc<MemoryStream>> {
    let mut records = original
        .read(ReadRequest {
            path: StreamPath::new("task-volume")?,
            from: 0,
            limit: 1,
        })
        .await?;
    let header = records.next().await.ok_or("missing header")??.value;
    let replacement = Arc::new(MemoryStream::default());
    if matches!(loss, Loss::Truncated | Loss::Reused) {
        replacement
            .append(AppendRequest {
                path: StreamPath::new("task-volume")?,
                records: vec![header.into()],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
        if loss == Loss::Reused {
            let volume =
                TaskVolume::open(replacement.clone(), StreamPath::new("task-volume")?).await?;
            let cut = volume.snapshot(&Query::Root, 0).await?;
            volume
                .append(cut.generation, &write("GOAL.md", "replacement")?)
                .await?;
        }
    }
    Ok(replacement)
}

fn check_result(case: &Case, result: std::result::Result<bool, Error>) -> Result {
    match (case.expected.as_str(), result) {
        ("reset", Err(Error::ResetRequired))
        | ("recover", Ok(true))
        | ("cut", Ok(false))
        | ("conflict", Err(Error::Conflict(_)))
        | ("unavailable", Err(Error::Storage(StreamError::Unavailable))) => Ok(()),
        (_, result) => {
            Err(format!("{}: expected {}, got {result:?}", case.name, case.expected).into())
        }
    }
}

async fn run(case: Case) -> Result {
    let (original, original_volume) = memory().await?;
    let initial = original_volume.snapshot(&Query::Root, 0).await?;
    let mut generation = original_volume
        .append(initial.generation, &write("GOAL.md", "lost")?)
        .await?;
    for n in 0..case.history {
        generation = original_volume
            .append(
                generation,
                &write(&format!(".tasks/t/notifications/n{n}.json"), "old")?,
            )
            .await?;
    }
    let replacement = replacement(&original, case.loss).await?;
    let provider = Arc::new(FaultStream {
        operation_claims: Default::default(),
        current: Arc::new(RwLock::new(original.clone())),
        replacement,
        fault: Default::default(),
        calls: Default::default(),
        contend: Arc::new(std::sync::atomic::AtomicBool::new(case.contend)),
        accepted: Default::default(),
        release: Default::default(),
    });
    let volume = TaskVolume::open(provider.clone(), StreamPath::new("task-volume")?).await?;
    let snapshot = volume.snapshot(&Query::Root, usize::MAX).await?;
    if case.suffix {
        original_volume
            .append(generation, &write(".tasks/t/status.json", "extra")?)
            .await?;
    }
    let expected_cut = if case.header_cut {
        Vec::new()
    } else if matches!(case.entry, Entry::SnapshotAt | Entry::HeadsAt) {
        snapshot.files.clone()
    } else {
        original_volume
            .snapshot(&Query::Root, usize::MAX)
            .await?
            .files
    };
    if case.entry == Entry::Initialize {
        *provider.current.write().map_err(|_| "provider lock")? = Arc::new(MemoryStream::default());
    }
    *provider.calls.lock().map_err(|_| "calls lock")? = [0; 5];
    *provider.fault.lock().map_err(|_| "fault lock")? = Some(case.clone());
    let mut cursor = if case.header_cut {
        initial.cursor
    } else {
        snapshot.cursor
    };
    if case.accepted_ahead {
        cursor.accepted = snapshot.generation.position;
    }
    let mut seen = None;
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        exercise(
            &volume,
            &provider,
            &case,
            cursor,
            snapshot.generation,
            &mut seen,
        ),
    )
    .await;
    assert!(result.is_ok(), "{} waited for a lost anchor", case.name);
    check_result(&case, result?)?;
    assert!(
        provider.fault.lock().map_err(|_| "fault lock")?.is_none(),
        "{} never reached its boundary",
        case.name
    );
    let fresh = TaskVolume::open(provider.source()?, StreamPath::new("task-volume")?).await?;
    let expected_current = fresh.snapshot(&Query::Root, usize::MAX).await?.files;
    if let Some(files) = seen {
        assert_eq!(
            files,
            if case.expected == "recover" {
                expected_current.clone()
            } else {
                expected_cut
            },
            "{} returned a mixed cut",
            case.name
        );
    }
    // A completed pre-loss cut is valid, but no obsolete projection may survive
    // the next observation through the same object.
    let observation = volume.observe(Query::Root, None, usize::MAX).await?;
    let actual = observation.snapshot.ok_or("missing current snapshot")?;
    assert_eq!(
        actual.files, expected_current,
        "{} retained lost files",
        case.name
    );
    Ok(())
}

#[tokio::test]
async fn every_history_loss_boundary_resets_without_hanging_or_retaining_stale_files() -> Result {
    let cases: Vec<Case> = serde_json::from_str(include_str!("history_loss/cases.json"))?;
    for result in futures::future::join_all(cases.into_iter().map(run)).await {
        result?;
    }
    Ok(())
}

#[test]
fn history_errors_have_one_classification_at_every_entry_point() {
    for error in StreamError::ALL {
        let converted = Error::from(error.clone());
        if matches!(
            error,
            StreamError::NotFound | StreamError::OutOfRange | StreamError::PrefixNotRetained
        ) {
            assert!(matches!(converted, Error::ResetRequired));
        } else {
            assert!(matches!(converted, Error::Storage(actual) if actual == error));
        }
    }
}

#[tokio::test]
async fn obsolete_ack_cannot_corrupt_a_rebuilt_generation_at_the_same_position() -> Result {
    let (original, original_volume) = memory().await?;
    let initial = original_volume.snapshot(&Query::Root, 0).await?;
    original_volume
        .append(initial.generation, &write("GOAL.md", "old")?)
        .await?;
    let replacement = Arc::new(MemoryStream::default());
    let replacement_volume =
        TaskVolume::open(replacement.clone(), StreamPath::new("task-volume")?).await?;
    let initial = replacement_volume.snapshot(&Query::Root, 0).await?;
    replacement_volume
        .append(initial.generation, &write("GOAL.md", "replacement")?)
        .await?;
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let cases: Vec<Case> = serde_json::from_str(include_str!("history_loss/cases.json"))?;
    let case = cases
        .into_iter()
        .find(|case| case.name == "append-after-accept-reused-position")
        .ok_or("missing ack case")?;
    let provider = Arc::new(FaultStream {
        operation_claims: Default::default(),
        current: Arc::new(RwLock::new(original)),
        replacement,
        fault: Arc::new(Mutex::new(Some(case))),
        calls: Default::default(),
        contend: Default::default(),
        accepted: Arc::new(Mutex::new(Some(accepted_tx))),
        release: Arc::new(tokio::sync::Mutex::new(Some(release_rx))),
    });
    let volume = Arc::new(TaskVolume::open(provider, StreamPath::new("task-volume")?).await?);
    let obsolete = volume.snapshot(&Query::Root, 1).await?;
    let pending = tokio::spawn({
        let volume = volume.clone();
        async move {
            volume
                .append(
                    obsolete.generation,
                    &Write::new(FileKey::Goal, OwnedBytes::from_static(b"old accepted")),
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(1), accepted_rx).await??;
    let reset = tokio::time::timeout(Duration::from_secs(1), volume.observe(Query::Root, None, 1))
        .await??;
    assert!(reset.reset);
    let rebuilt = reset.snapshot.ok_or("missing rebuilt snapshot")?;
    assert_eq!(rebuilt.generation.position, obsolete.generation.position);
    assert_ne!(rebuilt.generation.epoch, obsolete.generation.epoch);
    release_tx
        .send(())
        .map_err(|_| "old acknowledgement disappeared")?;
    pending.await??;
    let current = volume.snapshot(&Query::Root, 1).await?;
    assert_eq!(current.generation, rebuilt.generation);
    assert_eq!(current.files, vec![write("GOAL.md", "replacement")?]);
    Ok(())
}
