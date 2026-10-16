//! The real durability path with the task-volume and provider locks instrumented.
use super::*;
use crate::task_volume::FileKey;
use acyclic_stream::*;
use futures::FutureExt as _;
use futures::StreamExt as _;
use loom::{
    sync::{Condvar, Mutex},
    thread,
};
use std::sync::Arc;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    loom::future::block_on(Box::pin(future))
}

struct Histories {
    original: Vec<Record>,
    replacement: Vec<Record>,
    replaced: bool,
}

// Production never nests the index and provider locks. Recovery only acquires
// histories; the pause control releases its lock before provider state is read.
struct Provider(
    Mutex<Histories>,
    Option<Arc<Pause>>,
    Option<Arc<ReadPauses>>,
);

impl Histories {
    fn records(&self) -> &[Record] {
        if self.replaced {
            &self.replacement
        } else {
            &self.original
        }
    }
}

#[async_trait::async_trait]
impl StreamProvider for Provider {
    async fn inspect_idempotency(
        &self,
        _: IdempotencyKey,
    ) -> std::result::Result<Option<IdempotencyObservation>, StreamError> {
        Err(StreamError::Unsupported)
    }
    async fn tail(&self, _: StreamPath) -> std::result::Result<u64, StreamError> {
        Ok(self.0.lock().unwrap().records().len() as u64)
    }
    async fn bounds(&self, _: StreamPath) -> std::result::Result<StreamBounds, StreamError> {
        Err(StreamError::Unsupported)
    }
    async fn watermarks(
        &self,
        _: StreamPath,
    ) -> std::result::Result<StreamWatermarks, StreamError> {
        if let Some(pause) = &self.1 {
            pause.hit();
        }
        let state = self.0.lock().unwrap();
        let accepted = state.records().len() as u64;
        Ok(StreamWatermarks {
            accepted,
            durable: accepted - u64::from(!state.replaced),
        })
    }
    async fn read(&self, request: ReadRequest) -> std::result::Result<RecordStream, StreamError> {
        let records = self.0.lock().unwrap().records().to_vec();
        let pauses = self.2.clone();
        let read = pauses.as_ref().map_or(0, |pauses| pauses.begin());
        let limit = if pauses
            .as_ref()
            .is_some_and(|pauses| pauses.short_first && read == 1)
        {
            1
        } else {
            request.limit as usize
        };
        let records = records.into_iter().skip(request.from as usize).take(limit);
        Ok(Box::pin(futures::stream::unfold(
            (records, 0),
            move |(mut records, item)| {
                let pauses = pauses.clone();
                async move {
                    if let Some(pauses) = &pauses {
                        pauses.item(read, item);
                    }
                    records
                        .next()
                        .map(|record| (Ok(record), (records, item + 1)))
                }
            },
        )))
    }
    async fn append(&self, _: AppendRequest) -> std::result::Result<AppendOutcome, StreamError> {
        Err(StreamError::Unsupported)
    }
    async fn fork(&self, _: ForkRequest) -> std::result::Result<ForkReceipt, StreamError> {
        Err(StreamError::Unsupported)
    }
    async fn follow(
        &self,
        _: StreamPath,
        _: u64,
    ) -> std::result::Result<RecordStream, StreamError> {
        Err(StreamError::Unsupported)
    }
    async fn children(&self, _: ChildrenRequest) -> std::result::Result<ChildStream, StreamError> {
        Err(StreamError::Unsupported)
    }
    async fn commit(&self, _: CommitRequest) -> std::result::Result<CommitOutcome, StreamError> {
        Err(StreamError::Unsupported)
    }
    async fn read_commit(
        &self,
        _: CommitId,
    ) -> std::result::Result<CommittedEnvelope, StreamError> {
        Err(StreamError::Unsupported)
    }
}

async fn histories() -> (Histories, FileValue, StreamPath) {
    let path = StreamPath::new("model/files").unwrap();
    let original = Arc::new(MemoryStream::default());
    let volume = TaskVolume::open(original.clone(), path.clone())
        .await
        .unwrap();
    let replacement = Arc::new(MemoryStream::default());
    verification::recover(&replacement, &original)
        .await
        .unwrap();
    let next = TaskVolume::open_existing(replacement.clone(), path.clone())
        .await
        .unwrap();
    let value = FileValue::new(FileKey::Goal, b"old".to_vec());
    volume
        .append(volume.head().await.unwrap(), &value)
        .await
        .unwrap();
    next.append(
        next.head().await.unwrap(),
        &FileValue::new(FileKey::Goal, b"new".to_vec()),
    )
    .await
    .unwrap();
    let request = ReadRequest {
        path: path.clone(),
        from: 0,
        limit: 2,
    };
    let mut old = original.read(request.clone()).await.unwrap();
    let mut new = replacement.read(request).await.unwrap();
    let mut original = Vec::new();
    let mut replacement = Vec::new();
    while let Some(record) = old.next().await {
        original.push(record.unwrap());
    }
    while let Some(record) = new.next().await {
        replacement.push(record.unwrap());
    }
    (
        Histories {
            original,
            replacement,
            replaced: false,
        },
        value,
        path,
    )
}

fn check<const CHECK_ANCHOR: bool>() {
    // Two threads, one replacement and one read. Explore every interleaving;
    // bound the state rather than preemptions, permutations or runtime.
    loom::model(|| {
        let (histories, value, path) = block_on(histories());
        let provider = Arc::new(Provider(Mutex::new(histories), None, None));
        let volume = block_on(TaskVolume::open_existing(provider.clone(), path)).unwrap();
        let recovery = thread::spawn(move || provider.0.lock().unwrap().replaced = true);
        let result = block_on(volume.file_durability::<CHECK_ANCHOR, false>(&value));
        assert!(
            !matches!(result, Ok(true)),
            "the original accepted suffix was never durable"
        );
        recovery.join().unwrap();
    });
}

#[test]
fn replacement_cannot_confirm_a_different_history() {
    check::<true>();
}

#[test]
fn negative_control_missing_anchor_is_detected() {
    assert!(std::panic::catch_unwind(check::<false>).is_err());
}

#[derive(Default)]
struct Paused {
    remaining: usize,
    reached: bool,
    released: bool,
}

// The control mutex is never held while entering TaskVolume or provider state.
// Its condvar releases it while suspended; the model covers all wakeup orders.
#[derive(Default)]
struct Pause(Mutex<Paused>, Condvar);

impl Pause {
    fn hit(&self) {
        let mut state = self.0.lock().unwrap();
        if state.remaining == 0 {
            return;
        }
        state.remaining -= 1;
        if state.remaining == 0 {
            state.reached = true;
            self.1.notify_all();
            while !state.released {
                state = self.1.wait(state).unwrap();
            }
        }
    }

    fn reached(&self) {
        let mut state = self.0.lock().unwrap();
        while !state.reached {
            state = self.1.wait(state).unwrap();
        }
    }

    fn release(&self) {
        self.0.lock().unwrap().released = true;
        self.1.notify_all();
    }
}

fn suspended_io<const HOLD_INDEX: bool>(pause_at: usize) {
    loom::model(move || {
        let (histories, value, path) = block_on(histories());
        let pause = Arc::new(Pause::default());
        let provider = Arc::new(Provider(Mutex::new(histories), Some(pause.clone()), None));
        let volume = Arc::new(block_on(TaskVolume::open_existing(provider, path)).unwrap());
        pause.0.lock().unwrap().remaining = pause_at;
        let reader = volume.clone();
        let operation =
            thread::spawn(move || block_on(reader.file_durability::<true, HOLD_INDEX>(&value)));
        pause.reached();
        // Poll the real competing operation while the provider is suspended.
        // Even a read must finish without needing that I/O to be released.
        let progress = volume.head().now_or_never();
        pause.release();
        operation.join().unwrap().unwrap();
        assert!(
            matches!(progress, Some(Ok(_))),
            "provider I/O retained the index lock"
        );
    });
}

#[test]
fn suspended_provider_does_not_exclude_a_competing_index_operation() {
    for pause_at in [1, 2] {
        suspended_io::<false>(pause_at);
    }
}

#[test]
fn negative_control_holding_index_across_io_is_detected() {
    assert!(std::panic::catch_unwind(|| suspended_io::<true>(2)).is_err());
}

// Read-counter, pause and provider/index locks never nest. Reuse the same
// suspension hook to gate stream polls, including the finite page's EOF.
#[derive(Default)]
struct ReadPauses {
    reads: Mutex<usize>,
    first: Pause,
    second: Pause,
    short_first: bool,
}

impl ReadPauses {
    fn begin(&self) -> usize {
        let mut reads = self.reads.lock().unwrap();
        *reads += 1;
        *reads
    }
    fn item(&self, read: usize, item: usize) {
        if read == 1 && item == 1 {
            self.first.hit();
        }
        if read == 2 && item == 2 {
            self.second.hit();
        }
    }
}

fn concurrent_refresh<const RESUME: bool>(snapshot: bool, short_first: bool) {
    loom::model(move || {
        let (mut histories, _, path) = block_on(histories());
        let mut records = histories.original.clone();
        let latest = FileValue::new(FileKey::Goal, b"latest".to_vec());
        let mut last = records.last().unwrap().clone();
        last.sequence += 1;
        last.commit_id = CommitId::from_bytes([7; 32]);
        last.value = crate::task_volume::segment::encode(&latest, None)
            .unwrap()
            .0;
        records.push(last);
        histories.original.truncate(1);
        let pauses = Arc::new(ReadPauses {
            short_first,
            ..Default::default()
        });
        let provider = Arc::new(Provider(Mutex::new(histories), None, Some(pauses.clone())));
        let volume = Arc::new(block_on(TaskVolume::open_existing(provider.clone(), path)).unwrap());
        // Both writes completed before either reader begins. Its accepted tail
        // is immutable for this scenario, while the shared index starts empty.
        provider.0.lock().unwrap().original = records;
        *pauses.reads.lock().unwrap() = 0;
        pauses.first.0.lock().unwrap().remaining = 1;
        pauses.second.0.lock().unwrap().remaining = 1;
        let first = thread::spawn({
            let volume = volume.clone();
            move || {
                block_on(async {
                    if RESUME && snapshot {
                        let observed = volume
                            .snapshot(&crate::task_volume::Query::Root, 1)
                            .await
                            .unwrap();
                        return (
                            observed.generation.position,
                            observed.files[0].bytes.clone(),
                        );
                    }
                    let position = if RESUME {
                        volume.head().await.unwrap().position
                    } else {
                        volume.refresh_to_tail::<false>().await.unwrap();
                        volume.index.lock().await.next
                    };
                    let bytes = volume
                        .index
                        .lock()
                        .await
                        .files
                        .get(&FileKey::Goal)
                        .unwrap()
                        .file
                        .bytes
                        .clone();
                    (position, bytes)
                })
            }
        });
        pauses.first.reached();
        let second = thread::spawn({
            let volume = volume.clone();
            move || block_on(volume.head()).unwrap()
        });
        pauses.second.reached();
        // Reader two has applied only the first record. Reader one must finish
        // the captured tail without waiting for reader two's remaining I/O.
        pauses.first.release();
        let observed = first.join().unwrap();
        pauses.second.release();
        second.join().unwrap();
        assert_eq!(
            observed,
            (3, latest.bytes),
            "refresh returned a partial accepted prefix"
        );
    });
}

#[test]
fn concurrent_readers_finish_the_tail_captured_at_entry() {
    for snapshot in [false, true] {
        for short_first in [false, true] {
            concurrent_refresh::<true>(snapshot, short_first);
        }
    }
}

#[test]
fn negative_control_partial_peer_advance_cannot_count_as_complete() {
    for snapshot in [false, true] {
        for short_first in [false, true] {
            let failure =
                std::panic::catch_unwind(|| concurrent_refresh::<false>(snapshot, short_first))
                    .expect_err("the broken refresh unexpectedly reached its accepted tail");
            let message = failure
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| failure.downcast_ref::<&str>().copied())
                .unwrap_or("");
            assert!(
                message.contains("refresh returned a partial accepted prefix"),
                "unexpected model failure: {message}"
            );
        }
    }
}
