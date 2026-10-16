//! The real durability path with the task-volume and provider locks instrumented.
use super::*;
use crate::task_volume::FileKey;
use acyclic_stream::*;
use futures::FutureExt as _;
use futures::{StreamExt as _, executor::block_on};
use loom::{
    sync::{Condvar, Mutex},
    thread,
};
use std::sync::Arc;

struct Histories {
    original: Vec<Record>,
    replacement: Vec<Record>,
    replaced: bool,
}

// Production never nests the index and provider locks. Recovery only acquires
// histories; the pause control releases its lock before provider state is read.
struct Provider(Mutex<Histories>, Option<Arc<Pause>>);

#[async_trait::async_trait]
impl StreamProvider for Provider {
    async fn inspect_idempotency(
        &self,
        _: IdempotencyKey,
    ) -> std::result::Result<Option<IdempotencyObservation>, StreamError> {
        Err(StreamError::Unsupported)
    }
    async fn tail(&self, _: StreamPath) -> std::result::Result<u64, StreamError> {
        Ok(2)
    }
    async fn bounds(&self, _: StreamPath) -> std::result::Result<StreamBounds, StreamError> {
        Err(StreamError::Unsupported)
    }
    async fn watermarks(
        &self,
        _: StreamPath,
    ) -> std::result::Result<StreamWatermarks, StreamError> {
        if let Some(pause) = &self.1 {
            pause.watermark();
        }
        let state = self.0.lock().unwrap();
        Ok(StreamWatermarks {
            accepted: 2,
            durable: if state.replaced { 2 } else { 1 },
        })
    }
    async fn read(&self, request: ReadRequest) -> std::result::Result<RecordStream, StreamError> {
        let records = {
            let state = self.0.lock().unwrap();
            if state.replaced {
                state.replacement.clone()
            } else {
                state.original.clone()
            }
        };
        Ok(Box::pin(futures::stream::iter(
            records
                .into_iter()
                .skip(request.from as usize)
                .take(request.limit as usize)
                .map(Ok),
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
        let provider = Arc::new(Provider(Mutex::new(histories), None));
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
    fn watermark(&self) {
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

fn suspended_io<const HOLD_INDEX: bool>() {
    loom::model(|| {
        let (histories, value, path) = block_on(histories());
        let pause = Arc::new(Pause::default());
        let provider = Arc::new(Provider(Mutex::new(histories), Some(pause.clone())));
        let volume = Arc::new(block_on(TaskVolume::open_existing(provider, path)).unwrap());
        pause.0.lock().unwrap().remaining = 2;
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
    suspended_io::<false>();
}

#[test]
fn negative_control_holding_index_across_io_is_detected() {
    assert!(std::panic::catch_unwind(suspended_io::<true>).is_err());
}
