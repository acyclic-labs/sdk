//! Instrument Stream's actual append/recovery locks with RUSTFLAGS=--cfg acyclic_loom.
#![cfg(acyclic_loom)]
use acyclic_actors::timers::{TimerRecord, fire};
use acyclic_stream::{AppendRequest, MemoryStream, StreamPath, StreamProvider};
use futures::StreamExt as _;
use loom::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Pushed(AtomicBool);
impl Wake for Pushed {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    loom::future::block_on(Box::pin(future))
}

fn lost_ack_restart_and_append(broken: bool) -> Result {
    let timer = TimerRecord::new(
        "01010101-0101-0101-0101-010101010101".into(),
        "02020202-0202-0202-0202-020202020202".into(),
        1,
    )?;
    let inbox = StreamPath::new("inbox")?;
    let retained = MemoryStream::default();
    // Delivery commits, but its caller crashes before acknowledging the receipt.
    let first = block_on(fire(&retained, &timer, inbox.clone(), 1))?;
    let restarted = MemoryStream::default();
    // Production recovery releases the source read lock before taking the target
    // write lock. A reopened handle is published only after recovery completes.
    block_on(acyclic_stream::verification::recover(&restarted, &retained))?;
    drop(retained);
    let mut follow = block_on(restarted.follow(inbox.clone(), 1))?;
    let pushed = Arc::new(Pushed(AtomicBool::new(false)));
    let waker = Waker::from(pushed.clone());
    assert!(
        follow
            .as_mut()
            .poll_next(&mut Context::from_waker(&waker))
            .is_pending()
    );

    // Bound the model to two actors and two inbox records, without truncating
    // permutations or preemptions. Both operations use the production Stream lock.
    let retry = loom::thread::spawn({
        let provider = restarted.clone();
        let inbox = inbox.clone();
        let timer = timer.clone();
        move || {
            if broken {
                block_on(fire(&UnkeyedRetry(provider), &timer, inbox, 2))
            } else {
                block_on(fire(&provider, &timer, inbox, 2))
            }
        }
    });
    block_on(restarted.append(AppendRequest {
        path: inbox.clone(),
        records: vec![b"concurrent".to_vec().into()],
        if_tail: None,
        idempotency_key: None,
    }))?;
    let repeated = retry.join().map_err(|_| "retry panicked")??;
    assert_eq!(
        repeated, first,
        "timer fire retry changed committed receipt"
    );
    assert!(
        pushed.0.load(Ordering::SeqCst),
        "inbox publication lost its wake"
    );
    let delivered = block_on(follow.next()).ok_or("follow ended")??;
    assert_eq!(delivered.sequence, 1);
    assert_eq!(delivered.value.as_slice(), b"concurrent");
    assert_eq!(
        block_on(restarted.tail(inbox.clone()))?,
        2,
        "timer delivered twice after restart"
    );
    let mut history = block_on(restarted.follow(inbox, 0))?;
    let original = block_on(history.next()).ok_or("restored timer missing")??;
    assert_eq!(original.sequence, 0);
    assert_eq!(
        TimerRecord::decode(&original.value)?,
        timer,
        "restored timer state was torn"
    );
    Ok(())
}

#[test]
fn timer_fire_survives_lost_ack_restart_and_concurrent_append() {
    loom::model(|| {
        let result = lost_ack_restart_and_append(false);
        assert!(result.is_ok(), "{result:?}");
    });
}

struct UnkeyedRetry(MemoryStream);

type Provided<T> = std::result::Result<T, acyclic_stream::StreamError>;

// Delegates every operation except the deliberately broken append.
#[async_trait::async_trait]
impl StreamProvider for UnkeyedRetry {
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> Provided<Option<acyclic_stream::IdempotencyObservation>> {
        self.0.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> Provided<u64> {
        self.0.tail(path).await
    }
    async fn bounds(&self, path: StreamPath) -> Provided<acyclic_stream::StreamBounds> {
        self.0.bounds(path).await
    }
    async fn fork(
        &self,
        request: acyclic_stream::ForkRequest,
    ) -> Provided<acyclic_stream::ForkReceipt> {
        self.0.fork(request).await
    }
    async fn read(
        &self,
        request: acyclic_stream::ReadRequest,
    ) -> Provided<acyclic_stream::RecordStream> {
        self.0.read(request).await
    }
    async fn children(
        &self,
        request: acyclic_stream::ChildrenRequest,
    ) -> Provided<acyclic_stream::ChildStream> {
        self.0.children(request).await
    }
    async fn commit(
        &self,
        request: acyclic_stream::CommitRequest,
    ) -> Provided<acyclic_stream::CommitOutcome> {
        self.0.commit(request).await
    }
    async fn read_commit(
        &self,
        id: acyclic_stream::CommitId,
    ) -> Provided<acyclic_stream::CommittedEnvelope> {
        self.0.read_commit(id).await
    }
    async fn follow(&self, path: StreamPath, from: u64) -> Provided<acyclic_stream::RecordStream> {
        self.0.follow(path, from).await
    }
    async fn append(&self, mut request: AppendRequest) -> Provided<acyclic_stream::AppendOutcome> {
        // Deliberately drop the production fire's stable retry key.
        request.idempotency_key = None;
        self.0.append(request).await
    }
}

#[test]
#[should_panic(expected = "timer fire retry changed committed receipt")]
fn missing_retry_key_duplicates_the_restored_timer() {
    loom::model(|| {
        let result = lost_ack_restart_and_append(true);
        assert!(result.is_ok(), "{result:?}");
    });
}
