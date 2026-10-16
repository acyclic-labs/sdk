use super::*;
use acyclic_stream::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenRequest, CommitId, CommitOutcome,
    CommitRequest, CommittedEnvelope, ForkReceipt, ForkRequest, IdempotencyObservation,
    ReadRequest, RecordStream, StreamBounds, StreamPath,
};
use loom::sync::atomic::{AtomicU8, Ordering};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pub(super) type Pending<T> = Pin<Box<dyn Future<Output = T> + Send>>;

pub(super) fn block_on<F: Future>(future: F) -> F::Output {
    loom::future::block_on(Box::pin(future))
}

// Only pause at the publication boundary. All storage state, exclusion and
// wakeups still belong to MemoryStream's production Loom-visible locks.
pub(super) struct PublicationCut {
    pub(super) inner: MemoryStream,
    pub(super) phase: Arc<AtomicU8>,
    pub(super) omit_condition: bool,
}

async fn suspend_once() {
    let mut suspended = false;
    futures::future::poll_fn(|cx| {
        if std::mem::replace(&mut suspended, true) {
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

macro_rules! delegate {
    ($($name:ident($arg:ident: $input:ty) -> $output:ty;)*) => {
        #[async_trait::async_trait]
        impl StreamProvider for PublicationCut {
            $(async fn $name(&self, $arg: $input) -> std::result::Result<$output, StreamError> {
                self.inner.$name($arg).await
            })*

            async fn follow(&self, path: StreamPath, from: u64)
                -> std::result::Result<RecordStream, StreamError>
            {
                self.inner.follow(path, from).await
            }

            async fn commit(&self, request: CommitRequest)
                -> std::result::Result<CommitOutcome, StreamError>
            {
                self.phase.store(1, Ordering::SeqCst);
                suspend_once().await;
                let result = self.inner.commit(request).await;
                self.phase.store(2, Ordering::SeqCst);
                suspend_once().await;
                result
            }

            async fn append(&self, mut request: AppendRequest)
                -> std::result::Result<AppendOutcome, StreamError>
            {
                self.phase.store(1, Ordering::SeqCst);
                suspend_once().await;
                if self.omit_condition {
                    request.if_tail = None;
                }
                let result = self.inner.append(request).await;
                self.phase.store(2, Ordering::SeqCst);
                suspend_once().await;
                result
            }
        }
    };
}

delegate! {
    inspect_idempotency(key: acyclic_stream::IdempotencyKey) -> Option<IdempotencyObservation>;
    tail(path: StreamPath) -> u64;
    bounds(path: StreamPath) -> StreamBounds;
    fork(request: ForkRequest) -> ForkReceipt;
    read(request: ReadRequest) -> RecordStream;
    children(request: ChildrenRequest) -> ChildStream;
    read_commit(id: CommitId) -> CommittedEnvelope;
}

pub(super) fn pause_before_publish<T>(mut future: Pending<T>, phase: &AtomicU8) -> Pending<T> {
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
            .is_pending()
    );
    assert_eq!(
        phase.load(Ordering::SeqCst),
        1,
        "pause immediately before publication"
    );
    future
}

pub(super) fn publish_until_cut<T>(mut future: Pending<T>, phase: Arc<AtomicU8>) -> Pending<T> {
    block_on(futures::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        if phase.load(Ordering::SeqCst) == 2 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }));
    future
}

// Prepare a declaration through the real encoder, append and replay. Calling the
// command facade here nests acknowledgement replay under its entire async poll
// chain, exhausting Loom's default Windows coroutine stack before the race.
pub(super) fn declare_fixture(
    coordinator: &mut DistributedCoordinator<MemoryStream>,
    spec: OperationSpec,
    key: &str,
) -> Result<()> {
    let event = SchedulerEvent::Declared {
        spec: Box::new(spec),
    };
    let canonical = crate::store::history::canonical_bytes_with_work(
        &event,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        &mut None,
    )?;
    let bytes = encode(
        coordinator.revision + 1,
        event.operation_id(),
        key,
        *blake3::hash(&canonical).as_bytes(),
        canonical,
        coordinator.next_commit_time(&mut None)?,
        &mut None,
    )?;
    assert!(matches!(
        block_on(append(
            (&coordinator.stream.0, &coordinator.client),
            [OwnedBytes::from(bytes)],
            0,
            Some(coordinator.revision),
            StreamIdempotencyKey::new(key)?,
            event.operation_id(),
            &mut None,
        ))?,
        AppendOutcome::Committed(_)
    ));
    block_on(coordinator.refresh(&mut None))
}
