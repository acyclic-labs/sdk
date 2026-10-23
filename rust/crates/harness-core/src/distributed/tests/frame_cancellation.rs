use super::*;
use crate::conversation::{BoundedContentFuture, ContentIo};
use crate::filesystem::PreparationIo;
use acyclic_fs::{CancellationToken, WorkBudget};
use acyclic_stream::*;
use futures::{Future, Stream, executor::block_on, task::noop_waker_ref};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

const BUDGET: WorkBudget = acyclic_fs::performance::work_budget(1 << 24, 1 << 24);

async fn suspend() {
    let mut yielded = false;
    futures::future::poll_fn(|cx| {
        if std::mem::replace(&mut yielded, true) {
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

#[derive(Default)]
struct YieldingProvider(MemoryStream);

struct YieldingRecords {
    inner: RecordStream,
    yielded: bool,
}

impl Stream for YieldingRecords {
    type Item = std::result::Result<Record, StreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !std::mem::replace(&mut self.yielded, true) {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let result = self.inner.as_mut().poll_next(cx);
        if result.is_ready() {
            self.yielded = false;
        }
        result
    }
}

macro_rules! provider {
    ($($name:ident($arg:ident: $input:ty) -> $output:ty;)*) => {
        #[async_trait::async_trait]
        impl StreamProvider for YieldingProvider {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims { self.0.operation_claims() }

            $(async fn $name(&self, $arg: $input) -> std::result::Result<$output, StreamError> {
                suspend().await;
                self.0.$name($arg).await
            })*

            async fn follow(&self, path: StreamPath, from: u64, filter: acyclic_stream::FollowFilter) -> std::result::Result<RecordStream, StreamError> {
                suspend().await;
                self.0.follow(path, from, filter).await
            }

            async fn read(&self, request: ReadRequest) -> std::result::Result<RecordStream, StreamError> {
                suspend().await;
                Ok(Box::pin(YieldingRecords { inner: self.0.read(request).await?, yielded: false }))
            }
        }
    };
}

provider! {
    inspect_idempotency(key: acyclic_stream::IdempotencyKey) -> Option<IdempotencyObservation>;
    tail(path: StreamPath) -> u64;
    bounds(path: StreamPath) -> StreamBounds;
    append(request: AppendRequest) -> AppendOutcome;
    fork(request: ForkRequest) -> ForkReceipt;
    children(request: ChildrenRequest) -> ChildStream;
    commit(request: CommitRequest) -> CommitOutcome;
    read_commit(id: CommitId) -> CommittedEnvelope;
}

struct YieldingContent;

impl ContentResidencyVerifier for YieldingContent {
    fn supports_read(&self, _: &VolumeRef, _: Option<&ContentIo>) -> crate::Result<bool> {
        Ok(true)
    }

    fn read<'a>(
        &'a self,
        _: &'a FileRef,
        _: u64,
        _: Option<&'a crate::Capabilities>,
        io: Option<&'a ContentIo>,
    ) -> crate::Result<BoundedContentFuture<'a, OwnedBytes>> {
        BoundedContentFuture::new(io, || async {
            suspend().await;
            Ok(OwnedBytes::from_static(b"null"))
        })
    }
}

fn drop_at(kind: u8, seed: u8, after: usize, cancel: bool) -> usize {
    let client = Arc::new(YieldingProvider::default());
    let mut coordinator = block_on(DistributedCoordinator::open(
        &client,
        Arc::new(YieldingContent),
        None,
        &mut None,
    ))
    .unwrap();
    let task = OperationId::from_bytes([seed; 16]);
    let declaration = spec(task, 0).unwrap();
    if matches!(kind, 1 | 2 | 4) {
        block_on(declare(&mut coordinator, declaration.clone(), "seed")).unwrap();
    }
    let worker = worker("worker");
    if matches!(kind, 2 | 4) {
        block_on(start(&mut coordinator, &worker, "start")).unwrap();
    }
    let (owner, issuer) = owner_issuer();
    let scope = grant(
        &issuer,
        "calls",
        &["operation:declare", "operation:observe"],
    )
    .unwrap();
    let verifier = issuer.verifier(&mut None).unwrap();
    let cancellation = CancellationToken::new();
    let io = PreparationIo::from_budget(BUDGET, &cancellation).unwrap();
    let mut observer = io.history_io();
    let mut work = Some(&mut observer as &mut dyn HistoryIo);
    let result = SchedulerEvent::Completed {
        operation_id: task,
        outcome: crate::Outcome::Succeeded(declaration.state.clone()),
        fence: None,
        execution_duration_ns: None,
    };
    let mut future: Pin<Box<dyn Future<Output = crate::Result<()>> + '_>> = match kind {
        0 => Box::pin(coordinator.refresh(&mut work)),
        1 => Box::pin(async {
            coordinator
                .declare_operation(
                    &owner,
                    &scope,
                    &verifier,
                    declaration,
                    crate::IdempotencyKey::new("seed").unwrap(),
                    None,
                    &mut work,
                )
                .await
                .map(|_| ())
        }),
        2 => Box::pin(async {
            coordinator
                .apply(
                    crate::IdempotencyKey::new("cancel").unwrap(),
                    SchedulerEvent::CancellationRequested {
                        operation_id: task,
                        recursive: false,
                    },
                    &mut work,
                )
                .await
                .map(|_| ())
        }),
        3 => Box::pin(async {
            coordinator
                .pull_owned(&owner, &scope, &verifier, &worker, None, &mut work)
                .await
                .map(|pull| {
                    assert!(matches!(pull, WorkPull::Idle));
                })
        }),
        4 => Box::pin(coordinator.verify_published_result(&result, &mut work)),
        _ => unreachable!(),
    };
    let mut cx = Context::from_waker(noop_waker_ref());
    let mut points = 0;
    while points < after {
        match future.as_mut().poll(&mut cx) {
            Poll::Pending => points += 1,
            Poll::Ready(result) => {
                result.unwrap();
                break;
            }
        }
    }
    let charged = io.owned_bytes().unwrap();
    if cancel {
        cancellation.cancel();
    }
    drop(future);
    let remaining = io.owned_bytes().unwrap();
    assert_eq!(
        remaining, 0,
        "kind={kind}, seed={seed}, point={points}, charged={charged}"
    );
    assert_eq!(
        charged - remaining,
        charged,
        "charge must equal release at drop"
    );
    points
}

#[test]
fn ordinary_pending_calls_release_their_frame_charge() {
    for kind in 0..5 {
        assert_eq!(drop_at(kind, 41, 1, false), 1);
    }
}

#[test]
fn ordinary_pending_refresh_reuses_the_same_allowance() {
    let client = Arc::new(YieldingProvider::default());
    let mut coordinator = block_on(DistributedCoordinator::open(
        &client,
        Arc::new(YieldingContent),
        None,
        &mut None,
    ))
    .unwrap();
    let io = PreparationIo::from_budget(BUDGET, &CancellationToken::new()).unwrap();
    for _ in 0..32 {
        let mut observer = io.history_io();
        let mut work = Some(&mut observer as &mut dyn HistoryIo);
        let mut future = Box::pin(coordinator.refresh(&mut work));
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(future.as_mut().poll(&mut cx).is_pending());
        assert!(io.owned_bytes().unwrap() > 0);
        drop(future);
        assert_eq!(io.owned_bytes().unwrap(), 0);
    }
}

#[cfg(feature = "model-checking")]
mod properties {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(16))]
        #[test]
        fn drop_at_every_await_balances_ordinary_charges(seed in any::<u8>(), cancel in any::<bool>()) {
            for kind in 0..5 {
                let points = drop_at(kind, seed, usize::MAX, false);
                prop_assert!(points > 0);
                for after in 0..=points {
                    prop_assert_eq!(drop_at(kind, seed, after, cancel), after);
                }
            }
        }
    }
}
