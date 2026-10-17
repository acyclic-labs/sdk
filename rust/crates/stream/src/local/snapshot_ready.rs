#![cfg(all(feature = "local", not(acyclic_loom)))]

use crate::*;
use futures::{
    FutureExt, StreamExt,
    stream::{self, BoxStream},
};
use std::{
    cell::Cell,
    fmt::Debug,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Instant,
};

const COUNT: usize = 100;
struct Clock;
impl UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        42
    }
}

async fn seed(provider: &impl StreamProvider, children: bool) {
    provider
        .append(AppendRequest {
            path: StreamPath::new("root").unwrap(),
            records: (0..COUNT)
                .map(|_| OwnedBytes::from_static(b"record"))
                .collect(),
            if_tail: None,
            idempotency_key: None,
        })
        .await
        .unwrap();
    if children {
        for i in 0..COUNT {
            provider
                .append(AppendRequest {
                    path: StreamPath::new(format!("root/c{i:03}")).unwrap(),
                    records: vec![OwnedBytes::from_static(b"child")],
                    if_tail: None,
                    idempotency_key: None,
                })
                .await
                .unwrap();
        }
    }
}

// Independent pre-fix cursors: LocalStream boxes each step; MemoryStream does
// not. Each side owns a separately initialized provider and journal.
fn boxed_steps<T: Send + 'static>(input: BoxStream<'static, T>) -> BoxStream<'static, T> {
    stream::unfold(input, |mut input| async move {
        let item = Box::pin(input.next()).await?;
        Some((item, input))
    })
    .boxed()
}

#[derive(Clone)]
enum BaselineProvider {
    Memory(MemoryStream),
    Local(LocalStream),
}
impl BaselineProvider {
    fn memory(&self) -> &MemoryStream {
        match self {
            Self::Memory(provider) => provider,
            Self::Local(provider) => &provider.inner.provider,
        }
    }
}

async fn records(provider: BaselineProvider) -> RecordStream {
    let path = StreamPath::new("root").unwrap();
    let local = matches!(&provider, BaselineProvider::Local(_));
    let (_, cursor) = provider
        .memory()
        .prepare_follow(&path, 0, None)
        .await
        .unwrap();
    let records = stream::unfold(
        (provider, path, cursor),
        move |(provider, path, mut cursor)| async move {
            let mut anchor = None;
            // The pre-fix implementation owned a boxed acquisition on every step.
            let read = async {
                match &provider {
                    BaselineProvider::Memory(provider) => {
                        provider
                            .next_checked_record(&path, &mut cursor, &mut anchor)
                            .await
                    }
                    BaselineProvider::Local(provider) => {
                        provider
                            .accepted(provider.inner.provider.next_checked_record(
                                &path,
                                &mut cursor,
                                &mut anchor,
                            ))
                            .await?
                    }
                }
            };
            let record = if local {
                read.boxed().await
            } else {
                read.await
            }
            .unwrap()?;
            Some((Ok(record), (provider, path, cursor)))
        },
    )
    .boxed();
    if local {
        let span = crate::obs::OperationSpan::new(tracing::info_span!(
            "acyclic.stream.follow",
            rev = 0_u64,
            terminal = tracing::field::Empty,
            outcome = tracing::field::Empty,
            error.kind = tracing::field::Empty,
        ));
        span.scope(records).boxed()
    } else {
        records
    }
}

async fn children(provider: BaselineProvider) -> ChildStream {
    let local = matches!(&provider, BaselineProvider::Local(_));
    let parent = Some(StreamPath::new("root").unwrap());
    let changed = provider.memory().child_changes(parent.as_ref()).await;
    stream::unfold(
        (provider, parent, None, changed),
        move |(provider, parent, previous, mut changed)| async move {
            let read = async {
                match &provider {
                    BaselineProvider::Memory(provider) => {
                        provider
                            .next_child_snapshot(parent.as_ref(), previous.as_ref(), &mut changed)
                            .await
                    }
                    BaselineProvider::Local(provider) => {
                        provider
                            .accepted(provider.inner.provider.next_child_snapshot(
                                parent.as_ref(),
                                previous.as_ref(),
                                &mut changed,
                            ))
                            .await?
                    }
                }
            };
            let path = if local {
                read.boxed().await
            } else {
                read.await
            }
            .unwrap()?;
            Some((
                Ok(Child { path: path.clone() }),
                (provider, parent, Some(path), changed),
            ))
        },
    )
    .boxed()
}

fn compare<T: Send + Debug + Eq + 'static>(
    old: BoxStream<'static, Result<T, StreamError>>,
    candidate: BoxStream<'static, Result<T, StreamError>>,
    force_allocation: bool,
    label: &str,
) -> (u128, u128) {
    let candidate = if force_allocation {
        boxed_steps(candidate)
    } else {
        candidate
    };
    let elapsed = Cell::new([0; 2]);
    let run = |mut input: BoxStream<'static, Result<T, StreamError>>, index: usize| {
        let mut output = Vec::with_capacity(COUNT);
        let mut trace = Vec::with_capacity(COUNT * 2);
        let elapsed = &elapsed;
        move || {
            let start = Instant::now();
            for step in 0..COUNT {
                trace.push((step, false));
                let Poll::Ready(Some(item)) = input
                    .as_mut()
                    .poll_next(&mut Context::from_waker(Waker::noop()))
                else {
                    panic!("ready snapshot parked");
                };
                output.push(item);
                trace.push((step, true));
            }
            let mut times = elapsed.get();
            times[index] = start.elapsed().as_nanos();
            elapsed.set(times);
            (output, trace)
        }
    };
    let old = run(old, 0);
    let candidate = run(candidate, 1);
    let (_, [baseline, candidate]) = acyclic_test_support::differential(old, candidate);
    assert_eq!(
        baseline.count_total,
        if label.starts_with("local_") {
            COUNT as u64
        } else {
            0
        },
        "baseline allocation work changed"
    );
    assert_eq!(
        candidate.count_total, 0,
        "ready snapshots must allocate zero times"
    );
    assert_eq!(
        candidate.bytes_total, 0,
        "ready snapshots must allocate zero bytes"
    );
    (elapsed.get()[0], elapsed.get()[1])
}

async fn prepare<P: StreamProvider>(
    old: &P,
    candidate: &P,
    baseline: BaselineProvider,
    child: bool,
) -> ReadyPair {
    let path = StreamPath::new("root").unwrap();
    // Compare exact errors as well as all record bytes/metadata and observer order.
    let errors = [old, candidate].map(|provider| provider.follow(path.clone(), COUNT as u64 + 1));
    let [old_error, candidate_error] = errors;
    assert_eq!(old_error.await.err(), Some(StreamError::OutOfRange));
    assert_eq!(candidate_error.await.err(), Some(StreamError::OutOfRange));
    if child {
        ReadyPair::Children(
            children(baseline).await,
            candidate
                .children(ChildrenRequest {
                    parent: Some(StreamPath::new("root").unwrap()),
                    limit: COUNT as u32,
                })
                .await
                .unwrap(),
        )
    } else {
        ReadyPair::Records(
            records(baseline).await,
            candidate
                .follow(StreamPath::new("root").unwrap(), 0)
                .await
                .unwrap(),
        )
    }
}

enum ReadyPair {
    Records(RecordStream, RecordStream),
    Children(ChildStream, ChildStream),
}
impl ReadyPair {
    fn measure(self, negative: bool, label: &str) -> (u128, u128) {
        match self {
            Self::Records(old, candidate) => compare(old, candidate, negative, label),
            Self::Children(old, candidate) => compare(old, candidate, negative, label),
        }
    }
}

fn sample<P: StreamProvider>(
    runtime: &tokio::runtime::Runtime,
    old: &P,
    candidate: &P,
    baseline: BaselineProvider,
    child: bool,
    negative: bool,
    label: &str,
) {
    runtime.block_on(deferring_durability(async {
        seed(old, child).await;
        seed(candidate, child).await;
    }));
    let mut before = Vec::with_capacity(31);
    let mut after = Vec::with_capacity(31);
    for _ in 0..31 {
        let ready = runtime.block_on(prepare(old, candidate, baseline.clone(), child));
        // Ready polling is measured outside the executor's cooperative budget.
        let (old, candidate) = ready.measure(negative, label);
        before.push(old);
        after.push(candidate);
    }
    before.sort_unstable();
    after.sort_unstable();
    println!(
        "{label}: records={COUNT} samples=31 baseline_ns={} candidate_ns={} candidate_p95_ns={} baseline_allocations={} candidate_allocations=0 candidate_bytes=0",
        before[15],
        after[15],
        after[29],
        if label.starts_with("local_") {
            COUNT
        } else {
            0
        }
    );
}

fn run(negative: bool) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for child in [false, true] {
        let old = MemoryStream::new_with_clock(MemoryLimits::default(), Arc::new(Clock));
        let candidate = MemoryStream::new_with_clock(MemoryLimits::default(), Arc::new(Clock));
        sample(
            &runtime,
            &old,
            &candidate,
            BaselineProvider::Memory(old.clone()),
            child,
            negative,
            if child {
                "memory_children"
            } else {
                "memory_follow"
            },
        );
        let old_root = tempfile::tempdir().unwrap();
        let candidate_root = tempfile::tempdir().unwrap();
        let old = runtime
            .block_on(LocalStream::open_with_clock(
                old_root.path(),
                LocalStreamLimits::default(),
                Arc::new(Clock),
            ))
            .unwrap();
        let candidate = runtime
            .block_on(LocalStream::open_with_clock(
                candidate_root.path(),
                LocalStreamLimits::default(),
                Arc::new(Clock),
            ))
            .unwrap();
        sample(
            &runtime,
            &old,
            &candidate,
            BaselineProvider::Local(old.clone()),
            child,
            negative,
            if child {
                "local_children"
            } else {
                "local_follow"
            },
        );
    }
}

#[test]
fn ready_snapshots_allocate_zero_and_match_independent_prefixes() {
    run(false);
}

#[test]
#[should_panic(expected = "ready snapshots must allocate zero times")]
fn negative_control_reintroduces_per_record_boxing() {
    run(true);
}
