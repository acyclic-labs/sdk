use super::*;
use wasm_bindgen_test::*;
wasm_bindgen_test_configure!(run_in_browser);

fn name() -> String {
    format!("discovery-{}", uuid::Uuid::new_v4())
}
async fn append(provider: &BrowserStream, path: &str) -> Result<AppendOutcome, StreamError> {
    provider
        .append(AppendRequest {
            path: StreamPath::new(path)?,
            records: vec![OwnedBytes::from_static(b"value").into()],
            if_tail: None,
            idempotency_key: None,
        })
        .await
}
fn request() -> ChildrenRequest {
    ChildrenRequest {
        parent: None,
        limit: 1,
    }
}

#[wasm_bindgen_test]
async fn checked_browser_follow_validates_then_retains_the_yielded_anchor_across_cache_rebuild() {
    let database = name();
    let writer = BrowserStream::open(&database, BrowserStreamLimits::default())
        .await
        .expect("writer");
    let AppendOutcome::Committed(seed) = append(&writer, "events").await.expect("seed") else {
        panic!("seed conflict");
    };
    let reader = BrowserStream::open(&database, BrowserStreamLimits::default())
        .await
        .expect("reader");
    let path = StreamPath::new("events").expect("path");
    assert!(matches!(
        reader
            .follow_checked(
                path.clone(),
                1,
                CommitId::from_bytes([42; 32]),
                crate::FollowFilter::default()
            )
            .await,
        Err(StreamError::PrefixNotRetained)
    ));
    let mut records = reader
        .follow_checked(path, 1, seed.commit_id, crate::FollowFilter::default())
        .await
        .expect("checked follow");
    assert!(futures::poll!(records.next()).is_pending());
    for sequence in 1..=3 {
        append(&writer, "events").await.expect("append");
        assert_eq!(
            records
                .next()
                .await
                .expect("ended")
                .expect("record")
                .sequence,
            sequence
        );
        // Cancellation of an IndexedDB operation can invalidate the shared cache.
        // The next publication rebuilds identical immutable history from its journal.
        reader.inner.cache.lock().await.valid = false;
    }
}

#[wasm_bindgen_test]
async fn publications_refresh_once_and_wake_only_affected_follows() {
    enum Observed {
        Record(usize, Result<Record, StreamError>),
        Child(Result<Child, StreamError>),
    }
    let database = name();
    let writer = BrowserStream::open(&database, BrowserStreamLimits::default())
        .await
        .expect("writer");
    for id in 0..128 {
        append(&writer, &format!("agents/{id}/events"))
            .await
            .expect("seed");
    }
    // Opening after seeding excludes queued bootstrap broadcasts from the bound.
    let reader = BrowserStream::open(&database, BrowserStreamLimits::default())
        .await
        .expect("reader");
    let mut records = futures::stream::SelectAll::new();
    for id in 0..128 {
        records.push(
            reader
                .follow(
                    StreamPath::new(format!("agents/{id}/events")).expect("path"),
                    1,
                    crate::FollowFilter::default(),
                )
                .await
                .expect("follow")
                .map(move |record| Observed::Record(id, record))
                .boxed_local(),
        );
    }
    let mut children = reader
        .children(ChildrenRequest {
            parent: Some(StreamPath::new("agents").expect("parent")),
            limit: 1,
        })
        .await
        .expect("children");
    for _ in 0..128 {
        child(&mut children).await;
    }
    let mut observed = stream::select(records, children.map(Observed::Child));
    assert!(futures::poll!(observed.next()).is_pending());
    let initial_transactions = reader.inner.transactions.get();
    let initial_reads = reader.inner.follow_reads.get();
    for sequence in 1..=8 {
        append(&writer, "agents/0/events")
            .await
            .expect("publication");
        match observed.next().await.expect("follow ended") {
            Observed::Record(id, record) => {
                assert_eq!(id, 0);
                assert_eq!(record.expect("record").sequence, sequence);
            }
            Observed::Child(child) => panic!("existing-path append created a child: {child:?}"),
        }
    }
    assert!(
        reader.inner.transactions.get() <= initial_transactions + reader.inner.messages.get(),
        "a follower opened its own IndexedDB refresh transaction"
    );
    assert!(
        reader.inner.refreshes.get() <= reader.inner.messages.get(),
        "one publication started multiple refresh passes"
    );
    assert!(
        reader.inner.follow_reads.get() - initial_reads <= 16,
        "unrelated subscriptions performed snapshot work"
    );
}
async fn child(children: &mut ChildStream) -> StreamPath {
    children
        .next()
        .await
        .expect("discovery ended")
        .expect("child error")
        .path
}

#[wasm_bindgen_test]
async fn discovery_never_exposes_a_capacity_rejected_write() {
    let provider = BrowserStream::open(
        &name(),
        BrowserStreamLimits {
            commands: 1,
            ..Default::default()
        },
    )
    .await
    .expect("open");
    let mut children = provider.children(request()).await.expect("subscribe");
    append(&provider, "a").await.expect("accepted");
    assert_eq!(child(&mut children).await.as_str(), "a");
    assert_eq!(append(&provider, "b").await, Err(StreamError::Capacity));
    let next = Box::pin(children.next());
    let refreshed = Box::pin(provider.tail(StreamPath::new("b").expect("path")));
    match futures::future::select(next, refreshed).await {
        futures::future::Either::Left(_) => panic!("rejected child was published"),
        futures::future::Either::Right((tail, mut next)) => {
            assert_eq!(tail, Err(StreamError::NotFound));
            assert!(futures::poll!(next.as_mut()).is_pending());
        }
    }
}

#[wasm_bindgen_test]
async fn discovery_survives_cache_replacement_after_a_rejected_command() {
    let provider = BrowserStream::open(&name(), BrowserStreamLimits::default())
        .await
        .expect("open");
    let mut children = provider.children(request()).await.expect("subscribe");
    append(&provider, "a").await.expect("append");
    assert_eq!(child(&mut children).await.as_str(), "a");
    assert_eq!(
        provider
            .fork(ForkRequest {
                source: StreamPath::new("missing").expect("path"),
                destination: StreamPath::new("rejected").expect("path"),
                at_tail: None,
                idempotency_key: None
            })
            .await,
        Err(StreamError::NotFound)
    );
    provider
        .tail(StreamPath::new("a").expect("path"))
        .await
        .expect("refresh cache");
    append(&provider, "b").await.expect("append");
    assert_eq!(child(&mut children).await.as_str(), "b");
}

#[wasm_bindgen_test]
async fn discovery_wakes_for_another_database_opener() {
    let database = name();
    let reader = BrowserStream::open(&database, BrowserStreamLimits::default())
        .await
        .expect("reader");
    let writer = BrowserStream::open(&database, BrowserStreamLimits::default())
        .await
        .expect("writer");
    let mut children = reader.children(request()).await.expect("subscribe");
    assert!(futures::poll!(children.next()).is_pending());
    let (received, written) = futures::join!(child(&mut children), append(&writer, "remote"));
    written.expect("append");
    assert_eq!(received.as_str(), "remote");
}

#[wasm_bindgen_test]
async fn follow_pushes_every_record_after_its_initial_snapshot() {
    let provider = BrowserStream::open(&name(), BrowserStreamLimits::default())
        .await
        .expect("open");
    append(&provider, "events").await.expect("seed");
    let mut records = provider
        .follow(
            StreamPath::new("events").expect("path"),
            1,
            crate::FollowFilter::default(),
        )
        .await
        .expect("follow");
    let receive = async {
        for sequence in 1..=3 {
            let record = records.next().await.expect("follow ended").expect("record");
            assert_eq!(record.sequence, sequence);
            assert_eq!(record.value.as_ref(), b"delta");
        }
    };
    let write = provider.append(AppendRequest {
        path: StreamPath::new("events").expect("path"),
        records: vec![OwnedBytes::from_static(b"delta").into(); 3],
        if_tail: Some(1),
        idempotency_key: None,
    });
    let ((), written) = futures::join!(receive, write);
    assert!(matches!(written, Ok(AppendOutcome::Committed(_))));
}

#[wasm_bindgen_test]
async fn follow_wakes_for_another_database_opener() {
    let database = name();
    let writer = BrowserStream::open(&database, BrowserStreamLimits::default())
        .await
        .expect("writer");
    append(&writer, "events").await.expect("seed");
    let reader = BrowserStream::open(&database, BrowserStreamLimits::default())
        .await
        .expect("reader");
    let mut records = reader
        .follow(
            StreamPath::new("events").expect("path"),
            1,
            crate::FollowFilter::default(),
        )
        .await
        .expect("follow");
    let (record, written) = futures::join!(records.next(), append(&writer, "events"));
    assert!(matches!(written, Ok(AppendOutcome::Committed(_))));
    assert_eq!(record.expect("ended").expect("record").sequence, 1);
}

#[wasm_bindgen_test]
async fn follow_recovers_its_cache_without_exposing_rejected_records() {
    let provider = BrowserStream::open(
        &name(),
        BrowserStreamLimits {
            commands: 2,
            ..Default::default()
        },
    )
    .await
    .expect("open");
    append(&provider, "events").await.expect("seed");
    let mut records = provider
        .follow(
            StreamPath::new("events").expect("path"),
            1,
            crate::FollowFilter::default(),
        )
        .await
        .expect("follow");
    assert_eq!(
        provider
            .fork(ForkRequest {
                source: StreamPath::new("missing").expect("path"),
                destination: StreamPath::new("rejected").expect("path"),
                at_tail: None,
                idempotency_key: None,
            })
            .await,
        Err(StreamError::NotFound)
    );
    let (record, written) = futures::join!(records.next(), append(&provider, "events"));
    assert!(matches!(written, Ok(AppendOutcome::Committed(_))));
    assert_eq!(record.expect("ended").expect("record").sequence, 1);
    assert_eq!(
        append(&provider, "events").await,
        Err(StreamError::Capacity)
    );
    let pending = Box::pin(records.next());
    let refreshed = Box::pin(provider.tail(StreamPath::new("events").expect("path")));
    match futures::future::select(pending, refreshed).await {
        futures::future::Either::Left(_) => panic!("rejected record was published"),
        futures::future::Either::Right((tail, mut pending)) => {
            assert_eq!(tail, Ok(2));
            assert!(futures::poll!(pending.as_mut()).is_pending());
        }
    }
}

#[wasm_bindgen_test]
async fn browser_append_follow_latency() {
    let provider = BrowserStream::open(&name(), BrowserStreamLimits::default())
        .await
        .expect("open");
    append(&provider, "latency").await.expect("seed");
    let mut records = provider
        .follow(
            StreamPath::new("latency").expect("path"),
            1,
            crate::FollowFilter::default(),
        )
        .await
        .expect("follow");
    let performance = web_sys::window()
        .expect("window")
        .performance()
        .expect("performance");
    let mut measured = Vec::new();
    for sequence in 1..=40 {
        let started = performance.now();
        let receive = async {
            let record = records.next().await.expect("ended").expect("record");
            assert_eq!(record.sequence, sequence);
            assert_eq!(record.value.as_ref(), b"value");
            performance.now() - started
        };
        let (elapsed, written) = futures::join!(receive, append(&provider, "latency"));
        assert!(matches!(written, Ok(AppendOutcome::Committed(_))));
        if sequence > 8 {
            measured.push(elapsed);
        }
    }
    measured.sort_by(f64::total_cmp);
    console_log!(
        "browser_append_follow samples=32 p50_ms={} p99_ms={}",
        measured[15],
        measured[31]
    );
}

#[wasm_bindgen_test]
async fn filtered_publications_survive_cache_rebuild_without_waking_unrelated_follows() {
    use futures::Stream;
    use std::{sync::atomic::{AtomicUsize, Ordering}, task::{Wake, Waker, Context}, pin::Pin};
    struct Count(AtomicUsize);
    impl Wake for Count {
        fn wake(self: Arc<Self>) { self.0.fetch_add(1, Ordering::SeqCst); }
        fn wake_by_ref(self: &Arc<Self>) { self.0.fetch_add(1, Ordering::SeqCst); }
    }
    let database = name();
    let writer = BrowserStream::open(&database, BrowserStreamLimits::default()).await.unwrap();
    append(&writer, "events").await.unwrap();
    let reader = BrowserStream::open(&database, BrowserStreamLimits::default()).await.unwrap();
    let filter = crate::FollowFilter::new(wire::SessionFilter {
                    all_of: vec![],
        keys: vec![wire::KeyRange { start: b"a".as_slice().into(), end: b"c".as_slice().into() }], attributes: vec![],
    }).unwrap();
    let mut follow = reader.follow(StreamPath::new("events").unwrap(), 1, filter).await.unwrap();
    let count = Arc::new(Count(AtomicUsize::new(0)));
    let waker = Waker::from(count.clone());
    let mut context = Context::from_waker(&waker);
    assert!(Pin::new(&mut follow).poll_next(&mut context).is_pending());
    for cycle in 0..2 {
        reader.inner.cache.lock().await.valid = false;
        let record = |key: &'static str| crate::AppendRecord {
            value: "body".into(), headers: RecordHeaders::new(vec![key.into()], vec![]).unwrap(),
        };
        writer.append(AppendRequest { path: StreamPath::new("events").unwrap(), records: vec![record("z")], if_tail: None, idempotency_key: None }).await.unwrap();
        // Complete the cross-opener publication refresh without polling follow.
        reader.refresh_publication(true).await.unwrap();
        assert_eq!(count.0.load(Ordering::SeqCst), cycle, "an unrelated key woke the follow");
        writer.append(AppendRequest { path: StreamPath::new("events").unwrap(), records: vec![record("b")], if_tail: None, idempotency_key: None }).await.unwrap();
        reader.refresh_publication(true).await.unwrap();
        assert_eq!(count.0.load(Ordering::SeqCst), cycle + 1, "matching key did not wake the stable index");
        let record = follow.next().await.unwrap().unwrap();
        assert_eq!(record.sequence, 2 * cycle as u64 + 2);
        assert_eq!(record.headers.keys()[0].as_ref(), b"b");
        assert!(Pin::new(&mut follow).poll_next(&mut context).is_pending());
    }
}
