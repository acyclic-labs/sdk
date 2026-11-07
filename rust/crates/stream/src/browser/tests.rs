use super::*;
use futures::{FutureExt as _, TryFutureExt as _};
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
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await
}

#[wasm_bindgen_test]
async fn immutable_results_reuse_only_a_durable_browser_cache() {
    let database = name();
    let provider = BrowserStream::open(&database, BrowserStreamLimits::default())
        .await
        .expect("open");
    let request = AppendRequest {
        path: StreamPath::new("events").expect("path"),
        records: vec!["durable".into()],
        if_tail: Some(0),
        idempotency_key: IdempotencyKey::unique(),
    };
    let AppendOutcome::Committed(receipt) = provider.append(request.clone()).await.expect("append")
    else {
        panic!("append conflict");
    };
    let envelope = provider
        .read_commit(receipt.commit_id)
        .await
        .expect("envelope");
    let retained = crate::HeadsRequest {
        retained: [crate::RetainedRecord {
            commit_id: receipt.commit_id,
            path: request.path.clone(),
            sequence: receipt.start,
        }]
        .into_iter()
        .collect(),
        durable_through: None,
        projection: crate::HeadProjection::History,
        paths: Default::default(),
        children: None,
        limit: 1,
        at_least: crate::RevisionCut::of(receipt.revision.clone()),
    };
    let transactions = provider.inner.transactions.get();
    for _ in 0..2 {
        assert_eq!(
            provider
                .read_commit(receipt.commit_id)
                .now_or_never()
                .expect("retained commit opened a transaction")
                .expect("commit"),
            envelope
        );
        let observed = provider
            .inspect_idempotency(request.idempotency_key.clone())
            .now_or_never()
            .expect("retained key opened a transaction")
            .expect("inspect")
            .expect("retained key");
        assert_eq!(
            observed.outcome,
            IdempotencyOutcome::Append(AppendOutcome::Committed(receipt.clone()))
        );
        let view = provider
            .heads(retained.clone())
            .now_or_never()
            .expect("retained record opened a transaction")
            .expect("retained view");
        view.validate_for(&retained).expect("original selection");
        let record = view.retained[0].record.as_ref().expect("retained record");
        let crate::CommittedMutation::Append(append) = &envelope.mutations[0] else {
            panic!("original append");
        };
        assert_eq!(record, &append.records[0]);
        assert_eq!(record.value.as_ptr(), append.records[0].value.as_ptr());
    }
    assert_eq!(provider.inner.transactions.get(), transactions);

    let mut mixed = retained.clone();
    mixed.paths.push(request.path.clone());
    mixed.limit = 2;
    let mut current = Box::pin(provider.heads(mixed.clone()));
    assert!(futures::poll!(current.as_mut()).is_pending());
    assert!(provider.inner.transactions.get() > transactions);
    current
        .await
        .expect("current and retained view")
        .validate_for(&mixed)
        .expect("mixed selection");

    let cold = BrowserStream::open(&database, BrowserStreamLimits::default())
        .await
        .expect("cold opener");
    let locks = cold.inner.observation_locks.get();
    assert_eq!(
        cold.read_commit(receipt.commit_id)
            .await
            .expect("cold miss"),
        envelope
    );
    assert!(cold.inner.transactions.get() > 0);
    assert_eq!(cold.inner.observation_locks.get() - locks, 1);
    let transactions = provider.inner.transactions.get();
    assert_eq!(
        provider
            .tail(request.path.clone())
            .await
            .expect("current tail"),
        1
    );
    assert!(provider.inner.transactions.get() > transactions);
    let transactions = provider.inner.transactions.get();

    // No immutable fact certifies absence, even when the local cache is valid.
    let locks = provider.inner.observation_locks.get();
    let mut missing = Box::pin(provider.read_commit(CommitId::from_bytes([42; 32])));
    assert!(futures::poll!(missing.as_mut()).is_pending());
    assert!(provider.inner.transactions.get() > transactions);
    assert_eq!(missing.await, Err(StreamError::NotFound));
    assert_eq!(provider.inner.observation_locks.get() - locks, 1);

    let mut absent = retained.clone();
    absent.retained[0].commit_id = CommitId::from_bytes([42; 32]);
    let transactions = provider.inner.transactions.get();
    let locks = provider.inner.observation_locks.get();
    let mut missing = Box::pin(provider.heads(absent.clone()));
    assert!(futures::poll!(missing.as_mut()).is_pending());
    assert!(provider.inner.transactions.get() > transactions);
    let view = missing.await.expect("missing retained publication");
    view.validate_for(&absent).expect("absent selection");
    assert!(view.retained[0].record.is_none());
    assert_eq!(provider.inner.observation_locks.get() - locks, 1);

    enum Lookup {
        Commit,
        Key,
        Retained,
    }
    for lookup in [Lookup::Commit, Lookup::Key, Lookup::Retained] {
        let tentative = AppendRequest {
            path: StreamPath::new("tentative").expect("path"),
            records: vec!["not committed to IndexedDB".into()],
            if_tail: Some(0),
            idempotency_key: IdempotencyKey::unique(),
        };
        // Reproduce the original cancellation boundary: the real canonical core
        // has applied a command whose surrounding transaction never committed.
        let receipt = {
            let mut cache = provider.inner.cache.lock().await;
            cache.valid = false;
            let AppendOutcome::Committed(receipt) = cache
                .provider
                .append(tentative.clone())
                .await
                .expect("stage")
            else {
                panic!("staged append conflict");
            };
            receipt
        };
        let transactions = provider.inner.transactions.get();
        let locks = provider.inner.observation_locks.get();
        match lookup {
            Lookup::Key => assert_eq!(
                provider
                    .inspect_idempotency(tentative.idempotency_key)
                    .await
                    .expect("repair key"),
                None
            ),
            Lookup::Commit => assert_eq!(
                provider.read_commit(receipt.commit_id).await,
                Err(StreamError::NotFound)
            ),
            Lookup::Retained => {
                let mut selected = retained.clone();
                selected.retained[0] = crate::RetainedRecord {
                    commit_id: receipt.commit_id,
                    path: tentative.path,
                    sequence: receipt.start,
                };
                let view = provider
                    .heads(selected.clone())
                    .await
                    .expect("repair retained record");
                view.validate_for(&selected).expect("repaired selection");
                assert!(view.retained[0].record.is_none());
            }
        }
        assert!(provider.inner.transactions.get() > transactions);
        assert_eq!(provider.inner.observation_locks.get() - locks, 1);
        assert_eq!(
            provider
                .read_commit(envelope.commit_id)
                .now_or_never()
                .expect("repaired durable hit opened a transaction")
                .expect("repaired envelope"),
            envelope
        );
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
            .follow(crate::FollowRequest {
                bootstrap_head: false,
                until: None,
                when_absent: Default::default(),
                path: path.clone(),
                from: 1,
                preceding: Some(CommitId::from_bytes([42; 32])),
                filter: crate::FollowFilter::default(),
                progress: false,
                at_least: Default::default(),
            })
            .map_ok(crate::records_only)
            .await,
        Err(StreamError::PrefixNotRetained)
    ));
    let mut records = reader
        .follow(crate::FollowRequest {
            bootstrap_head: false,
            until: None,
            when_absent: Default::default(),
            path: path,
            from: 1,
            preceding: Some(seed.commit_id),
            filter: crate::FollowFilter::default(),
            progress: false,
            at_least: Default::default(),
        })
        .map_ok(crate::records_only)
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
                .follow(crate::FollowRequest::new(
                    StreamPath::new(format!("agents/{id}/events")).expect("path"),
                    1,
                ))
                .map_ok(crate::records_only)
                .await
                .expect("follow")
                .map(move |record| Observed::Record(id, record))
                .boxed_local(),
        );
    }
    let mut observed = records;
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
#[wasm_bindgen_test]
async fn follow_pushes_every_record_after_its_initial_snapshot() {
    let provider = BrowserStream::open(&name(), BrowserStreamLimits::default())
        .await
        .expect("open");
    append(&provider, "events").await.expect("seed");
    let mut records = provider
        .follow(crate::FollowRequest::new(
            StreamPath::new("events").expect("path"),
            1,
        ))
        .map_ok(crate::records_only)
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
        idempotency_key: crate::IdempotencyKey::unique(),
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
        .follow(crate::FollowRequest::new(
            StreamPath::new("events").expect("path"),
            1,
        ))
        .map_ok(crate::records_only)
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
            commands: Some(2),
            ..Default::default()
        },
    )
    .await
    .expect("open");
    append(&provider, "events").await.expect("seed");
    let mut records = provider
        .follow(crate::FollowRequest::new(
            StreamPath::new("events").expect("path"),
            1,
        ))
        .map_ok(crate::records_only)
        .await
        .expect("follow");
    assert_eq!(
        provider
            .fork(ForkRequest {
                exclude: Vec::new(),
                source: StreamPath::new("missing").expect("path"),
                destination: StreamPath::new("rejected").expect("path"),
                at_tail: None,
                idempotency_key: crate::IdempotencyKey::unique(),
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
        .follow(crate::FollowRequest::new(
            StreamPath::new("latency").expect("path"),
            1,
        ))
        .map_ok(crate::records_only)
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
    use std::{
        pin::Pin,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Wake, Waker},
    };
    struct Count(AtomicUsize);
    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let database = name();
    let writer = BrowserStream::open(&database, BrowserStreamLimits::default())
        .await
        .unwrap();
    append(&writer, "events").await.unwrap();
    let reader = BrowserStream::open(&database, BrowserStreamLimits::default())
        .await
        .unwrap();
    let filter = crate::FollowFilter::new(wire::SessionFilter {
        all_of: vec![],
        keys: vec![wire::KeyRange {
            start: b"a".as_slice().into(),
            end: b"c".as_slice().into(),
        }],
        attributes: vec![],
    })
    .unwrap();
    let mut follow = reader
        .follow(crate::FollowRequest {
            bootstrap_head: false,
            until: None,
            when_absent: Default::default(),
            path: StreamPath::new("events").unwrap(),
            from: 1,
            preceding: None,
            filter: filter,
            progress: false,
            at_least: Default::default(),
        })
        .map_ok(crate::records_only)
        .await
        .unwrap();
    let count = Arc::new(Count(AtomicUsize::new(0)));
    let waker = Waker::from(count.clone());
    let mut context = Context::from_waker(&waker);
    assert!(Pin::new(&mut follow).poll_next(&mut context).is_pending());
    for cycle in 0..2 {
        reader.inner.cache.lock().await.valid = false;
        let record = |key: &'static str| crate::AppendRecord {
            value: "body".into(),
            headers: RecordHeaders::new(vec![key.into()], vec![]).unwrap(),
        };
        writer
            .append(AppendRequest {
                path: StreamPath::new("events").unwrap(),
                records: vec![record("z")],
                if_tail: None,
                idempotency_key: crate::IdempotencyKey::unique(),
            })
            .await
            .unwrap();
        // Complete the cross-opener publication refresh without polling follow.
        reader.refresh_publication(true).await.unwrap();
        assert_eq!(
            count.0.load(Ordering::SeqCst),
            cycle,
            "an unrelated key woke the follow"
        );
        writer
            .append(AppendRequest {
                path: StreamPath::new("events").unwrap(),
                records: vec![record("b")],
                if_tail: None,
                idempotency_key: crate::IdempotencyKey::unique(),
            })
            .await
            .unwrap();
        reader.refresh_publication(true).await.unwrap();
        assert_eq!(
            count.0.load(Ordering::SeqCst),
            cycle + 1,
            "matching key did not wake the stable index"
        );
        let record = follow.next().await.unwrap().unwrap();
        assert_eq!(record.sequence, 2 * cycle as u64 + 2);
        assert_eq!(record.headers.keys()[0].as_ref(), b"b");
        assert!(Pin::new(&mut follow).poll_next(&mut context).is_pending());
    }
}

#[wasm_bindgen_test]
async fn browser_wave1_conformance() {
    let provider = BrowserStream::open(&name(), BrowserStreamLimits::default())
        .await
        .unwrap();
    crate::conformance::verify(&crate::conformance::Fixture::new(provider.clone()))
        .await
        .unwrap();
    assert!(
        provider
            .inner
            .publications
            .follows
            .keys_in::<StreamPath, _>(..)
            .unwrap()
            .is_empty()
    );
    assert!(
        provider
            .inner
            .publications
            .interests
            .keys_in::<publication::Key, _>(..)
            .unwrap()
            .is_empty()
    );
}

#[wasm_bindgen_test]
async fn every_opener_of_one_database_proves_the_others_revisions() {
    let database = name();
    let writer = BrowserStream::open(&database, BrowserStreamLimits::default())
        .await
        .expect("writer");
    let reader = BrowserStream::open(&database, BrowserStreamLimits::default())
        .await
        .expect("reader");
    let Ok(AppendOutcome::Committed(receipt)) = append(&writer, "events").await else {
        panic!("append");
    };
    let covered = crate::RevisionCut::of(receipt.revision.clone());
    let transactions = writer.inner.transactions.get();
    {
        let _cache = writer.inner.cache.lock().await;
        let applied = writer
            .wait_revision(&covered)
            .now_or_never()
            .expect("covered authority waited for the cache")
            .expect("covered revision");
        assert!(covered.proven_by(Some(&applied)));
    }
    assert_eq!(writer.inner.transactions.get(), transactions);
    let mut records = writer
        .read(ReadRequest {
            path: StreamPath::new("events").expect("path"),
            key: None,
            from: receipt.start,
            limit: 1,
            at_least: covered,
        })
        .await
        .expect("covered read");
    let record = records.next().await.expect("record").expect("read");
    assert_eq!(record.commit_id, receipt.commit_id);
    assert_eq!(writer.inner.transactions.get(), transactions + 1);
    let mut future = receipt.revision.clone();
    future.index += 1;
    let request = ReadRequest {
        path: StreamPath::new("events").expect("path"),
        key: None,
        from: 1,
        limit: 1,
        at_least: crate::RevisionCut::of(future),
    };
    for invalid in [
        ReadRequest {
            limit: 0,
            ..request.clone()
        },
        ReadRequest {
            limit: u32::MAX,
            ..request.clone()
        },
        ReadRequest {
            key: Some(crate::ReadKeySeek {
                key: vec![0; crate::MAX_RECORD_KEY_BYTES + 1].into(),
                before: 2,
            }),
            ..request
        },
    ] {
        // Invalid controls must not refresh IndexedDB or wait on the future cut.
        assert!(matches!(
            reader.read(invalid).now_or_never(),
            Some(Err(StreamError::LimitExceeded))
        ));
    }
    let applied = reader
        .wait_applied(&crate::RevisionCut::of(receipt.revision.clone()))
        .await
        .expect("shared identity");
    assert!(applied.same_authority(&receipt.revision) && applied.index >= receipt.revision.index);
    // Negative control: another database mints its own identity.
    let other = BrowserStream::open(&name(), BrowserStreamLimits::default())
        .await
        .expect("other");
    // Foreign cuts must refuse before the cache lock or an IndexedDB refresh.
    let _cache = other.inner.cache.lock().await;
    let foreign = crate::RevisionCut::of(receipt.revision);
    assert!(matches!(
        other.wait_applied(&foreign).now_or_never(),
        Some(Err(StreamError::RevisionForeign))
    ));
    assert!(matches!(
        other
            .read(ReadRequest {
                path: StreamPath::new("events").expect("path"),
                from: 1,
                limit: 1,
                key: None,
                at_least: foreign.clone(),
            })
            .now_or_never(),
        Some(Err(StreamError::RevisionForeign))
    ));
    assert!(matches!(
        other
            .follow(FollowRequest {
                at_least: foreign,
                ..FollowRequest::new(StreamPath::new("events").expect("path"), 1)
            })
            .now_or_never(),
        Some(Err(StreamError::RevisionForeign))
    ));
}

/// The actual journal API hands off a static future, so it must outlive both
/// its transaction and its provider without retaining a nonexistent producer.
#[wasm_bindgen_test]
async fn detached_journal_wait_ends_when_its_last_provider_drops() {
    use crate::replicated::CommandJournal as _;
    let provider = BrowserStream::open(&name(), BrowserStreamLimits::default())
        .await
        .expect("open");
    let surviving = provider.clone();
    let mut port = port::BrowserPort::new(&provider, provider.transaction().expect("transaction"))
        .expect("journal port");
    let mut waiting = port.changed(0);
    drop(port);
    drop(provider);
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    drop(surviving);
    assert!(matches!(
        waiting.now_or_never(),
        Some(Err(StreamError::Unavailable))
    ));
}
