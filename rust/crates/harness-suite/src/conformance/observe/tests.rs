use super::*;
use crate::conformance::{Result, fixtures, vectors};
use acyclic_actors::guest::Handler;
use acyclic_harness_core::agent::Delivered;
use acyclic_stream::StreamCategory;
use acyclic_stream::testing::Counted;

#[test]
fn counter_only_mutation_keeps_the_provider_allocation_budget() {
    fn admitted(provider: &dyn StreamProvider) -> acyclic_test_support::AllocationInfo {
        let request = WriteRequest::Append(AppendRequest {
            path: StreamPath::new("unpolled-counter-budget").unwrap(),
            records: vec![OwnedBytes::from_static(b"value").into()],
            if_tail: Some(0),
            idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"counter-budget"))
                .unwrap(),
        });
        let mut pending = None;
        let allocations = acyclic_test_support::measure(|| pending = Some(provider.write(request)));
        drop(pending);
        allocations
    }

    let direct = MemoryStream::default();
    let observed = Observed::counters(Arc::new(MemoryStream::default()));
    let baseline = admitted(&direct);
    assert!(baseline.count_total > 0);
    assert_eq!(
        admitted(&observed),
        baseline,
        "counter-only mutation added an allocation"
    );
}

#[tokio::test]
async fn counter_only_keeps_effects_and_dispatch_counts_without_request_retention() -> Result {
    let mut samples = Vec::new();
    for capture in [true, false] {
        let inner = Counted::new(MemoryStream::default());
        let dispatched = inner.ledger();
        let provider = Arc::new(if capture {
            Observed::capturing(Arc::new(inner))
        } else {
            Observed::counters(Arc::new(inner))
        });
        let handler: Arc<dyn Handler> = vectors::handler(&provider)?;
        let host = vectors::hosting(provider.clone(), handler, None)?;
        let root = host
            .create(&vectors::intent("counter-mode")?, fixtures::adopted())
            .await?;
        vectors::drain(&host).await?;
        assert!(matches!(
            host.send(&root, &fixtures::sender(), &fixtures::request("one")?)
                .await?,
            Delivered::Committed { .. }
        ));
        vectors::drain(&host).await?;
        assert_eq!(
            vectors::outputs(&*provider, &root).await?,
            [b"one".to_vec()]
        );
        assert_eq!(host.runtime().counts(), (1, 0));
        assert_eq!(
            provider
                .tail(StreamPath::new("absent")?)
                .await
                .map_err(|error| error.category()),
            Err(StreamCategory::NotFound)
        );
        let requests = provider.requests.requests();
        assert_eq!(requests, dispatched.requests(), "unreported provider call");
        assert!(requests.data() > 0);
        assert!(requests.control() > 0, "failed calls must remain counted");
        samples.push(requests);
        if capture {
            let retained = provider.take_commits().len() + provider.take_follows().len();
            assert!(retained > 0);
            assert!(provider.history_reads.load(Ordering::SeqCst) > 0);
        } else {
            assert!(
                provider.capture.is_none(),
                "counter-only observation retained a request"
            );
            assert_eq!(provider.calls(), 0);
            assert_eq!(provider.history_reads.load(Ordering::SeqCst), 0);
        }
    }
    assert_eq!(samples[0], samples[1], "capture changed dispatch work");
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
/// One call of every native method, issued and dropped: a call is counted
/// when it is made, before its future is polled.
type NativeCall = fn(&dyn NativeStreamProvider, &Arc<dyn acyclic_native_runtime::NativeReadMeter>);

#[cfg(not(target_arch = "wasm32"))]
fn native_calls() -> Vec<(&'static str, NativeCall)> {
    fn path() -> StreamPath {
        StreamPath::new("native-count").unwrap()
    }
    fn heads(children: bool) -> HeadsRequest {
        HeadsRequest {
            at: None,
            retained: Default::default(),
            durable_through: None,
            projection: HeadProjection::Watermarks,
            paths: Default::default(),
            children: children.then(|| ChildrenPageRequest {
                offset: 0,
                tail_stream: None,
                parent: Some(path()),
                after: None,
                hierarchy_version: None,
                limit: 1,
            }),
            limit: 1,
            at_least: RevisionCut::default(),
        }
    }
    fn commit() -> CommitRequest {
        CommitRequest {
            deadline_unix_millis: None,
            effects: Vec::new(),
            idempotency_key: IdempotencyKey::unique(),
        }
    }
    vec![
        ("durability barrier", |p, m| {
            drop(p.native_durability_barrier(m.clone()))
        }),
        ("prepared durability barrier", |p, m| {
            drop(p.prepare_native_durability_barrier(m.clone()))
        }),
        ("inspect idempotency", |p, m| {
            drop(p.native_inspect_idempotency(IdempotencyKey::unique(), m.clone()))
        }),
        ("tail", |p, m| drop(p.native_tail(path(), m.clone()))),
        ("heads", |p, m| {
            drop(p.native_heads(heads(false), m.clone()))
        }),
        ("child page", |p, m| {
            drop(p.native_heads(heads(true), m.clone()))
        }),
        ("read", |p, m| {
            drop(p.native_read(
                ReadRequest {
                    key: None,
                    path: path(),
                    from: 0,
                    limit: 1,
                    at_least: RevisionCut::default(),
                },
                m.clone(),
            ))
        }),
        ("follow", |p, m| {
            drop(p.native_follow(FollowRequest::new(path(), 0), m.clone()))
        }),
        ("write", |p, m| {
            drop(p.native_write(WriteRequest::Commit(commit()), m.clone()))
        }),
        ("guarded commit", |p, m| {
            let guard = NativeCommitGuard::new(Vec::new(), Box::new(|| Box::pin(async { Ok(()) })));
            drop(p.native_commit_guarded(commit(), None, m.clone(), guard))
        }),
        ("prepared commit", |p, m| {
            let capacity = PreparedRecordCapacity {
                payload_bytes: 0,
                keys: 0,
                attributes: 0,
            };
            drop(p.native_prepare_commit(commit(), capacity, m.clone()))
        }),
    ]
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn every_native_forward_counts_like_the_provider_negative_control() -> Result {
    let meter = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|_| "native work admission")?
    .runtime_meter();
    for capture in [true, false] {
        let inner = Arc::new(Counted::new(MemoryStream::default()));
        let dispatched = inner.ledger();
        let observed = if capture {
            Observed::capturing(inner.clone())
        } else {
            Observed::counters(inner.clone())
        };
        for (name, call) in native_calls() {
            call(&observed, &meter);
            assert_eq!(
                observed.requests.requests(),
                dispatched.requests(),
                "native {name} skipped or misclassified the request ledger"
            );
        }
        // Negative control: a forward that bypasses the observer's count is
        // caught by the same comparison.
        for (name, call) in native_calls() {
            let before = observed.requests.requests();
            call(&*inner, &meter);
            assert_eq!(observed.requests.requests(), before);
            assert_ne!(
                observed.requests.requests(),
                dispatched.requests(),
                "an uncounted native {name} went unnoticed"
            );
        }
    }
    Ok(())
}
