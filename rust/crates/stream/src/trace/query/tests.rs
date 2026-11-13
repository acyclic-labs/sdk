use super::*;
use crate::{AppendOutcome, IdempotencyKey, MemoryStream, OwnedBytes};
use futures::TryStreamExt as _;
use std::sync::Arc;

#[tokio::test]
async fn admitted_future_parentage_survives_suspension_without_leaking_to_other_work()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::trace::{Recorder, Request, Settlement};
    let stream = crate::Stream::new(Arc::new(MemoryStream::default()), "agents/worker/trace")?;
    let recorder = Arc::new(Recorder::new(stream.clone(), [13; 16]));
    let request = |name| Request {
        name,
        mechanism: Mechanism::Io,
        parent: None,
        guarantee: Some("original_causal_request"),
    };
    let (release, resumed) = futures::channel::oneshot::channel::<()>();
    let mut observed = Box::pin(Recorder::observe(
        Some(&recorder),
        request("outer"),
        async {
            let child = Recorder::observe(
                Some(&recorder),
                request("child"),
                async { resumed.await.map_err(std::io::Error::other) },
                |_| Some(0),
            )
            .await;
            child.result?;
            Ok::<_, std::io::Error>(())
        },
        |_| Some(0),
    ));
    assert!(futures::poll!(&mut observed).is_pending());
    let unrelated = recorder
        .start(
            "unrelated",
            Mechanism::Io,
            None,
            Some("independent_request"),
        )
        .await?;
    unrelated.finish(None, Outcome::Succeeded).await?;
    release
        .send(())
        .map_err(|_| "observed work abandoned its event")?;
    let finished = observed.await;
    finished.result?;
    assert!(matches!(finished.observation?, Settlement::Finished(_)));
    let mut query = TraceQuery::new(stream.read(0, 6).await?, Default::default());
    let Some(Observation::Started { id: outer, .. }) = query.next().await? else {
        return Err("outer admission missing".into());
    };
    let mut completed = 0;
    while let Some(event) = query.next().await? {
        if let Observation::Completed { start, .. } = event {
            assert_eq!(start.parent, (start.name == "child").then_some(outer));
            completed += 1;
        }
    }
    assert_eq!(completed, 3);
    assert_eq!(query.active().len(), 0);
    Ok(())
}

#[tokio::test]
async fn repeated_calls_use_original_descriptor_evidence() -> Result<(), Box<dyn std::error::Error>>
{
    original_comparisons(ComparisonKind::Request, "repeatedCalls").await
}

#[tokio::test]
async fn repeated_outputs_use_original_descriptor_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    original_comparisons(ComparisonKind::Output, "repeatedOutputs").await
}

#[tokio::test]
async fn cache_prefix_breaks_use_original_descriptor_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    original_comparisons(ComparisonKind::CachePrefix, "cachePrefixBreaks").await
}

async fn original_comparisons(
    kind: ComparisonKind,
    field: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::trace::{Comparison, Recorder};
    let provider = Arc::new(MemoryStream::default());
    let originals = crate::Stream::new(provider.clone(), "agents/worker/requests")?;
    let AppendOutcome::Committed(receipt) = originals
        .append(serde_json::to_vec(&[11_u8; 32])?, IdempotencyKey::unique())
        .await?
    else {
        return Err("original descriptor was not published".into());
    };
    let previous = RetainedRecord {
        commit_id: receipt.commit_id,
        path: originals.path().clone(),
        sequence: receipt.start,
    };
    let stream = crate::Stream::new(provider.clone(), "agents/worker/trace")?;
    let recorder = Arc::new(Recorder::new(stream.clone(), [9; 16]));
    let first = recorder
        .start(
            "model.request",
            Mechanism::ProviderCall,
            None,
            Some("original_request_identity"),
        )
        .await?;
    let mut live = TraceQuery::new(
        stream.follow(0, Default::default()).await?,
        Default::default(),
    );
    assert!(matches!(
        live.next().await?,
        Some(Observation::Started { .. })
    ));
    let mut next = Some(first);
    for identical in [true, false] {
        let recording = match next.take() {
            Some(first) => first,
            None => {
                let recording = recorder
                    .start(
                        "model.request",
                        Mechanism::ProviderCall,
                        None,
                        Some("original_request_identity"),
                    )
                    .await?;
                live.next().await?;
                recording
            }
        };
        let comparison = Comparison {
            kind,
            previous: previous.clone(),
            previous_digest: [11; 32],
            current_digest: if identical { [11; 32] } else { [12; 32] },
        };
        let published = recording.compare(comparison.clone()).await?;
        assert_eq!(
            recording.compare(comparison).await?,
            published,
            "retry duplicated a comparison"
        );
        assert!(matches!(live.next().await?, Some(Observation::Compared {
            evidence, comparison, identical: actual, ..
        }) if actual == identical && evidence == published && comparison.previous == previous));
        recording.finish(None, Outcome::Succeeded).await?;
        assert!(matches!(
            live.next().await?,
            Some(Observation::Completed { .. })
        ));
    }
    verify_comparison_query(provider.as_ref(), &stream, &live, kind, field, &previous).await
}

async fn verify_comparison_query<S>(
    provider: &MemoryStream,
    stream: &crate::Stream<MemoryStream>,
    live: &TraceQuery<S>,
    kind: ComparisonKind,
    field: &str,
    previous: &RetainedRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(stream.tail().await?, 6);
    let mut paged = TraceQuery::new((), Default::default());
    for from in 0..6 {
        assert_eq!(
            paged
                .read_page(
                    provider,
                    crate::ReadRequest {
                        key: None,
                        path: stream.path().clone(),
                        from,
                        limit: 1,
                        at_least: Default::default(),
                    }
                )
                .await?,
            1
        );
    }
    let finite = serde_json::to_value(paged.snapshot(0, 6, Some(6), 0))?;
    assert_eq!(
        finite,
        serde_json::to_value(live.snapshot(0, 6, Some(6), 0))?
    );
    assert_eq!(finite[field], 1);
    let totals = &live.comparisons[&kind];
    assert_eq!((totals.identical, totals.different), (1, 1));
    assert_eq!(
        totals
            .last_identical
            .as_ref()
            .ok_or("equal evidence missing")?
            .sequence,
        1
    );
    assert_eq!(
        totals
            .last_different
            .as_ref()
            .ok_or("different evidence missing")?
            .sequence,
        4
    );
    // A comparison is self-contained even when neither the owning start nor
    // the preceding request publication lies in this selected suffix.
    let mut suffix = TraceQuery::new(stream.read(1, 1).await?, Default::default());
    assert!(
        matches!(suffix.next().await?, Some(Observation::Compared { comparison, identical: true, .. }) if &comparison.previous == previous)
    );
    assert_eq!(suffix.active().len(), 0);
    assert!(suffix.totals().is_empty());
    assert_eq!(suffix.comparisons[&kind].identical, 1);
    Ok(())
}

async fn append(
    stream: &crate::Stream<MemoryStream>,
    event: SpanEvent,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = stream
        .append(
            OwnedBytes::from(serde_json::to_vec(&TraceRecord::V1(event))?),
            IdempotencyKey::unique(),
        )
        .await?;
    assert!(matches!(result, AppendOutcome::Committed(_)));
    Ok(())
}

#[tokio::test]
async fn paged_and_live_queries_preserve_clock_domains_coverage_and_settlement()
-> Result<(), Box<dyn std::error::Error>> {
    let stream = crate::Stream::new(Arc::new(MemoryStream::default()), "agents/worker/trace")?;
    let wait = SpanId {
        process: [1; 16],
        sequence: 0,
    };
    let io = SpanId {
        process: [2; 16],
        sequence: 0,
    };
    append(
        &stream,
        SpanEvent::Started {
            id: wait,
            parent: None,
            name: "wait".into(),
            mechanism: Mechanism::Wait,
            at_ns: 10,
            guarantee: None,
        },
    )
    .await?;
    let mut page = TraceQuery::new(stream.read(0, 1).await?, Default::default());
    assert!(matches!(
        page.next().await?,
        Some(Observation::Started {
            missing_guarantee: true,
            ..
        })
    ));
    assert!(page.next().await?.is_none());
    assert_eq!(page.active_waits(), 1);
    assert!(page.totals().is_empty(), "EOF cannot settle an active wait");

    let mut live = TraceQuery::new(
        stream.follow(0, Default::default()).await?,
        BTreeMap::from([(Mechanism::Io, 10), (Mechanism::Wait, 100)]),
    );
    assert!(matches!(
        live.next().await?,
        Some(Observation::Started { .. })
    ));
    append(
        &stream,
        SpanEvent::Started {
            id: io,
            parent: Some(wait),
            name: "read".into(),
            mechanism: Mechanism::Io,
            at_ns: 1000,
            guarantee: Some("committed_prefix".into()),
        },
    )
    .await?;
    assert!(matches!(
        live.next().await?,
        Some(Observation::Started {
            missing_guarantee: false,
            ..
        })
    ));
    let count = std::num::NonZeroU32::new(1).unwrap();
    let first = live.active_page(None, count).collect::<Vec<_>>();
    assert_eq!(first.len(), 1);
    assert_eq!(*first[0].0, wait);
    assert_eq!(first[0].1.evidence.sequence, 0);
    let between = SpanId {
        sequence: 1,
        ..wait
    };
    let second = live.active_page(Some(between), count).collect::<Vec<_>>();
    assert_eq!(second.len(), 1);
    assert_eq!(*second[0].0, io);
    assert_eq!(second[0].1.evidence.sequence, 1);
    assert!(live.active_page(Some(io), count).next().is_none());
    append(
        &stream,
        SpanEvent::Finished {
            id: io,
            at_ns: 1015,
            bytes: Some(64),
            result: Outcome::Succeeded,
        },
    )
    .await?;
    assert!(matches!(
        live.next().await?,
        Some(Observation::Completed {
            elapsed_ns: 15,
            outlier: Some(true),
            ..
        })
    ));
    append(
        &stream,
        SpanEvent::Finished {
            id: wait,
            at_ns: 30,
            bytes: None,
            result: Outcome::Unsettled,
        },
    )
    .await?;
    assert!(matches!(
        live.next().await?,
        Some(Observation::Completed {
            elapsed_ns: 20,
            result: Outcome::Unsettled,
            outlier: Some(false),
            ..
        })
    ));
    assert_eq!(live.active_waits(), 0);
    assert_eq!(live.active().count(), 0);
    let totals = &live.totals()[&Mechanism::Io];
    assert_eq!(
        (totals.completed, totals.elapsed_ns, totals.known_bytes),
        (1, 15, 64)
    );
    let (duration, original) = totals.longest.as_ref().ok_or("missing largest span")?;
    assert_eq!((*duration, original.sequence), (15, 1));
    assert_eq!(original.path, *stream.path());
    assert_eq!(totals.outliers, Some(1));
    let waits = &live.totals()[&Mechanism::Wait];
    assert_eq!((waits.unsettled, waits.unknown_bytes), (1, 1));
    assert_eq!(waits.outliers, Some(0));
    assert_eq!(live.missing_guarantees(), 1);
    let pages = futures::stream::iter([0, 1, 2, 3])
        .then(|from| stream.read(from, 1))
        .try_flatten();
    let mut finite = TraceQuery::new(
        Box::pin(pages),
        BTreeMap::from([(Mechanism::Io, 10), (Mechanism::Wait, 100)]),
    );
    let mut observations = 0;
    while finite.next().await?.is_some() {
        observations += 1;
    }
    assert_eq!(observations, 4);
    assert_eq!(finite.active_waits(), 0);
    assert_eq!(
        serde_json::to_value(finite.totals())?,
        serde_json::to_value(live.totals())?
    );
    let mut unclassified = TraceQuery::new(stream.read(1, 2).await?, Default::default());
    unclassified.next().await?;
    assert!(matches!(
        unclassified.next().await?,
        Some(Observation::Completed { outlier: None, .. })
    ));
    assert_eq!(unclassified.totals()[&Mechanism::Io].outliers, None);

    // A finish in another producer domain cannot pair merely by sequence.
    append(
        &stream,
        SpanEvent::Finished {
            id: SpanId {
                process: [3; 16],
                sequence: 0,
            },
            at_ns: 99,
            bytes: Some(1),
            result: Outcome::Succeeded,
        },
    )
    .await?;
    assert!(matches!(
        live.next().await?,
        Some(Observation::UnpairedFinish { .. })
    ));
    assert_eq!(live.unpaired_finishes(), 1);
    assert_eq!(live.totals()[&Mechanism::Io].completed, 1);

    stream
        .append(
            OwnedBytes::from(b"not JSON".to_vec()),
            IdempotencyKey::unique(),
        )
        .await?;
    let error = live.next().await.unwrap_err();
    let QueryError::Decode { evidence, source } = error else {
        panic!("lost original decode cause")
    };
    assert_eq!(evidence.sequence, 5);
    assert!(source.is_syntax());
    // Use original records in adversarial delivery order: rejecting a later
    // malformed publication cannot reopen an earlier consumed cursor.
    let mut originals = stream.read(0, 6).await?.try_collect::<Vec<_>>().await?;
    let malformed = originals.pop().ok_or("missing malformed publication")?;
    let earlier = originals.remove(1);
    let start = originals.remove(0);
    let mut reordered = TraceQuery::new(
        futures::stream::iter([Ok(start), Ok(malformed), Ok(earlier)]),
        Default::default(),
    );
    reordered.next().await?;
    assert!(matches!(
        reordered.next().await,
        Err(QueryError::Decode { .. })
    ));
    assert!(matches!(reordered.next().await, Err(QueryError::Invalid {
        evidence, source: QueryViolation::ReorderedRecord,
    }) if evidence.sequence == 1));
    assert_eq!(reordered.active_waits(), 1);
    assert!(reordered.totals().is_empty());

    let next = SpanId {
        process: [1; 16],
        sequence: 1,
    };
    for _ in 0..2 {
        append(
            &stream,
            SpanEvent::Started {
                id: next,
                parent: None,
                name: "wait".into(),
                mechanism: Mechanism::Wait,
                at_ns: 40,
                guarantee: None,
            },
        )
        .await?;
    }
    live.next().await?;
    assert!(matches!(
        live.next().await,
        Err(QueryError::Invalid {
            source: QueryViolation::RepeatedStart,
            ..
        })
    ));
    append(
        &stream,
        SpanEvent::Finished {
            id: next,
            at_ns: 39,
            bytes: None,
            result: Outcome::Succeeded,
        },
    )
    .await?;
    assert!(matches!(
        live.next().await,
        Err(QueryError::Invalid {
            source: QueryViolation::ReversedTime,
            ..
        })
    ));
    assert_eq!(live.active_waits(), 1);
    assert_eq!(live.missing_guarantees(), 2);
    assert_eq!(live.totals()[&Mechanism::Wait].completed, 1);
    append(
        &stream,
        SpanEvent::Finished {
            id: next,
            at_ns: 50,
            bytes: Some(0),
            result: Outcome::Succeeded,
        },
    )
    .await?;
    assert!(matches!(
        live.next().await?,
        Some(Observation::Completed { elapsed_ns: 10, .. })
    ));
    assert_eq!(live.active_waits(), 0);
    assert_eq!(live.totals()[&Mechanism::Wait].elapsed_ns, 30);
    Ok(())
}

#[tokio::test]
async fn last_owner_interest_revokes_inherited_producer_during_original_work()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::trace::{Recorder, Registry};
    let provider = Arc::new(MemoryStream::default());
    let data = crate::Stream::new(provider.clone(), "agents/worker/data")?;
    data.append(b"original".to_vec(), IdempotencyKey::unique())
        .await?;
    let original = data.head().await?;
    let registry = Registry::new(provider.clone());
    let owner = crate::StreamPath::new("agents/worker")?;
    let source = crate::StreamPath::new("agents/worker/trace")?;
    let first = registry.subscribe(owner.clone(), source.clone())?;
    let last = registry.subscribe(owner, source.clone())?;
    let _other = registry.subscribe(
        crate::StreamPath::new("agents/other")?,
        crate::StreamPath::new("agents/other/trace")?,
    )?;
    let recorder: Arc<Recorder<_>> = registry
        .recorder("agents/worker")?
        .ok_or("owner interest missing")?;
    let root = recorder
        .start(
            "request",
            Mechanism::ToolCall,
            None,
            Some("original_request"),
        )
        .await?;
    let parent = root.id();
    let root = root.into_owned();
    root.within(async {
        assert_eq!(data.head().await?, original);
        drop(first);
        assert_eq!(data.head().await?, original);
        let mut retained = data.read(0, 1).await?;
        drop(last);
        // The unrelated owner remains followed: a global subscriber count is
        // insufficient to decide whether this retained producer can emit.
        assert_eq!(data.head().await?, original);
        assert_eq!(
            retained
                .try_next()
                .await?
                .expect("original retained row")
                .value
                .as_ref(),
            b"original"
        );
        assert!(retained.try_next().await?.is_none());
        Ok::<_, crate::StreamError>(())
    })
    .await?;
    assert!(registry.recorder("agents/worker")?.is_none());
    assert!(registry.recorder("agents/other")?.is_some());
    let _completion = root.finish_submitted(None, Outcome::Succeeded)?;
    let traces = crate::Stream::new(provider, source.as_str())?;
    let mut query = TraceQuery::new(
        traces.follow(0, Default::default()).await?,
        Default::default(),
    );
    query.next().await?;
    for _ in 0..2 {
        query.next().await?;
        assert!(
            matches!(query.next().await?, Some(Observation::Completed { start, .. })
            if start.name == "stream.head" && start.parent == Some(parent))
        );
    }
    query.next().await?;
    assert!(
        matches!(query.next().await?, Some(Observation::Completed { start, .. })
        if start.name == "stream.read_open" && start.parent == Some(parent))
    );
    assert!(matches!(query.next().await?, Some(Observation::Completed { id, .. }) if id == parent));
    assert_eq!(
        traces.tail().await?,
        8,
        "unfollowed owner retained inherited observation work"
    );
    Ok(())
}
